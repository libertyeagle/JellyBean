//! Pipeline scope used to build dataflow pipeline graph

use std::cell::RefCell;
use std::rc::Rc;

use rand::thread_rng;
use rand::distributions::{Distribution, Uniform};


use timely::communication::{Pull, Push, RelayConnectAllocate};
use timely::communication::allocator::thread::{ThreadPuller, ThreadPusher};
use timely::ExchangeData;
use timely::dataflow::channels::Message;
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::dataflow::channels::connector::{RelayLogPuller, RelayLogPusher};
use timely::dataflow::channels::pushers::Exchange as ExchangePusher;
use timely::logging::{TimelyLogger as Logger, WorkerIdentifier};
use timely::logging::TimelyProgressLogger as ProgressLogger;
use timely::logging_core::Registry;
use timely::progress::pipeline_wrapper::PipelineBuilder;
use timely::progress::{Operate, Source, SubgraphBuilder, Target, Timestamp};
use timely::progress::timestamp::Refines;
use timely::scheduling::{Activations, Scheduler};
use timely::worker::{AsWorker, Config, RelayConnector};

use crate::static_timely::timely_static_worker::Worker;

/// The root level (pipeline level) scope
/// "wraps" a pipeline builder (which itself is a wrapper of a subgraph builder)
pub struct PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    /// The pipeline builder
    pub pipeline: Rc<RefCell<PipelineBuilder<(), T>>>,
    /// The worker scope for resource allocation
    pub worker: Worker<A>,
    /// Log writer
    pub logging: Option<Logger>,
    /// The progress log writer
    pub progress_logging: Option<ProgressLogger>
}

impl<A, T> PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    /// Acquire input from input pipelines
    /// Returns a Stream
    /// which can be used to built consecutive operators
    /// Note that the index of the input may not correspond to the port index of the subgraph
    /// (i.e., subgraph's progress tracking module)
    /// We can acquire inputs in any order
    pub fn acquire_pipeline_input<D: ExchangeData>(&mut self, index: usize) -> Stream<Self, D> {
        // channel 2 * index is used for receiving frontier changes
        // channel 2 * index + 1 is used for receiving data
        let (_senders, receiver) = self.worker.allocate_relay_channel::<Message<T, D>>(2 * index + 1);
        std::mem::drop(_senders);
        let receiver = Box::new(RelayLogPuller::new(receiver, index, self.worker.index(), self.logging.clone()));
        let (source, registrar) = self.pipeline.borrow_mut().new_input(receiver, index);
        Stream::new(source, registrar, self.clone())
    }

    /// Register an output as this pipeline's outputs
    /// Require the stream, and the index of the output
    /// NOTE: We do require that register_pipeline_output() is called
    /// after we have acquired every pipeline inputs through acquire_pipeline_input()
    pub fn register_pipeline_output<D: ExchangeData, H>(&mut self, stream: &Stream<Self, D>, index: usize, mut mapper_fn: H)
    where
        H : FnMut(&D) -> u64 + 'static
    {
        let num_inputs = self.pipeline.borrow().num_inputs();
        // channel ID: 2 * (num_inputs + index) + 1 is used to send data, channel ID 2 * (num_inputs + index)
        // is used to send frontier updates.
        let (senders, _receiver) = self.worker.allocate_relay_channel(2 * (num_inputs + index) + 1);
        std::mem::drop(_receiver);
        let senders = senders.into_iter().enumerate().map(|(i,x)| RelayLogPusher::new(x, index, self.worker.index(), i, self.logging.clone())).collect::<Vec<_>>();
        let exchange_sender = ExchangePusher::new(senders, move |_, d| (mapper_fn)(d));
        let target = self.pipeline.borrow_mut().new_output(index);
        stream.connect_to(target, exchange_sender, index);
    }

    pub fn register_pipeline_output_random_exchange<D: ExchangeData>(&mut self, stream: &Stream<Self, D>, index: usize) {
        let mut rng = thread_rng();
        let uniform_dist = Uniform::new(0, self.worker.num_relay_nodes() as u64);

        self.register_pipeline_output::<D, _>(stream, index, move |_| uniform_dist.sample(&mut rng));
    }

    pub fn register_pipeline_output_balanced_exchange<D: ExchangeData>(&mut self, stream: &Stream<Self, D>, index: usize) {
        let mut counter = 0;
        self.register_pipeline_output::<D, _>(stream, index, move |_| {
            counter += 1;
            counter - 1
        });
    }
}


impl<A, T> Scope for PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    fn name(&self) -> String {
        self.pipeline.borrow().name.clone()
    }

    fn addr(&self) -> Vec<usize> {
       self.pipeline.borrow().path.clone()
    }

    fn add_edge(&self, source: Source, target: Target) {
        self.pipeline.borrow_mut().connect(source, target);
    }

    fn allocate_operator_index(&mut self) -> usize {
        self.pipeline.borrow_mut().allocate_child_id()
    }

    fn add_operator_with_indices(&mut self, operator: Box<dyn Operate<Self::Timestamp>>, local: usize, global: usize) {
        self.pipeline.borrow_mut().add_child(operator, local, global);
    }

    #[inline]
    fn scoped<T2, R, F>(&mut self, name: &str, func: F) -> R
    where
        T2: Timestamp + Refines<T>,
        F: FnOnce(&mut timely::dataflow::scopes::Child<Self, T2>) -> R
    {
        let index = self.pipeline.borrow_mut().allocate_child_id();
        let path = self.pipeline.borrow().path.clone();

        let subscope = RefCell::new(SubgraphBuilder::new_from(index, path, self.logging.clone(), self.progress_logging.clone(), name));
        let result = {
            let mut builder = timely::dataflow::scopes::Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
                progress_logging: self.progress_logging.clone(),
            };
            func(&mut builder)
        };

        let subscope = subscope.into_inner().build(self);
        self.add_operator_with_index(Box::new(subscope), index);
        result
    }
}


impl<A, T> ScopeParent for PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    type Timestamp = T;
}

impl<A, T> Clone for PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    fn clone(&self) -> Self {
        PipelineScope {
            pipeline: self.pipeline.clone(),
            worker: self.worker.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone()
        }
    }
}

impl<A, T> Scheduler for PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.worker.activations()
    }
}

impl<A, T> AsWorker for PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>,
{
    fn config(&self) -> &Config {
        self.worker.config()
    }

    fn index(&self) -> usize { self.worker.index() }

    fn peers(&self) -> usize { self.worker.peers() }

    fn allocate<D: timely::communication::Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<dyn Push<timely::communication::Message<D>>>>, Box<dyn Pull<timely::communication::Message<D>>>) {
        self.worker.allocate(identifier, address)
    }

    fn pipeline<D: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<timely::communication::Message<D>>, ThreadPuller<timely::communication::Message<D>>) {
        self.worker.pipeline(identifier, address)
    }

    fn new_identifier(&mut self) -> usize { self.worker.new_identifier() }

    fn log_register(&self) -> std::cell::RefMut<Registry<WorkerIdentifier>> {
        self.worker.log_register()
    }
}
