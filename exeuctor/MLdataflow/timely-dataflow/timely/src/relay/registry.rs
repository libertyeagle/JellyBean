use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;

use rand::thread_rng;
use rand::distributions::{Distribution, Uniform};

use crate::communication::allocator::relay::{InputRelayAllocate, OutputRelayAllocate};
use crate::communication::Data;
use crate::progress::frontier_relay::SingleChannelFrontierUpdateVec;
use crate::progress::Timestamp;
use crate::relay::connector::{DataConnect, DataConnector, FrontierConnect, InputFrontierConnector, OutputFrontierConnector};

/// Registry for input relay worker
pub struct InputRelayRegistry<'a, T: Timestamp+Send, A: InputRelayAllocate> {
    allocator: &'a mut A,
    pub(super) data_connectors: Vec<Box<dyn DataConnect>>,
    input_index_mapping: HashMap<usize, usize>,
    pub(super) frontier_connector: Option<Box<dyn FrontierConnect>>,
    phantom_timestamp: PhantomData<T>
}

impl<'a, T: Timestamp+Send, A: InputRelayAllocate> InputRelayRegistry<'a, T, A> {
    pub(super) fn new(allocator: &'a mut A) -> InputRelayRegistry<'a, T, A> {
        InputRelayRegistry {
            allocator,
            data_connectors: Vec::new(),
            input_index_mapping: HashMap::new(),
            frontier_connector: None,
            phantom_timestamp: PhantomData
        }
    }

    /// Get the current input pipeline index
    pub fn pipeline_index(&self) -> usize { self.allocator.pipeline_index() }

    /// Get the number of timely workers in this pipeline
    pub fn num_timely_workers(&self) -> usize {self.allocator.get_num_timely_workers()}

    /// Register a new scope input that is obtained from current input pipeline
    pub fn register_input<D: Data, H>(&mut self, output_index: usize, input_index: usize, hash_fn: H)
    where
        H: FnMut(&T, &Vec<D>) -> usize + 'static,
    {
        // allocate channels
        let puller = self.allocator.allocate_input_pipeline_puller(output_index + 1);
        let pushers = self.allocator.allocate_timely_workers_pushers(input_index + 1);

        let connector = DataConnector {
            pushers,
            puller,
            hash_fn
        };
        self.data_connectors.push(Box::new(connector));
        self.input_index_mapping.insert(output_index, input_index);
    }

    /// Register a new scope input, push input messages received randomly to one of the timely worker;
    pub fn register_input_random_exchange<D: Data>(&mut self, output_index: usize, input_index: usize) {
        let mut rng = thread_rng();
        let uniform_dist = Uniform::new(0, self.num_timely_workers());

        self.register_input::<D, _>(output_index, input_index, move |_t, _d| uniform_dist.sample(&mut rng))
    }

    /// Register a new scope input, push input messages received to one of the timely worker in a balanced way;
    pub fn register_input_balance_exchange<D: Data>(&mut self, output_index: usize, input_index: usize) {
        let mut count = 0;
        self.register_input::<D, _>(output_index, input_index, move |_t, _d| {
            count += 1;
            count - 1
        })
    }

    /// Finish registration for all inputs, create frontier changes connector
    pub fn release(&mut self) {
        let puller = self.allocator.allocate_input_pipeline_puller::<(usize, SingleChannelFrontierUpdateVec<T>)>(0);
        let pushers = self.allocator.allocate_timely_workers_pushers::<(usize, SingleChannelFrontierUpdateVec<T>)>(0);

        let frontier_connector = InputFrontierConnector {
            pushers,
            input_index_mapping: self.input_index_mapping.clone(),
            puller
        };

        self.frontier_connector = Some(Box::new(frontier_connector));
        self.allocator.finish_channel_allocation();
    }
}

/// Registry for output relay worker
pub struct OutputRelayRegistry<'a, T: Timestamp+Send, A: OutputRelayAllocate> {
    allocator: &'a mut A,
    pub(super) data_connectors: Vec<Box<dyn DataConnect>>,
    required_outputs: HashSet<usize>,
    pub(super) frontier_connector: Option<Box<dyn FrontierConnect>>,
    num_inputs: Option<usize>,
    phantom_timestamp: PhantomData<T>
}

impl<'a, T: Timestamp+Send, A: OutputRelayAllocate> OutputRelayRegistry<'a, T, A> {
    pub(super) fn new(allocator: &'a mut A) -> OutputRelayRegistry<'a, T, A> {
        OutputRelayRegistry {
            allocator,
            data_connectors: Vec::new(),
            required_outputs: HashSet::new(),
            frontier_connector: None,
            num_inputs: None,
            phantom_timestamp: PhantomData
        }
    }

    /// Get the current output pipeline index
    pub fn pipeline_index(&self) -> usize { self.allocator.pipeline_index() }

    /// Get the number of relay nodes of this output pipeline
    pub fn num_relay_nodes(&self) -> usize { self.allocator.get_num_output_relay_nodes() }

    /// For the purpose of allocating channel to timely workers,
    /// we must know the number of inputs at current pipeline (of this relay node)
    pub fn set_num_scope_inputs(&mut self, num_inputs: usize) {
        self.num_inputs = Some(num_inputs);
    }

    /// Register an output that needed to be relay to the output pipeline
    /// Same output may be registered multiple times, for different output pipelines
    pub fn register_output<D: Data, H>(&mut self, output_index: usize, hash_fn: H)
    where
        H: FnMut(&T, &Vec<D>) -> usize + 'static,
    {
        let num_inputs = self.num_inputs.expect("#scope inputs must be set first");
        let puller = self.allocator.allocate_timely_workers_puller(num_inputs + output_index + 1);
        let pushers = self.allocator.allocate_output_pipeline_pushers(output_index + 1);

        let connector = DataConnector {
            pushers,
            puller,
            hash_fn
        };
        self.data_connectors.push(Box::new(connector));
        self.required_outputs.insert(output_index);
    }

    /// Register a new scope output, push output messages received randomly to one of the relay node in the output pipeline;
    pub fn register_output_random_exchange<D: Data>(&mut self, output_index: usize) {
        let mut rng = thread_rng();
        let uniform_dist = Uniform::new(0, self.num_relay_nodes());

        self.register_output::<D, _>(output_index, move |_t, _d| uniform_dist.sample(&mut rng))
    }

    /// Register a new scope output, push output messages received to one of the relay node in the output pipeline;
    pub fn register_output_balance_exchange<D: Data>(&mut self, output_index: usize) {
        let mut count = 0;
        self.register_output::<D, _>(output_index, move |_t, _d| {
            count += 1;
            count - 1
        })
    }

    /// Finish registration for all outputs, create frontier changes connector
    pub fn release<H>(&mut self, mapper_fn: H)
    where
        H: Fn() -> usize + 'static
    {
        let puller = self.allocator.allocate_timely_workers_puller::<(usize, SingleChannelFrontierUpdateVec<T>)>(0);
        let pushers = self.allocator.allocate_output_pipeline_pushers::<(usize, SingleChannelFrontierUpdateVec<T>)>(0);

        let frontier_connector = OutputFrontierConnector {
            pushers,
            required_indices: self.required_outputs.clone(),
            puller,
            mapper_fn
        };

        self.frontier_connector = Some(Box::new(frontier_connector));
        self.allocator.finish_channel_allocation();
    }
}