// READ, Sep 14 2021
//! Types to build operators with general shapes.
//!
//! These types expose some raw timely interfaces, and while public so that others can build on them,
//! they require some sophistication to use correctly. I recommend checking out `builder_rc.rs` for
//! an interface that is intentionally harder to mis-use.

use std::default::Default;
use std::rc::Rc;
use std::cell::RefCell;

use crate::Data;

use crate::scheduling::{Schedule, Activations};

use crate::progress::{Source, Target};
use crate::progress::{Timestamp, Operate, operate::SharedProgress, Antichain};

use crate::dataflow::{Stream, Scope};
use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::ParallelizationContract;
use crate::dataflow::operators::generic::operator_info::OperatorInfo;

/// Contains type-free information about the operator properties.
pub struct OperatorShape {
    name: String,   // A meaningful name for the operator.
    notify: bool,   // Does the operator require progress notifications.
    // we store the number of workers
    peers: usize,   // The total number of workers in the computation.
    inputs: usize,  // The number of input ports.
    outputs: usize, // The number of output ports.
}

/// Core data for the structure of an operator, minus scope and logic.
impl OperatorShape {
    fn new(name: String, peers: usize) -> Self {
        OperatorShape {
            name,
            notify: true,
            peers,
            inputs: 0,
            outputs: 0,
        }
    }

    /// The number of inputs of this operator
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// The number of outputs of this operator
    pub fn outputs(&self) -> usize {
        self.outputs
    }
}

/// Builds operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    scope: G,
    index: usize,
    global: usize,
    address: Vec<usize>,    // path to the operator (ending with index).
    shape: OperatorShape,
    summary: Vec<Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>>,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, mut scope: G) -> Self {

        // worker unnique globbal ID
        let global = scope.new_identifier();
        // allocate an index for the opeartor
        let index = scope.allocate_operator_index();
        let mut address = scope.addr();
        // push the index into the address (add the index to the scope's address)
        address.push(index);
        let peers = scope.peers();

        OperatorBuilder {
            scope,
            index,
            global,
            address,
            shape: OperatorShape::new(name, peers),
            summary: vec![],
        }
    }

    /// The operator's scope-local index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The operator's worker-unique identifier.
    pub fn global(&self) -> usize {
        self.global
    }

    /// Return a reference to the operator's shape
    pub fn shape(&self) -> &OperatorShape {
        &self.shape
    }

    /// Indicates whether the operator requires frontier information.
    pub fn set_notify(&mut self, notify: bool) {
        self.shape.notify = notify;
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P) -> P::Puller
        where
            P: ParallelizationContract<G::Timestamp, D> {
        let connection = vec![Antichain::from_elem(Default::default()); self.shape.outputs];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input_connection<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> P::Puller
    where
        P: ParallelizationContract<G::Timestamp, D> {

        let channel_id = self.scope.new_identifier();
        let logging = self.scope.logging();
        // construct a channel between the output port (where it connected to) and this input port.
        let (sender, receiver) = pact.connect(&mut self.scope, channel_id, &self.address[..], logging);
        let target = Target::new(self.index, self.shape.inputs);
        // connect the stream
        // i.e., now the stream will push the data it received (pushed to the Tee)
        // to the sender we just allocated
        // so that receiver can receive data
        stream.connect_to(target, sender, channel_id);

        self.shape.inputs += 1;
        assert_eq!(self.shape.outputs, connection.len());
        // push the summaries from this input port to all existing output ports
        self.summary.push(connection);

        receiver
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (Tee<G::Timestamp, D>, Stream<G, D>) {
        // by default, we create an output with a default summary from each input port to this output port
        let connection = vec![Antichain::from_elem(Default::default()); self.shape.inputs];
        self.new_output_connection(connection)
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output_connection<D: Data>(&mut self, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> (Tee<G::Timestamp, D>, Stream<G, D>) {

        // we create a Tee to enable simultaneously push to multiple downstream input ports
        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let source = Source::new(self.index, self.shape.outputs);
        // we can connect the input ports to the stream
        // the input port will create a pusher and puller
        // the input port then uses the puller to receive data
        // the pusher is then added to the Tee to let the output stream pushes the outputs to
        let stream = Stream::new(source, registrar, self.scope.clone());

        self.shape.outputs += 1;
        assert_eq!(self.shape.inputs, connection.len());
        // add summaries from existing input ports to this new output port
        for (summary, entry) in self.summary.iter_mut().zip(connection.into_iter()) {
            summary.push(entry);
        }

        // return the pusher and the stream
        // we must push the data to output into the pusher
        (targets, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    // L is a provided logic
    pub fn build<L>(mut self, logic: L)
    where
        L: FnMut(&mut SharedProgress<G::Timestamp>)->bool+'static
    {
        let inputs = self.shape.inputs;
        let outputs = self.shape.outputs;

        let operator = OperatorCore {
            shape: self.shape,
            address: self.address,
            activations: self.scope.activations().clone(),
            logic,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),
            // transfer the ownership of the internal summaries to OperatorCore
            summary: self.summary,
        };

        // insert the operator to the (parent) scope
        self.scope.add_operator_with_indices(Box::new(operator), self.index, self.global);
    }

    /// Information describing the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        OperatorInfo::new(self.index, self.global, &self.address[..])
    }
}

// OperatorCore packs the logic of
// which describe the actual behavior of the operator
// the logic can access the SharedProgress of the operator
// inspect the inputs frontier, output port capabilities
// consumed messages, produced messages
struct OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>)->bool+'static,
{
    shape: OperatorShape,
    address: Vec<usize>,
    // logic takes the reference of the SharedProgress
    // it also possess the ability to change the SharedProgress
    // returns a bool, indicate whether the operation is completed
    logic: L,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    activations: Rc<RefCell<Activations>>,
    // the internal summaries between inputs ports and output ports are stored here
    summary: Vec<Vec<Antichain<T::Summary>>>,
}

impl<T, L> Schedule for OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>)->bool+'static,
{
    fn name(&self) -> &str { &self.shape.name }
    fn path(&self) -> &[usize] { &self.address[..] }
    fn schedule(&mut self) -> bool {
        // when schedule is called, we exectue the logic
        let shared_progress = &mut *self.shared_progress.borrow_mut();
        // return a bool to indicate complete or not.
        (self.logic)(shared_progress)
    }
}

impl<T, L> Operate<T> for OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>)->bool+'static,
{
    fn inputs(&self) -> usize { self.shape.inputs }
    fn outputs(&self) -> usize { self.shape.outputs }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Rc<RefCell<SharedProgress<T>>>) {

        // Request the operator to be scheduled at least once.
        self.activations.borrow_mut().activate(&self.address[..]);

        // by default, we reserve a capability for each output port at `Default::default()`.
        // for each output port, initially gives it the capability to output messages with any timestamp
        // e.g., capability with T=0
        // and for each worker we give an capability
        self.shared_progress
            .borrow_mut()
            .internals
            .iter_mut()
            .for_each(|output| output.update(T::minimum(), self.shape.peers as i64));

        (self.summary.clone(), self.shared_progress.clone())
    }

    // initialize self.frontier antichains as indicated by hosting scope.
    fn set_external_summary(&mut self) {
        // should we schedule the operator here, or just await the first invocation?
        self.schedule();
    }

    fn notify_me(&self) -> bool { self.shape.notify }
}
