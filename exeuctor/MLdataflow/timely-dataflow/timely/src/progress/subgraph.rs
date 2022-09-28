// READ, Sep 12 2021
//! A dataflow subgraph
//!
//! Timely dataflow graphs can be nested hierarchically, where some region of
//! graph is grouped, and presents upwards as an operator. This grouping needs
//! some care, to make sure that the presented operator reflects the behavior
//! of the grouped operators.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::cmp::Reverse;

use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;

use crate::scheduling::Schedule;
use crate::scheduling::activate::Activations;

use crate::progress::frontier::{Antichain, MutableAntichain, MutableAntichainFilter};
use crate::progress::{Timestamp, Operate, operate::SharedProgress};
use crate::progress::{Location, Port, Source, Target};

use crate::progress::ChangeBatch;
use crate::progress::broadcast::Progcaster;
use crate::progress::reachability;
use crate::progress::timestamp::Refines;

use crate::worker::ProgressMode;

/// IMPORTANT: In curren
///
///

// IMPORTANT : by convention, a child identifier of zero is used to indicate inputs and outputs of
// the Subgraph itself. An identifier greater than zero corresponds to an actual child, which can
// be found at position (id - 1) in the `children` field of the Subgraph.

/// A builder for interactively initializing a `Subgraph`.
///
/// This collects all the information necessary to get a `Subgraph` up and
/// running, and is important largely through its `build` method which
/// actually creates a `Subgraph`.
pub struct SubgraphBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp,
{
    /// The name of this subgraph.
    pub name: String,

    /// A sequence of integers uniquely identifying the subgraph.
    pub path: Vec<usize>,

    // whether this is a builder for the pipeline-level subgraph?
    pipeline_scope: bool,

    /// The index assigned to the subgraph by its parent.
    index: usize,

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<TInner>>,
    child_count: usize,

    // dataflow edges
    edge_stash: Vec<(Source, Target)>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    // just counts the number of input messages
    input_messages: Vec<Rc<RefCell<ChangeBatch<TInner>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    /// Logging handle
    logging: Option<Logger>,

    /// Progress logging handle
    progress_logging: Option<ProgressLogger>,
}

impl<TOuter, TInner> SubgraphBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    /// Allocates a new input to the subgraph and returns the target to that input in the outer graph.
    pub fn new_input(&mut self, shared_counts: Rc<RefCell<ChangeBatch<TInner>>>) -> Target {
        // shared_counts counts the number of records entering this subgraph / scope
        self.input_messages.push(shared_counts);
        Target::new(self.index, self.input_messages.len() - 1)
    }

    /// Allocates a new output from the subgraph and returns the source of that output in the outer graph.
    pub fn new_output(&mut self) -> Source {
        self.output_capabilities.push(MutableAntichain::new());
        Source::new(self.index, self.output_capabilities.len() - 1)
    }

    /// Introduces a dependence from the source to the target.
    ///
    /// This method does not effect data movement, but rather reveals to the progress tracking infrastructure
    /// that messages produced by `source` should be expected to be consumed at `target`.
    pub fn connect(&mut self, source: Source, target: Target) {
        self.edge_stash.push((source, target));
    }

    /// Creates a new Subgraph from a channel allocator and "descriptive" indices.
    pub fn new_from(
        index: usize,
        mut path: Vec<usize>,
        logging: Option<Logger>,
        progress_logging: Option<ProgressLogger>,
        name: &str,
    )
        -> SubgraphBuilder<TOuter, TInner>
    {
        path.push(index);

        // Put an empty placeholder for "outer scope" representative.
        let children = vec![PerOperatorState::empty(0, 0)];

        SubgraphBuilder {
            name: name.to_owned(),
            path,
            pipeline_scope: false,
            index,
            children,
            child_count: 1,
            edge_stash: Vec::new(),
            input_messages: Vec::new(),
            output_capabilities: Vec::new(),
            logging,
            progress_logging,
        }
    }

    /// Creates a new Subgraph builder for building the pipeline-level subgraph
    pub fn new_pipeline_scope_from(
        index: usize,
        path: Vec<usize>,
        logging: Option<Logger>,
        progress_logging: Option<ProgressLogger>,
        name: &str
    ) -> SubgraphBuilder<TOuter, TInner>
    {
        let mut builder = Self::new_from(
            index,
            path,
            logging,
            progress_logging,
            name
        );
        builder.pipeline_scope = true;
        builder
    }


    /// Allocates a new child identifier, for later use.
    pub fn allocate_child_id(&mut self) -> usize {
        self.child_count += 1;
        self.child_count - 1
    }

    /// Adds a new child to the subgraph.
    pub fn add_child(&mut self, child: Box<dyn Operate<TInner>>, index: usize, identifier: usize) {
        {
            // Operate inherits Schedule, which has path
            let mut child_path = self.path.clone();
            child_path.push(index);
            self.logging.as_mut().map(|l| l.log(crate::logging::OperatesEvent {
                id: identifier,
                addr: child_path,
                name: child.name().to_owned(),
            }));
        }
        self.children.push(PerOperatorState::new(child, index, self.path.clone(), identifier, self.logging.clone()))
    }

    /// Now that initialization is complete, actually build a subgraph.
    pub fn build<A: crate::worker::AsWorker>(mut self, worker: &mut A) -> Subgraph<TOuter, TInner> {
        // at this point, the subgraph is frozen. we should initialize any internal state which
        // may have been determined after construction (e.g. the numbers of inputs and outputs).
        // we also need to determine what to return as a summary and initial capabilities, which
        // will depend on child summaries and capabilities, as well as edges in the subgraph.

        // perhaps first check that the children are sanely identified
        self.children.sort_by(|x,y| x.index.cmp(&y.index));
        assert!(self.children.iter().enumerate().all(|(i,x)| i == x.index));

        let inputs = self.input_messages.len();
        let outputs = self.output_capabilities.len();

        // Create empty child zero represenative.
        self.children[0] = PerOperatorState::empty(outputs, inputs);

        let mut builder = reachability::Builder::new();

        // Child 0 has `inputs` outputs and `outputs` inputs, not yet connected.
        builder.add_node(0, outputs, inputs, vec![vec![Antichain::new(); inputs]; outputs]);
        for (index, child) in self.children.iter().enumerate().skip(1) {
            builder.add_node(index, child.inputs, child.outputs, child.internal_summary.clone());
        }

        for (source, target) in self.edge_stash {
            self.children[source.node].edges[source.port].push(target);
            builder.add_edge(source, target);
        }

        // scope_summary: Op 0's output ports to input ports.
        // i.e., scope inputs to scope outputs
        let (tracker, scope_summary) = builder.build();

        let progcaster = Progcaster::new(worker, &self.path, self.logging.clone(), self.progress_logging.clone());

        let mut incomplete = vec![true; self.children.len()];
        incomplete[0] = false;
        let incomplete_count = incomplete.len() - 1;

        let activations = worker.activations().clone();

        activations.borrow_mut().activate(&self.path[..]);

        Subgraph {
            name: self.name,
            path: self.path,
            inputs,
            outputs,
            incomplete,
            incomplete_count,
            activations,
            temp_active: BinaryHeap::new(),
            children: self.children,
            input_messages: self.input_messages,
            output_capabilities: self.output_capabilities,

            pipeline_output_frontier_changes: ChangeBatch::new(),
            pipeline_scope: self.pipeline_scope,

            local_pointstamp: ChangeBatch::new(),
            final_pointstamp: ChangeBatch::new(),
            progcaster,
            pointstamp_tracker: tracker,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),
            scope_summary,

            progress_mode: worker.config().progress_mode,
        }
    }
}


/// A dataflow subgraph.
///
/// The subgraph type contains the infrastructure required to describe the topology of and track
/// progress within a dataflow subgraph.
pub struct Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    name: String,           // an informative name.
    /// Path of identifiers from the root.
    pub path: Vec<usize>,
    inputs: usize,          // number of inputs.
    outputs: usize,         // number of outputs.

    // handles to the children of the scope. index i corresponds to entry i-1, unless things change.
    children: Vec<PerOperatorState<TInner>>,

    incomplete: Vec<bool>,   // the incompletion status of each child.
    incomplete_count: usize, // the number of incomplete children.

    // shared activations (including children).
    activations: Rc<RefCell<Activations>>,
    temp_active: BinaryHeap<Reverse<usize>>,

    // shared state written to by the datapath, counting records entering this subgraph instance.
    input_messages: Vec<Rc<RefCell<ChangeBatch<TInner>>>>,

    // expressed capabilities, used to filter changes against.
    output_capabilities: Vec<MutableAntichain<TOuter>>,

    // the frontier changes of the scope outputs to send to relay node to relay to the output pipelines
    // for each of the scope output (Targets of operator 0), we will include
    // the frontier changes (not including the pointstamps at this target port,
    // since we are not reporting the pointstamp changes at Op 0)
    // and the pointstamps pushed at target 0 (#produced messages waiting for consumption)
    pipeline_output_frontier_changes: ChangeBatch<(Target, TInner)>,

    // is the subgraph the root scope (i.e., pipeline level)?
    pipeline_scope: bool,

    // pointstamp messages to exchange. ultimately destined for `messages` or `internal`.
    local_pointstamp: ChangeBatch<(Location, TInner)>,
    final_pointstamp: ChangeBatch<(Location, TInner)>,

    // Graph structure and pointstamp tracker.
    // pointstamp_builder: reachability::Builder<TInner>,
    pointstamp_tracker: reachability::Tracker<TInner>,

    // channel / whatever used to communicate pointstamp updates to peers.
    progcaster: Progcaster<TInner>,

    shared_progress: Rc<RefCell<SharedProgress<TOuter>>>,
    scope_summary: Vec<Vec<Antichain<TInner::Summary>>>,

    progress_mode: ProgressMode,
}

impl<TOuter, TInner> Schedule for Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    fn name(&self) -> &str { &self.name }

    fn path(&self) -> &[usize] { &self.path }

    // begin execution of scope / operator when schedule is called
    fn schedule(&mut self) -> bool {
        // This method performs several actions related to progress tracking
        // and child operator scheduling. The actions have been broken apart
        // into atomic actions that should be able to be safely executed in
        // isolation, by a potentially clueless user (yours truly).
        if !self.pipeline_scope {self.accept_frontier()}  // Accept supplied frontier changes.
        self.harvest_inputs();          // Count records entering the scope.

        // Receive post-exchange progress updates.
        self.progcaster.recv(&mut self.final_pointstamp);

        // Commit and propagate final pointstamps.
        self.propagate_pointstamps();

        {   // Enqueue active children; scoped to let borrow drop.
            let temp_active = &mut self.temp_active;
            self.activations
                .borrow_mut()
                .for_extensions(&self.path[..], |index| temp_active.push(Reverse(index)));
        }

        // Schedule child operators.
        //
        // We should be able to schedule arbitrary subsets of children, as
        // long as we eventually schedule all children that need to do work.
        let mut previous = 0;
        while let Some(Reverse(index)) = self.temp_active.pop() {
            // De-duplicate, and don't revisit.
            if index > previous {
                // TODO: This is a moment where a scheduling decision happens.
                self.activate_child(index);
                previous = index;
            }
        }

        // Transmit produced progress updates.
        self.send_progress();

        // If child scopes surface more final pointstamp updates we must re-execute.
        if !self.final_pointstamp.is_empty() {
            self.activations.borrow_mut().activate(&self.path[..]);
        }

        // A subgraph is incomplete if any child is incomplete, or there are outstanding messages.
        let incomplete = self.incomplete_count > 0;
        let tracking = self.pointstamp_tracker.tracking_anything();

        incomplete || tracking
    }
}


impl<TOuter, TInner> Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    /// Schedules a child operator and collects progress statements.
    ///
    /// The return value indicates that the child task cannot yet shut down.
    fn activate_child(&mut self, child_index: usize) -> bool {

        let child = &mut self.children[child_index];
        let incomplete = child.schedule();

        if incomplete != self.incomplete[child_index] {
            if incomplete { self.incomplete_count += 1; }
            else          { self.incomplete_count -= 1; }
            self.incomplete[child_index] = incomplete;
        }

        if !incomplete {
            // Consider shutting down the child, if neither capabilities nor input frontier.
            let child_state = self.pointstamp_tracker.node_state(child_index);
            // no frontier at the input ports
            // nothing more will come
            let frontiers_empty = child_state.targets.iter().all(|x| x.implications.is_empty());
            // no capabilities at the output ports
            // no more messages will be sent
            let no_capabilities = child_state.sources.iter().all(|x| x.pointstamps.is_empty());
            if frontiers_empty && no_capabilities {
                child.shut_down();
            }
        }
        else {
            // In debug mode, check that the progress statements do not violate invariants.
            #[cfg(debug_assertions)] {
                child.validate_progress(self.pointstamp_tracker.node_state(child_index));
            }
        }

        // Extract progress statements into either pre- or post-exchange buffers.
        if child.local {
            child.extract_progress(&mut self.local_pointstamp, &mut self.temp_active, self.pipeline_scope, &mut self.pipeline_output_frontier_changes);
        }
        else {
            child.extract_progress(&mut self.final_pointstamp, &mut self.temp_active, self.pipeline_scope, &mut self.pipeline_output_frontier_changes);
        }

        incomplete
    }

    /// Move frontier changes from parent into progress statements.
    fn accept_frontier(&mut self) {
        for (port, changes) in self.shared_progress.borrow_mut().frontiers.iter_mut().enumerate() {
            let source = Source::new(0, port);
            for (time, value) in changes.drain() {
                self.pointstamp_tracker.update_source(
                    source,
                    TInner::to_inner(time),
                    value
                );
            }
        }
    }

    /// Peak whether any of the input operator's frontier becomes empty after accepting `frontier_changes`
    pub(super) fn peek_pipeline_inputs_frontier_is_empty(&mut self, frontier_changes: &mut ChangeBatch<(usize, TInner)>) -> bool {
        let mut pointstamps = self.pointstamp_tracker.node_state(0).sources.iter().map(|x| x.pointstamps.clone()).collect::<Vec<_>>();
        // for ((source, time), diff) in self.pointstamp_tracker.source_changes.iter() {
        //     if source.node == 0 {
        //         pointstamps[source.port].update_iter(Some((time.clone(), *diff)));
        //     }
        // }
        for ((input_port, time), delta) in frontier_changes.iter() {
            pointstamps[*input_port].update_iter(Some((time.clone(), *delta)));
        }
        // return true if any of the inputs have empty frontier, after `frontier_changes` committed
        pointstamps.iter().any(|x| x.is_empty())
    }

    /// Accept scope inputs' frontiers from the relay node (received from input pipelines)
    pub(super) fn accept_pipeline_inputs_frontier(&mut self, frontier_changes: &mut ChangeBatch<(usize, TInner)>) {
        for ((input_port, time), delta) in frontier_changes.drain() {
            let source = Source::new(0, input_port);
            self.pointstamp_tracker.update_source(source, time, delta);
        }
    }

    /// Drain (pull) the frontier changes (including #produced messages) at the scope outputs
    pub(super) fn pull_pipeline_outputs_frontier_change(&mut self, targets: &mut ChangeBatch<(Target, TInner)>) {
        targets.extend(self.pipeline_output_frontier_changes.drain());
    }

    /// Block a scope input port with minimal timestamp
    /// should be called just after creating the pipeline-level subgraph
    /// before get_internal_summary() and set_external_summary()
    pub(super) fn block_input_progress(&mut self, index: usize) {
        let source = Source::new(0, index);
        // put a pointstamp with minimum timestamp at the scope input port index.
        self.pointstamp_tracker.update(Location::from(source), TInner::minimum(), 1);
        // self.propagate_pointstamps();
    }

    /// Remove the block pointstamp at the scope input port
    /// should be called just after receiving the first frontier change (i.e., initial frontier)
    /// from the relay node and before calling accept_pipeline_input_frontier()
    // pub(super) fn unblock_input_progress(&mut self, index: usize) {
    //     let source = Source::new(0, index);
    //     self.pointstamp_tracker.update(Location::from(source), TInner::minimum(), -1);
    // }
    pub(super) fn unblock_input_progress(&self, index: usize, frontier_changes: &mut ChangeBatch<(usize, TInner)>) {
        frontier_changes.update((index, TInner::minimum()), -1);
    }

    /// Collects counts of records entering the scope.
    ///
    /// This method moves message counts from the output of child zero to the inputs to
    /// attached operators. This is a bit of a hack, because normally one finds capabilities
    /// at an operator output, rather than message counts. These counts are used only at
    /// mark [XXX] where they are reported upwards to the parent scope.
    fn harvest_inputs(&mut self) {
        for input in 0 .. self.inputs {
            let source = Location::new_source(0, input);
            let mut borrowed = self.input_messages[input].borrow_mut();
            for (time, delta) in borrowed.drain() {
                for target in &self.children[0].edges[input] {
                    self.local_pointstamp.update((Location::from(*target), time.clone()), delta);
                }
                // this pointstamp change at Op 0 will not actually be updated into the tracker.
                // it is just used to fill in the subgraph's SharedProgress
                // to tell the parent scope the number of messages the subgraph consumed.
                self.local_pointstamp.update((source, time), -delta);
            }
        }
    }

    /// Commits pointstamps in `self.final_pointstamp`.
    ///
    /// This method performs several steps that for reasons of correctness must
    /// be performed atomically, before control is returned. These are:
    ///
    /// 1. Changes to child zero's outputs are reported as consumed messages.
    /// 2. Changes to child zero's inputs are reported as produced messages.
    /// 3. Frontiers for child zero's inputs are reported as internal capabilities.
    ///
    /// Perhaps importantly, the frontiers for child zero are determined *without*
    /// the messages that are produced for child zero inputs, as we only want to
    /// report retained internal capabilities, and not now-external messages.
    ///
    /// In the course of propagating progress changes, we also propagate progress
    /// changes for all of the managed child operators.
    fn propagate_pointstamps(&mut self) {
        // TODO (March 2022)
        // We still should propagate #produced messages of the scope (pipeline) outputs
        // as frontier updates to the output pipelines.
        // Since in current implementation, frontier updates may arrive earlier than data.
        // When a timestamp `T` is dropped, we may still have incoming data of this timestamp `T`
        // We should use some mechanism during `harvest_inputs`
        // #produced messages are propagated as regular frontier updates
        // When the operators receive actual data from the relay,
        // Then the data is pushed into the `PipelineInputTee` (`Tee`) created in `PipelineBuilder::new_input()`
        // (`pipeline_input_connectors` will forward the data)
        // During `harvest_inputs`, these #consumed messages will be forwarded to the target ports that use the input
        // `self.local_pointstamp.update((Location::from(*target), time.clone()), delta);`
        // We can do something similar as in the sub-scope to parent-scope #consumed messages reporting mechanism
        // `self.local_pointstamp.update((source, time), -delta);`
        // We should decrease the pointstamps (-delta) for the Source(0, port) of the pipeline inputs
        // We should also put them in local_pointstamp, as these updates needed to be broadcasted among the workers
        // The data are distributed among the workers, so only one worker will actually decrease the frontier update
        // caused by #produced messages
        // In this manner, the frontier updates (#produced messages) may arrive earlier/later than data
        // But even as data arrived first, and timestamp at Source(0, port) is decreased
        // The operators' frontiers will not move forward,
        // as we push the #produced messages waiting to be consumed to the operators
        // `self.local_pointstamp.update((Location::from(*target), time.clone()), delta);`

        // Process exchanged pointstamps. Handle child 0 statements carefully.
        for ((location, timestamp), delta) in self.final_pointstamp.drain() {

            // Child 0 corresponds to the parent scope and has special handling.
            // we don't update pointstamp for operator 0
            // the changes recorded in final_pointstamps
            if (location.node == 0) & (self.pipeline_scope) {
                match location.port {
                    Port::Source(_) => {
                        // for pipeline scope, we must update #consumed messages at scope input ports
                        // because #produced messages is included when we accept frontier changes
                        // from input pipelines
                        // UPDATE: do not propagate #produeced messages as frontier updates to the output pipeline
                        // self.pointstamp_tracker.update(location, timestamp, delta);
                    },
                    Port::Target(_) => ()
                }
            }
            else if location.node == 0 {
                match location.port {
                    // [XXX] Report child 0's capabilities as consumed messages.
                    //       Note the re-negation of delta, to make counts positive.
                    Port::Source(scope_input) => {
                        self.shared_progress
                            .borrow_mut()
                            .consumeds[scope_input]
                            .update(timestamp.to_outer(), -delta);
                    },
                    // [YYY] Report child 0's input messages as produced messages.
                    //       Do not otherwise record, as we will not see subtractions,
                    //       and we do not want to present their implications upward.
                    Port::Target(scope_output) => {
                        self.shared_progress
                            .borrow_mut()
                            .produceds[scope_output]
                            .update(timestamp.to_outer(), delta);
                    },
                }
            }
            else {
                self.pointstamp_tracker.update(location, timestamp, delta);
            }
        }

        // Propagate implications of progress changes.
        self.pointstamp_tracker.propagate_all();

        // Drain propagated information into shared progress structure.
        for ((location, time), diff) in self.pointstamp_tracker.pushed().drain() {
            // Targets are actionable, sources are not.
            if let crate::progress::Port::Target(port) = location.port {
                if self.children[location.node].notify {
                    self.temp_active.push(Reverse(location.node));
                }
                // TODO: This logic could also be guarded by `.notify`, but
                // we want to be a bit careful to make sure all related logic
                // agrees with this (e.g. initialization, operator logic, etc.)
                self.children[location.node]
                    .shared_progress
                    .borrow_mut()
                    .frontiers[port]
                    .update(time, diff);
            }
        }

        if self.pipeline_scope {
            // if this is a pipeline subgraph,
            // then we need to obtain the frontier changes
            // at the scope outputs
            for port in 0 .. self.outputs {
                let target = Target::new(0, port);
                self.pipeline_output_frontier_changes.extend(
                    self.children[0].shared_progress.borrow_mut()
                        .frontiers[port].drain().map(|(time, diff)| ((target, time), diff))
                );
            }
        }

        // Extract child zero frontier changes and report as internal capability changes.
        for (output, internal) in self.shared_progress.borrow_mut().internals.iter_mut().enumerate() {
            self.pointstamp_tracker
                .pushed_output()[output]
                .drain()
                .map(|(time, diff)| (time.to_outer(), diff))
                .filter_through(&mut self.output_capabilities[output])
                .for_each(|(time, diff)| internal.update(time, diff));
        }
    }

    /// Sends local progress updates to all workers.
    ///
    /// This method does not guarantee that all of `self.local_pointstamps` are
    /// sent, but that no blocking pointstamps remain
    fn send_progress(&mut self) {

        // If we are requested to eagerly send progress updates, or if there are
        // updates visible in the scope-wide frontier, we must send all updates.
        let must_send = self.progress_mode == ProgressMode::Eager || {
            let tracker = &mut self.pointstamp_tracker;
            self.local_pointstamp
                .iter()
                .any(|((location, time), diff)|
                    // Must publish scope-wide visible subtractions.
                    // non visible subtraction of occurrence counts for the pointstamp not in the frontier
                    // does not need to broadcast
                    tracker.is_global(*location, time) && *diff < 0
                )
        };

        if must_send {
            // self.local_pointstamp will be drained.
            self.progcaster.send(&mut self.local_pointstamp);
        }
    }
}


impl<TOuter, TInner> Operate<TOuter> for Subgraph<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    fn local(&self) -> bool { false }
    fn inputs(&self)  -> usize { self.inputs }
    fn outputs(&self) -> usize { self.outputs }

    // produces connectivity summaries from inputs to outputs, and reports initial internal
    // capabilities on each of the outputs (projecting capabilities from contained scopes).
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Rc<RefCell<SharedProgress<TOuter>>>) {

        // double-check that child 0 (the outside world) is correctly shaped.
        assert_eq!(self.children[0].outputs, self.inputs());
        assert_eq!(self.children[0].inputs, self.outputs());

        let mut internal_summary = vec![vec![Antichain::new(); self.outputs()]; self.inputs()];
        for input in 0 .. self.scope_summary.len() {
            for output in 0 .. self.scope_summary[input].len() {
                for path_summary in self.scope_summary[input][output].elements().iter() {
                    // convert the inner path summary to the parent scope's summary
                    internal_summary[input][output].insert(TInner::summarize(path_summary.clone()));
                }
            }
        }

        // Each child has expressed initial capabilities (their `shared_progress.internals`).
        // We introduce these into the progress tracker to determine the scope's initial
        // internal capabilities.

        // set up initial capabilities
        // the child may write the initial capabilities to their `shared_progress.internals`
        for child in self.children.iter_mut() {
            child.extract_progress(&mut self.final_pointstamp, &mut self.temp_active, self.pipeline_scope, &mut self.pipeline_output_frontier_changes);
        }

        self.propagate_pointstamps();  // Propagate expressed capabilities to output frontiers.

        // Return summaries and shared progress information.
        (internal_summary, self.shared_progress.clone())
    }

    fn set_external_summary(&mut self) {
        self.accept_frontier();
        self.propagate_pointstamps();  // ensure propagation of input frontiers.
        self.children
            .iter_mut()
            .flat_map(|child| child.operator.as_mut())
            .for_each(|op| op.set_external_summary());
    }
}

struct PerOperatorState<T: Timestamp> {

    name: String,       // name of the operator
    index: usize,       // index of the operator within its parent scope
    id: usize,          // worker-unique identifier

    local: bool,        // indicates whether the operator will exchange data or not
    notify: bool,       // whether the operator requires `push_external_progress` information or not.
    inputs: usize,      // number of inputs to the operator
    outputs: usize,     // number of outputs from the operator

    operator: Option<Box<dyn Operate<T>>>,

    // outgoing edges
    edges: Vec<Vec<Target>>,    // edges from the outputs of the operator

    shared_progress: Rc<RefCell<SharedProgress<T>>>,

    internal_summary: Vec<Vec<Antichain<T::Summary>>>,   // cached result from get_internal_summary.

    logging: Option<Logger>,
}

impl<T: Timestamp> PerOperatorState<T> {

    fn empty(inputs: usize, outputs: usize) -> PerOperatorState<T> {
        PerOperatorState {
            name:       "External".to_owned(),
            operator:   None,
            index:      0,
            id:         usize::MAX,
            local:      false,
            notify:     true,
            inputs,
            outputs,

            edges: vec![Vec::new(); outputs],

            logging: None,

            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs,outputs))),
            internal_summary: Vec::new(),
        }
    }

    pub fn new(
        mut scope: Box<dyn Operate<T>>,
        index: usize,
        mut _path: Vec<usize>,
        identifier: usize,
        logging: Option<Logger>
    ) -> PerOperatorState<T>
    {
        let local = scope.local();
        let inputs = scope.inputs();
        let outputs = scope.outputs();
        // Indicates of whether the operator requires `push_external_progress` information or not.
        let notify = scope.notify_me();

        // internal PathSummaries (input ports -> output parts) and SharedProgress information
        // SharedProgress is always created by the child
        let (internal_summary, shared_progress) = scope.get_internal_summary();

        assert_eq!(internal_summary.len(), inputs);
        assert!(!internal_summary.iter().any(|x| x.len() != outputs));

        PerOperatorState {
            name:               scope.name().to_owned(),
            operator:           Some(scope),
            index,
            id:                 identifier,
            local,
            notify,
            inputs,
            outputs,
            edges:              vec![vec![]; outputs],

            logging,

            shared_progress,
            internal_summary,
        }
    }

    pub fn schedule(&mut self) -> bool {

        if let Some(ref mut operator) = self.operator {

            // Perhaps log information about the start of the schedule call.
            if let Some(l) = self.logging.as_mut() {
                // FIXME: There is no contract that the operator must consume frontier changes.
                //        This report could be spurious.
                // TODO:  Perhaps fold this in to `ScheduleEvent::start()` as a "reason"?
                // Frontier capability changes reported
                let frontiers = &mut self.shared_progress.borrow_mut().frontiers[..];
                if frontiers.iter_mut().any(|buffer| !buffer.is_empty()) {
                    l.log(crate::logging::PushProgressEvent { op_id: self.id })
                }

                l.log(crate::logging::ScheduleEvent::start(self.id));
            }

            let incomplete = operator.schedule();

            // Perhaps log information about the stop of the schedule call.
            if let Some(l) = self.logging.as_mut() {
                l.log(crate::logging::ScheduleEvent::stop(self.id));
            }

            incomplete
        }
        else {

            // If the operator is closed and we are reporting progress at it, something has surely gone wrong.
            if self.shared_progress.borrow_mut().frontiers.iter_mut().any(|x| !x.is_empty()) {
                println!("Operator prematurely shut down: {}", self.name);
                println!("  {:?}", self.notify);
                println!("  {:?}", self.shared_progress.borrow_mut().frontiers);
                panic!();
            }

            // A closed operator shouldn't keep anything open.
            false
        }
    }

    fn shut_down(&mut self) {
        if self.operator.is_some() {
            if let Some(l) = self.logging.as_mut() {
                l.log(crate::logging::ShutdownEvent{ id: self.id });
            }
            self.operator = None;
            self.name = format!("{}(tombstone)", self.name);
        }
    }

    /// Extracts shared progress information and converts to pointstamp changes.
    fn extract_progress(&mut self, pointstamps: &mut ChangeBatch<(Location, T)>, temp_active: &mut BinaryHeap<Reverse<usize>>, _pipeline_scope: bool, _pipeline_outputs_pointstamps: &mut ChangeBatch<(Target, T)>) {

        let shared_progress = &mut *self.shared_progress.borrow_mut();

        // consumes the shared progress
        // Migrate consumeds, internals, produceds into progress statements.
        for (input, consumed) in shared_progress.consumeds.iter_mut().enumerate() {
            let target = Location::new_target(self.index, input);
            // Note that we call drain() to drain the updates
            for (time, delta) in consumed.drain() {
                // consumes messages
                // decrease the occurrence counts for the input port that receives the messages
                pointstamps.update((target, time), -delta);
            }
        }
        for (output, internal) in shared_progress.internals.iter_mut().enumerate() {
            let source = Location::new_source(self.index, output);
            for (time, delta) in internal.drain() {
                // changes to capabilities, update the pointstamps for the output port
                pointstamps.update((source, time.clone()), delta);
            }
        }
        for (output, produced) in shared_progress.produceds.iter_mut().enumerate() {
            for (time, delta) in produced.drain() {
                for target in &self.edges[output] {
                    // send messages
                    // increase the occurrence counts for all the input ports this output port connects to
                    // UPDATE: do not propagate #produeced messages as frontier updates to the output pipeline
                    // if target.node == 0 && pipeline_scope {
                    //    pipeline_outputs_pointstamps.update((*target, time.clone()), delta);
                    // }
                    pointstamps.update((Location::from(*target), time.clone()), delta);
                    temp_active.push(Reverse(target.node));
                }
            }
        }
    }

    /// Test the validity of `self.shared_progress`.
    ///
    /// The validity of shared progress information depends on both the external frontiers and the
    /// internal capabilities, as events can occur that cannot be explained locally otherwise.
    #[allow(dead_code)]
    fn validate_progress(&mut self, child_state: &reachability::PerOperator<T>) {

        let shared_progress = &mut *self.shared_progress.borrow_mut();

        // Increments to internal capabilities require a consumed input message
        // or it can be obtained from the frontier.
        // if time `t` is greater than the timestamps in the frontier (precursor)
        // the output port would have the capability to send messages with timestamp `t`.
        for (output, internal) in shared_progress.internals.iter_mut().enumerate() {
            for (time, diff) in internal.iter() {
                if *diff > 0 {
                    let consumed = shared_progress.consumeds.iter_mut().any(|x| x.iter().any(|(t,d)| *d > 0 && t.less_equal(time)));
                    let internal = child_state.sources[output].implications.less_equal(time);
                    if !consumed && !internal {
                        println!("Increment at {:?}, not supported by\n\tconsumed: {:?}\n\tinternal: {:?}", time, shared_progress.consumeds, child_state.sources[output].implications);
                        panic!("Progress error; internal {:?}", self.name);
                    }
                }
            }
        }
        // It is the same for produced messages
        for (output, produced) in shared_progress.produceds.iter_mut().enumerate() {
            for (time, diff) in produced.iter() {
                if *diff > 0 {
                    let consumed = shared_progress.consumeds.iter_mut().any(|x| x.iter().any(|(t,d)| *d > 0 && t.less_equal(time)));
                    let internal = child_state.sources[output].implications.less_equal(time);
                    if !consumed && !internal {
                        println!("Increment at {:?}, not supported by\n\tconsumed: {:?}\n\tinternal: {:?}", time, shared_progress.consumeds, child_state.sources[output].implications);
                        panic!("Progress error; produced {:?}", self.name);
                    }
                }
            }
        }
    }
}

// Explicitly shut down the operator to get logged information.
impl<T: Timestamp> Drop for PerOperatorState<T> {
    fn drop(&mut self) {
        self.shut_down();
    }
}
