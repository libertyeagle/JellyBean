//! The pipeline and pipeline builder in order to connect input pipelines and output pipelines
//! These are the wrappers for SubgraphBuilder and Subgraph

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::communication::Pull;
use crate::Data;
use crate::dataflow::channels::Bundle;
use crate::dataflow::channels::connector::{Connector, PushPullConnector};
use crate::dataflow::channels::pushers::{Counter as PushCounter, TeeHelper};
use crate::dataflow::channels::pushers::tee::PipelineInputTee;
use crate::logging::TimelyLogger as Logger;
use crate::logging::TimelyProgressLogger as ProgressLogger;
use crate::logging::TimelyRelayProgressLogger as RelayProgressLogger;
use crate::progress::{Antichain, ChangeBatch, Operate, Source, Subgraph, SubgraphBuilder, Target, Timestamp};
use crate::progress::frontier_relay::{FrontierRelay, MultiChannelFrontierRelay, SingleChannelFrontierRelay};
use crate::progress::operate::SharedProgress;
use crate::progress::timestamp::Refines;
use crate::scheduling::Schedule;
use crate::worker::{AsWorker, RelayConnector};

/// The builder for the pipeline level subgraph
/// It warps a subgraph builder
pub struct PipelineBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp,
{
    /// The name of the pipeline
    pub name: String,
    /// The address of the pipeline
    pub path: Vec<usize>,
    // wrapped subgraph builder
    subgraph_builder: SubgraphBuilder<TOuter, TInner>,
    // scope inputs index -> input port id in subgraph mapping
    input_index_id_map: HashMap<usize, usize>,
    // scope output id in subgraph -> output index mapping
    output_id_index_map: HashMap<usize, usize>,
    // relay connectors to pull data from relay node and
    // push to the corresponding pusher (Tee) for the input port
    pipeline_input_connectors: Vec<Box<dyn Connector>>,
    // Logger to send/receive progress updates from relay nodes
    relay_progress_logging: Option<RelayProgressLogger>,
    num_inputs: usize,
}

impl<TOuter, TInner> PipelineBuilder<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
{
    /// Allocate a new input connected from an input pipeline
    pub fn new_input<D: Data, P: Pull<Bundle<TInner, D>> + 'static>(&mut self, receiver: P, index: usize) -> (Source, TeeHelper<TInner, D>) {
        let (targets, registrar) = PipelineInputTee::<TInner, D>::new();
        let ingress = PushCounter::new(targets);
        let recv_input_counts = ingress.produced().clone();
        let recv_send_connector = PushPullConnector::new(ingress, receiver);
        self.pipeline_input_connectors.push(Box::new(recv_send_connector));

        let port = self.subgraph_builder.new_input(recv_input_counts).port;
        self.input_index_id_map.insert(index, port);
        let source = Source::new(0, port);
        self.num_inputs += 1;
        (source, registrar)
    }

    /// Allocate a new output port
    pub fn new_output(&mut self, index: usize) -> Target {
        let port = self.subgraph_builder.new_output().port;
        self.output_id_index_map.insert(port, index);
        Target::new(0, port)
    }

    /// Connect a Source to a Target
    pub fn connect(&mut self, source: Source, target: Target) {
        self.subgraph_builder.connect(source, target)
    }

    /// Creates a new Pipeline builder
    pub fn new_from(
        index: usize,
        mut path: Vec<usize>,
        logging: Option<Logger>,
        progress_logging: Option<ProgressLogger>,
        relay_progress_logging: Option<RelayProgressLogger>,
        name: &str,
    )
        -> Self
    {
        let subgraph_builder = SubgraphBuilder::new_pipeline_scope_from(
            index,
            path.clone(),
            logging,
            progress_logging.clone(),
            name,
        );
        path.push(index);
        PipelineBuilder {
            name: name.to_owned(),
            path,
            subgraph_builder,
            input_index_id_map: HashMap::new(),
            output_id_index_map: HashMap::new(),
            pipeline_input_connectors: Vec::new(),
            relay_progress_logging,
            num_inputs: 0,
        }
    }

    /// Allocate a new child identifier
    pub fn allocate_child_id(&mut self) -> usize {
        self.subgraph_builder.allocate_child_id()
    }

    /// Add a new child to the pipeline
    pub fn add_child(&mut self, child: Box<dyn Operate<TInner>>, index: usize, identifier: usize) {
        self.subgraph_builder.add_child(child, index, identifier);
    }

    /// Build the pipeline, block the input ports' progress (frontier)
    /// to wait for the initial frontiers
    pub fn build_with_multi_channel_relay<A: RelayConnector + AsWorker + Clone, H>(self, worker: &mut A, mapper_fn: H) -> Pipeline<TOuter, TInner, MultiChannelFrontierRelay<TInner, H>, A>
    where
        H: FnMut(usize, usize) -> usize
    {
        let mut subgraph = self.subgraph_builder.build(worker);

        let frontier_relay = MultiChannelFrontierRelay::new(
            worker,
            self.input_index_id_map,
            self.output_id_index_map,
            mapper_fn,
            self.relay_progress_logging
        );

        let num_inputs = subgraph.inputs();
        let mut blocked_inputs = HashSet::with_capacity(num_inputs);
        for port in 0..num_inputs {
            subgraph.block_input_progress(port);
            blocked_inputs.insert(port);
        }
        Pipeline {
            worker: worker.clone(),
            blocked_inputs,
            subgraph,
            staged_input_frontier_changes: ChangeBatch::new(),
            staged_output_frontier_changes: ChangeBatch::new(),
            frontier_relay,
            input_frontier_started: Vec::new(),
            pipeline_input_connectors: self.pipeline_input_connectors
        }
    }

    pub fn build_with_single_channel_relay<A: RelayConnector + AsWorker + Clone>(self, worker: &mut A) -> Pipeline<TOuter, TInner, SingleChannelFrontierRelay<TInner>, A> {
        let mut subgraph = self.subgraph_builder.build(worker);
        let frontier_relay = SingleChannelFrontierRelay::new(
            worker,
            self.input_index_id_map,
            self.output_id_index_map,
            self.relay_progress_logging
        );

        let num_inputs = subgraph.inputs();
        let mut blocked_inputs = HashSet::with_capacity(num_inputs);
        for port in 0..num_inputs {
            subgraph.block_input_progress(port);
            blocked_inputs.insert(port);
        }
        Pipeline {
            worker: worker.clone(),
            blocked_inputs,
            subgraph,
            staged_input_frontier_changes: ChangeBatch::new(),
            staged_output_frontier_changes: ChangeBatch::new(),
            frontier_relay,
            input_frontier_started: Vec::new(),
            pipeline_input_connectors: self.pipeline_input_connectors
        }
    }

    /// Returns the current #inputs (note that the dataflow graph is still under construction).
    pub fn num_inputs(&self) -> usize { self.num_inputs }
}


/// The actual pipeline (root-level subgraph)
/// which wraps a Subgraph
/// when schedule(), it takes the input frontier changes
/// pulls input data from input pipelines
/// It also send the output frontier changes
pub struct Pipeline<TOuter, TInner, R, A>
where
    A: RelayConnector,
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
    R: FrontierRelay<TInner>
{
    worker: A,
    blocked_inputs: HashSet<usize>,
    subgraph: Subgraph<TOuter, TInner>,
    staged_input_frontier_changes: ChangeBatch<(usize, TInner)>,
    staged_output_frontier_changes: ChangeBatch<(Target, TInner)>,
    frontier_relay: R,
    input_frontier_started: Vec<usize>,
    pipeline_input_connectors: Vec<Box<dyn Connector>>
}

impl<TOuter, TInner, R, A> Schedule for Pipeline<TOuter, TInner, R, A>
where
    A: RelayConnector,
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
    R: FrontierRelay<TInner>
{
    fn name(&self) -> &str {
        self.subgraph.name()
    }

    fn path(&self) -> &[usize] {
        self.subgraph.path()
    }

    fn schedule(&mut self) -> bool {
        // receive input frontier changes
        let bootstraped = self.frontier_relay.recv(&mut self.staged_input_frontier_changes, &mut self.input_frontier_started);

        // pull and relay the data received from input pipelines
        for connector in self.pipeline_input_connectors.iter_mut() {
            connector.relay_messages();
        }

        // remove the pointstamp with minimal timestamp at the input port idx of operator 0
        for idx in self.input_frontier_started.drain(..) {
            if self.blocked_inputs.contains(&idx) {
                self.subgraph.unblock_input_progress(idx, &mut self.staged_input_frontier_changes);
                self.blocked_inputs.remove(&idx);
            }
        }
        // still have not received any frontier updates
        // even after all messages from input pipelines are received
        // this could happen when the input pipelines' frontier updates are merged
        // (e.g., (timestamp 0, +1), (timestamp 0, -1) -> nothing)
        if self.worker.relay_input_pipelines_completed() {
            for idx in 0 .. self.subgraph.inputs() {
                if self.blocked_inputs.contains(&idx) {
                    self.subgraph.unblock_input_progress(idx, &mut self.staged_input_frontier_changes);
                    self.blocked_inputs.remove(&idx);
                }
            }
        }
        let frontier_empty = self.subgraph.peek_pipeline_inputs_frontier_is_empty(&mut self.staged_input_frontier_changes);
        if frontier_empty {
            // drain all remaining inputs before update frontiers
            while !self.worker.relay_input_pipelines_completed() {
                // there could be a situation that input pipelines relay threads are still not finished
                // i.e., still executing recv_loop
                // however, we will no longer receive any data
                // so if we await_relay_events() indefinitely, we may stuck forever
                self.worker.receive_from_relay();
                for connector in self.pipeline_input_connectors.iter_mut() {
                    connector.relay_messages();
                }
                if self.worker.activate_paths() {
                    self.subgraph.schedule();
                }
            }

            // deal with remaining frontier changes
            self.frontier_relay.recv(&mut self.staged_input_frontier_changes, &mut self.input_frontier_started);
            for idx in 0 .. self.subgraph.inputs() {
                if self.blocked_inputs.contains(&idx) {
                    self.subgraph.unblock_input_progress(idx, &mut self.staged_input_frontier_changes);
                    self.blocked_inputs.remove(&idx);
                }
            }
        }

        self.subgraph.accept_pipeline_inputs_frontier(&mut self.staged_input_frontier_changes);

        let incomplete = self.subgraph.schedule();
        self.subgraph.pull_pipeline_outputs_frontier_change(&mut self.staged_output_frontier_changes);

        if bootstraped || self.worker.relay_input_pipelines_completed() {
            // send the output frontier changes after we have completed bootstrapping
            // i.e., after we have received the initial frontiers from all scope inputs
            self.frontier_relay.send(&mut self.staged_output_frontier_changes);
        }

        incomplete
    }
    
}

impl<TOuter, TInner, R, A> Operate<TOuter> for Pipeline<TOuter, TInner, R, A>
where
    A: RelayConnector,
    TOuter: Timestamp,
    TInner: Timestamp+Refines<TOuter>,
    R: FrontierRelay<TInner>
{
    fn local(&self) -> bool {
        false
    }
    fn inputs(&self)  -> usize {
        self.subgraph.inputs()
    }
    fn outputs(&self) -> usize {
        self.subgraph.outputs()
    }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Rc<RefCell<SharedProgress<TOuter>>>) {
        self.subgraph.get_internal_summary()
    }

    fn set_external_summary(&mut self) {
        self.subgraph.set_external_summary();
    }
}