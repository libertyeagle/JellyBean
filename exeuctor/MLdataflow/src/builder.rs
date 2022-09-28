use std::any::Any;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use timely::{ExchangeData, Data};
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;
use timely::communication::allocator::{Generic, GenericToRelay};


use crate::graph::ComputeGraph;
use crate::graph::{GraphNode::LocalInputNode, GraphNode::ExchangeInputNode};
use crate::handle::Handle;
use crate::input::{ClosureInputSource, ContainedInputSource, WorkerDistributedContainedInputSource, BufferedWorkerDistributedContainedInputSource, BufferedContainedInputSource};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;
use crate::static_timely::timely_static_scope::Child;
use crate::static_timely::timely_static_worker::Worker;

pub struct PipelineGraphBuilder<T> 
where
    T: Timestamp + Refines<()> + TotalOrder 
{
    graph: RefCell<ComputeGraph<T>>,
    counter: RefCell<usize>,
    pipeline_index: usize,
    worker_index: usize,
    worker_peers: usize,
    assigned_operators: Option<Vec<String>>,
    builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>>
}

impl<T> PipelineGraphBuilder<T> 
where
    T: Timestamp + Refines<()> + TotalOrder
{
    pub(crate) fn new(
        pipeline_index: usize, 
        worker_index: usize, 
        worker_peers: usize, 
        assigned_ops: Option<Vec<String>>, 
        builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>>
    ) -> Self {
        PipelineGraphBuilder {
            graph: RefCell::new(ComputeGraph::new()),
            counter: RefCell::new(0),
            pipeline_index,
            worker_index,
            worker_peers,
            assigned_operators: assigned_ops,
            builder_configs
        }
    }

    pub(crate) fn get_graph(self) -> ComputeGraph<T> {
        let graph = self.graph;
        graph.into_inner()
    }

    pub fn get_config<D: 'static + Send + Sync>(&self, key: &str) -> Option<&D> {
        self.builder_configs.get(key).and_then(|val| val.downcast_ref())
    }

    pub fn get_assigned_operators(&self) -> Option<&Vec<String>> {
        self.assigned_operators.as_ref()
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn worker_peers(&self) -> usize {
        self.worker_peers
    }

    pub fn pipeline_index(&self) -> usize {
        self.pipeline_index
    }
 
    pub fn new_input<'a, D, L>(&'a self, emit_logic: L, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D>
    where 
        D: ExchangeData,
        L: FnMut() -> (Option<D>, Option<T>) + 'static,
    {
        let input_node = ClosureInputSource::<_, _, L, PipelineScope<GenericToRelay, T>>::new(emit_logic);
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, ExchangeInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }
    

    pub fn new_local_input<'a, D, L>(&'a self, emit_logic: L, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D>
    where 
        D: Data,
        L: FnMut() -> (Option<D>, Option<T>) + 'static,
    {
        let input_node = ClosureInputSource::<_, _, L, PipelineScope<GenericToRelay, T>>::new(emit_logic);
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_input_from_source<'a, D, L>(&'a self, data_stream: VecDeque<D>, advance_logic: L, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D>
    where
        D: ExchangeData,
        L: FnMut(&D, &T) -> T + 'static,
    {
        let input_node = ContainedInputSource::<_, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            advance_logic
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, ExchangeInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }


    pub fn new_local_input_from_source<'a, D, L>(&'a self, data_stream: VecDeque<D>, advance_logic: L, name: &str)  -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D>
    where
        D: Data,
        L: FnMut(&D, &T) -> T + 'static,
    {
        let input_node = ContainedInputSource::<_, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            advance_logic
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_input_from_source_distributed<'a, D, L>(&'a self, data_stream: VecDeque<D>, advance_logic: L, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D>
    where
        D: ExchangeData,
        L: FnMut(&D, &T) -> T + 'static,
    {
        let input_node = WorkerDistributedContainedInputSource::<_, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            advance_logic,
            self.worker_index,
            self.worker_peers
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, ExchangeInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_local_input_from_source_distributed<'a, D, L>(&'a self, data_stream: VecDeque<D>, advance_logic: L, name: &str)  -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D>
    where
        D: Data,
        L: FnMut(&D, &T) -> T + 'static,
    {
        let input_node = WorkerDistributedContainedInputSource::<_, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            advance_logic,
            self.worker_index,
            self.worker_peers
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_input_buffered_from_source<'a, D, D2, L>(&'a self, data_stream: VecDeque<D>, emit_logic: L, buffer_size: usize, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D2>
    where
        D: Data,
        D2: ExchangeData,
        L: FnMut(D) -> (D2, T) + 'static,
    {
        let input_node = BufferedContainedInputSource::<_, _, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            emit_logic,
            buffer_size
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, ExchangeInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_local_input_buffered_from_source<'a, D, D2, L>(&'a self, data_stream: VecDeque<D>, emit_logic: L, buffer_size: usize, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D2>
    where
        D: Data,
        D2: Data,
        L: FnMut(D) -> (D2, T) + 'static,
    {
        let input_node = BufferedContainedInputSource::<_, _, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            emit_logic,
            buffer_size
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }    

    pub fn new_input_buffered_from_source_distributed<'a, D, D2, L>(&'a self, data_stream: VecDeque<D>, emit_logic: L, buffer_size: usize, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D2>
    where
        D: Data,
        D2: ExchangeData,
        L: FnMut(D) -> (D2, T) + 'static,
    {
        let input_node = BufferedWorkerDistributedContainedInputSource::<_, _, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            emit_logic,
            self.worker_index,
            self.worker_peers,
            buffer_size
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, ExchangeInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_local_input_buffered_from_source_distributed<'a, D, D2, L>(&'a self, data_stream: VecDeque<D>, emit_logic: L, buffer_size: usize, name: &str) -> Handle<'a, T, PipelineScope<GenericToRelay, T>, D2>
    where
        D: Data,
        D2: Data,
        L: FnMut(D) -> (D2, T) + 'static,
    {
        let input_node = BufferedWorkerDistributedContainedInputSource::<_, _, _, L, PipelineScope<GenericToRelay, T>>::new(
            data_stream,
            emit_logic,
            self.worker_index,
            self.worker_peers,
            buffer_size
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }
}

pub struct GraphBuilder<T> 
where
    T: Timestamp + Refines<()> + TotalOrder 
{
    graph: RefCell<ComputeGraph<T>>,
    counter: RefCell<usize>,
    worker_index: usize,
    worker_peers: usize
}

impl<T> GraphBuilder<T>
where
    T: Timestamp + Refines<()> + TotalOrder
{
    pub fn new(worker_index: usize, worker_peers: usize) -> Self {
        GraphBuilder {
            graph: RefCell::new(ComputeGraph::new()),
            counter: RefCell::new(0),
            worker_index,
            worker_peers
        }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub(crate) fn get_graph(self) -> ComputeGraph<T> {
        let graph = self.graph;
        graph.into_inner()
    }

    pub fn new_input<'a, D, L>(&'a self, emit_logic: L, name: &str) -> Handle<'a, T, Child<Worker<Generic>, T>, D>
    where 
        D: Data,
        L: FnMut() -> (Option<D>, Option<T>) + 'static,
    {
        let input_node = ClosureInputSource::<_, _, L, Child<Worker<Generic>, T>>::new(emit_logic);
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_input_from_source<'a, D, L>(&'a self, data_stream: VecDeque<D>, advance_logic: L, name: &str)  -> Handle<'a, T, Child<Worker<Generic>, T>, D>
    where
        D: Data,
        L: FnMut(&D, &T) -> T + 'static,
    {
        let input_node = ContainedInputSource::<_, _, L, Child<Worker<Generic>, T>>::new(
            data_stream,
            advance_logic
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);
        
        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_input_from_source_distributed<'a, D, L>(&'a self, data_stream: VecDeque<D>, advance_logic: L, name: &str)  -> Handle<'a, T, Child<Worker<Generic>, T>, D>
    where
        D: Data,
        L: FnMut(&D, &T) -> T + 'static,
    {
        let input_node = WorkerDistributedContainedInputSource::<_, _, L, Child<Worker<Generic>, T>>::new(
            data_stream,
            advance_logic,
            self.worker_index,
            self.worker_peers
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);
        
        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }

    pub fn new_input_buffered_from_source<'a, D, D2, L>(&'a self, data_stream: VecDeque<D>, emit_logic: L, buffer_size: usize, name: &str) -> Handle<'a, T, Child<Worker<Generic>, T>, D2>
    where
        D: Data,
        D2: Data,
        L: FnMut(D) -> (D2, T) + 'static,
    {
        let input_node = BufferedContainedInputSource::<_, _, _, L, Child<Worker<Generic>, T>>::new(
            data_stream,
            emit_logic,
            buffer_size
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }    

    pub fn new_input_buffered_from_source_distributed<'a, D, D2, L>(&'a self, data_stream: VecDeque<D>, emit_logic: L, buffer_size: usize, name: &str) -> Handle<'a, T, Child<Worker<Generic>, T>, D2>
    where
        D: Data,
        D2: Data,
        L: FnMut(D) -> (D2, T) + 'static,
    {
        let input_node = BufferedWorkerDistributedContainedInputSource::<_, _, _, L, Child<Worker<Generic>, T>>::new(
            data_stream,
            emit_logic,
            self.worker_index,
            self.worker_peers,
            buffer_size
        );
        let index = *self.counter.borrow();
        self.graph.borrow_mut().operators.insert(index, LocalInputNode(Box::new(input_node)));
        self.graph.borrow_mut().input_operator_indices.push(index);
        *self.counter.borrow_mut() += 1;

        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), index);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(index, unique_name);

        Handle {
            graph: &self.graph,
            counter: &self.counter,
            id: index,
            phantom_scope: PhantomData,
            phantom_data: PhantomData           
        }
    }    
}