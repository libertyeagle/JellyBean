use std::marker::PhantomData;

use timely::communication::RelayConnectAllocate;
use timely::{Data, ExchangeData};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;

use crate::node::{MapNode, FlatMapNode, BatchedMapNode, BufferedMapNode};
use crate::graph::{GraphNode::ExchangeComputeNode, GraphNode::LocalComputeNode};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::Handle;

pub trait Map<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn map<D2: ExchangeData, L: FnMut(D) -> D2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D2>;
    fn flat_map<I: IntoIterator + 'static, L: FnMut(D) -> I + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, I::Item> where I::Item: ExchangeData;
    fn batch_map<D2: ExchangeData, I2: IntoIterator<Item=D2> + 'static, L: FnMut(Vec<D>) -> I2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D2>;    
    fn buffered_map<D2: ExchangeData, L: FnMut(D) -> D2 + 'static>(&self, logic: L,  name: &str) -> Handle<'a, T, S, D2>;
}

pub trait MapLocal<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn map_local<D2: Data, L: FnMut(D) -> D2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D2>;
    fn flat_map_local<I: IntoIterator + 'static, L: FnMut(D) -> I + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, I::Item> where I::Item: Data;
    fn batch_map_local<D2: Data, I2: IntoIterator<Item=D2> + 'static, L: FnMut(Vec<D>) -> I2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D2>;
    fn buffered_map_local<D2: Data, L: FnMut(D) -> D2 + 'static>(&self, logic: L,  name: &str) -> Handle<'a, T, S, D2>;
}

impl<'a, T, A, D> Map<'a, T, PipelineScope<A, T>, D> for Handle<'a, T, PipelineScope<A, T>, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    A: RelayConnectAllocate + 'static,
    D: Data
{
    fn map<D2: ExchangeData, L: FnMut(D) -> D2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D2> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = MapNode::<_, _, _, PipelineScope<A, T>>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }
    
    fn flat_map<I: IntoIterator + 'static, L: FnMut(D) -> I + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, I::Item> where I::Item: ExchangeData {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = FlatMapNode::<_, _, _, PipelineScope<A, T>>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn batch_map<D2: ExchangeData, I2: IntoIterator<Item=D2> + 'static, L: FnMut(Vec<D>) -> I2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D2> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = BatchedMapNode::<_, _, _, _, PipelineScope<A, T>>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn buffered_map<D2: ExchangeData, L: FnMut(D) -> D2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D2> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = BufferedMapNode::<_, _, _, PipelineScope<A, T>>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }
}

impl<'a, T, S, D> MapLocal<'a, T, S, D> for Handle<'a, T, S, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn map_local<D2: Data, L: FnMut(D) -> D2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D2> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = MapNode::<_, _, _, S>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn flat_map_local<I: IntoIterator + 'static, L: FnMut(D) -> I + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, I::Item>
    where I::Item: Data {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = FlatMapNode::<_, _, _, S>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn batch_map_local<D2: Data, I2: IntoIterator<Item=D2> + 'static, L: FnMut(Vec<D>) -> I2 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D2> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = BatchedMapNode::<_, _, _, _, S>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn buffered_map_local<D2: Data, L: FnMut(D) -> D2 + 'static>(&self, logic: L,  name: &str) -> Handle<'a, T, S, D2> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = BufferedMapNode::<_, _, _, S>::new(prev_id, logic);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.to_owned(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }
}