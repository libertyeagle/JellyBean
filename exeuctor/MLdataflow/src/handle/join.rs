use std::hash::Hash;
use std::marker::PhantomData;

use timely::communication::RelayConnectAllocate;
use timely::{Data, ExchangeData};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;

use crate::graph::{GraphNode::ExchangeComputeNode, GraphNode::LocalComputeNode};
use crate::node::{JoinNode, TimestampJoinNode, SingleItemJoinNode, TimestampSingleItemJoinNode};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::Handle;

pub trait Join<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: ExchangeData
{
    fn join<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Clone+ Eq + Hash + 'static,
        H1: Fn(&D) -> (K, usize) + 'static,
        H2: Fn(&D2) -> (K, usize) + 'static;

    fn timestamp_join<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static;

    fn concat<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static;

    fn timestamp_concat<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static;
}

pub trait JoinLocal<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn join_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Clone+ Eq + Hash + 'static,
        H1: Fn(&D) -> (K, usize) + 'static,
        H2: Fn(&D2) -> (K, usize) + 'static;

    fn timestamp_join_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static;

    fn concat_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static;

    fn timestamp_concat_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static;
}

impl<'a, T, A, D> Join<'a, T, PipelineScope<A, T>, D> for Handle<'a, T, PipelineScope<A, T>, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    A: RelayConnectAllocate + 'static,
    D: ExchangeData
{
    fn join<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, PipelineScope<A, T>, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, PipelineScope<A, T>, (D, D2)>
    where
        K: Clone+ Eq + Hash + 'static,
        H1: Fn(&D) -> (K, usize) + 'static,
        H2: Fn(&D2) -> (K, usize) + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = JoinNode::<_, _, _, _, _, PipelineScope<A, T>>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

    fn timestamp_join<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, PipelineScope<A, T>, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, PipelineScope<A, T>, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampJoinNode::<_, _, _, _, _, PipelineScope<A, T>>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

    fn concat<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, PipelineScope<A, T>, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, PipelineScope<A, T>, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = SingleItemJoinNode::<_, _, _, _, _, PipelineScope<A, T>>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

    fn timestamp_concat<D2: ExchangeData, K, H1, H2>(&self, right_handle: &Handle<'a, T, PipelineScope<A, T>, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, PipelineScope<A, T>, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampSingleItemJoinNode::<_, _, _, _, _, PipelineScope<A, T>>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

impl<'a, T, S, D> JoinLocal<'a, T, S, D> for Handle<'a, T, S, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn join_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Clone+ Eq + Hash + 'static,
        H1: Fn(&D) -> (K, usize) + 'static,
        H2: Fn(&D2) -> (K, usize) + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = JoinNode::<_, _, _, _, _, S>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

    fn timestamp_join_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampJoinNode::<_, _, _, _, _, S>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

    fn concat_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = SingleItemJoinNode::<_, _, _, _, _, S>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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

    fn timestamp_concat_local<D2: Data, K, H1, H2>(&self, right_handle: &Handle<'a, T, S, D2>, hash_left: H1, hash_right: H2, name: &str) -> Handle<'a, T, S, (D, D2)>
    where
        K: Eq + Hash + 'static,
        H1: Fn(&D) -> K + 'static,
        H2: Fn(&D2) -> K + 'static
    {
        let left_id = self.id;
        let right_id = right_handle.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampSingleItemJoinNode::<_, _, _, _, _, S>::new(
            left_id,
            right_id,
            hash_left,
            hash_right
        );
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