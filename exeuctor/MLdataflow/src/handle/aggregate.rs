use std::hash::Hash;
use std::marker::PhantomData;

use timely::communication::RelayConnectAllocate;
use timely::{Data, ExchangeData};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;

use crate::graph::{GraphNode::ExchangeComputeNode, GraphNode::LocalComputeNode};
use crate::node::{AggregateNode, TimestampAggregateNode, IncrementalAggregateNode, TimestampIncrementalAggregateNode};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::Handle;

pub trait Aggregate<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn aggregate<D2: ExchangeData, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, S, D2>
    where
        K: Clone + Hash + Eq + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static;

    fn timestamp_aggregate<D2: ExchangeData, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, S, D2>
    where
        K: Eq + Hash + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static;        
    
    fn incremental_aggregate<Di: Default + 'static, R: ExchangeData, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, S, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) -> bool + 'static,
        E: Fn(K, Di) -> R + 'static;
    
    fn timestamp_incremental_aggregate<Di: Default + 'static, R: ExchangeData, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, S, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) + 'static,
        E: Fn(K, Di) -> R + 'static;
}

pub trait AggregateLocal<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn aggregate_local<D2: Data, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, S, D2>
    where
        K: Clone + Hash + Eq + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static;

    fn timestamp_aggregate_local<D2: Data, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, S, D2>
    where
        K: Eq + Hash + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static;        
    
    fn incremental_aggregate_local<Di: Default + 'static, R: Data, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, S, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) -> bool + 'static,
        E: Fn(K, Di) -> R + 'static;
    
    fn timestamp_incremental_aggregate_local<Di: Default + 'static, R: Data, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, S, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) + 'static,
        E: Fn(K, Di) -> R + 'static;
}

impl<'a, T, A, D> Aggregate<'a, T, PipelineScope<A, T>, D> for Handle<'a, T, PipelineScope<A, T>, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    A: RelayConnectAllocate + 'static,
    D: Data
{
    fn aggregate<D2: ExchangeData, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D2>
    where
        K: Clone + Hash + Eq + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = AggregateNode::<_, _, _, _, _, PipelineScope<A, T>>::new(prev_id, hash, agg);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn timestamp_aggregate<D2: ExchangeData, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D2>
    where
        K: Eq + Hash + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampAggregateNode::<_, _, _, _, _, PipelineScope<A, T>>::new(prev_id, hash, agg);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn incremental_aggregate<Di: Default + 'static, R: ExchangeData, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, PipelineScope<A, T>, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) -> bool + 'static,
        E: Fn(K, Di) -> R + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = IncrementalAggregateNode::<_, _, _, _, _, _, _, PipelineScope<A, T>>::new(prev_id, hash, fold, emit);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn timestamp_incremental_aggregate<Di: Default + 'static, R: ExchangeData, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, PipelineScope<A, T>, R>
    where
            K: Clone + Hash + Eq + 'static,
            H: Fn(&D) -> K + 'static,
            F: Fn(&K, D, &mut Di) + 'static,
            E: Fn(K, Di) -> R + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampIncrementalAggregateNode::<_, _, _, _, _, _, _, PipelineScope<A, T>>::new(prev_id, hash, fold, emit);
        self.graph.borrow_mut().operators.insert(next_id, ExchangeComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
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

impl<'a, T, S, D> AggregateLocal<'a, T, S, D> for Handle<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn aggregate_local<D2: Data, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, S, D2>
    where
        K: Clone + Hash + Eq + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = AggregateNode::<_, _, _, _, _, S>::new(prev_id, hash, agg);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn timestamp_aggregate_local<D2: Data, K, L, H>(&self, hash: H, agg: L, name: &str) -> Handle<'a, T, S, D2>
    where
        K: Eq + Hash + 'static,
        L: FnMut(Vec<D>) -> D2 + 'static,
        H: Fn(&D) -> (K, usize) + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampAggregateNode::<_, _, _, _, _, S>::new(prev_id, hash, agg);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn incremental_aggregate_local<Di: Default + 'static, R: Data, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, S, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) -> bool + 'static,
        E: Fn(K, Di) -> R + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = IncrementalAggregateNode::<_, _, _, _, _, _, _, S>::new(prev_id, hash, fold, emit);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
        self.graph.borrow_mut().op_local_id_name_mapping.insert(next_id, unique_name);
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: next_id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData
        }
    }

    fn timestamp_incremental_aggregate_local<Di: Default + 'static, R: Data, K, H, F, E>(&self, hash: H, fold: F, emit: E, name: &str) -> Handle<'a, T, S, R>
    where
        K: Clone + Hash + Eq + 'static,
        H: Fn(&D) -> K + 'static,
        F: Fn(&K, D, &mut Di) + 'static,
        E: Fn(K, Di) -> R + 'static
    {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = TimestampIncrementalAggregateNode::<_, _, _, _, _, _, _, S>::new(prev_id, hash, fold, emit);
        self.graph.borrow_mut().operators.insert(next_id, LocalComputeNode(Box::new(node)));
        let mut duplicate = 0;
        let mut unique_name = name.to_owned();
        while self.graph.borrow().op_name_local_id_mapping.contains_key(&unique_name) {
            duplicate += 1;
            unique_name = format!("{}_{}", name, duplicate);
        }
        self.graph.borrow_mut().op_name_local_id_mapping.insert(unique_name.clone(), next_id);
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