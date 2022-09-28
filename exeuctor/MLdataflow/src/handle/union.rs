use std::marker::PhantomData;

use timely::communication::RelayConnectAllocate;
use timely::{Data, ExchangeData};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;

use crate::graph::{GraphNode::ExchangeComputeNode, GraphNode::LocalComputeNode};
use crate::node::UnionNode;
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::Handle;

pub trait Union<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: ExchangeData
{
    fn union(&self, source: &Handle<'a, T, S, D>, name: &str) -> Handle<'a, T, S, D>;
    fn union_multi<I: IntoIterator<Item=Handle<'a, T, S, D>>>(&self, sources: I, name: &str) -> Handle<'a, T, S, D>;
}

pub trait UnionLocal<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn union_local(&self, source: &Handle<'a, T, S, D>, name: &str) -> Handle<'a, T, S, D>;
    fn union_multi_local<I: IntoIterator<Item=Handle<'a, T, S, D>>>(&self, sources: I, name: &str) -> Handle<'a, T, S, D>;
}

impl<'a, T, A, D> Union<'a, T, PipelineScope<A, T>, D> for Handle<'a, T, PipelineScope<A, T>, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    A: RelayConnectAllocate + 'static,
    D: ExchangeData
{
    fn union(&self, source: &Handle<'a, T, PipelineScope<A, T>, D>, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D> {
        self.union_multi(vec![self.clone(), source.clone()], name)
    }

    fn union_multi<I: IntoIterator<Item=Handle<'a, T, PipelineScope<A, T>, D>>>(&self, sources: I, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D> {
        let sources_ids = sources.into_iter().map(|x| x.id).collect::<Vec<_>>();
        let next_id = self.allocate_new_operator_id();
        let node = UnionNode::<D, PipelineScope<A, T>>::new(sources_ids);
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

impl<'a, T, S, D> UnionLocal<'a, T, S, D> for Handle<'a, T, S, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn union_local(&self, source: &Handle<'a, T, S, D>, name: &str) -> Handle<'a, T, S, D> {
        self.union_multi_local(vec![self.clone(), source.clone()], name)
    }

    fn union_multi_local<I: IntoIterator<Item=Handle<'a, T, S, D>>>(&self, sources: I, name: &str) -> Handle<'a, T, S, D> {
        let sources_ids = sources.into_iter().map(|x| x.id).collect::<Vec<_>>();
        let next_id = self.allocate_new_operator_id();
        let node = UnionNode::<D, S>::new(sources_ids);
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
