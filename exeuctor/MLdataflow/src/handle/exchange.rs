use std::marker::PhantomData;

use timely::communication::RelayConnectAllocate;
use timely::{Data, ExchangeData};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;

use crate::node::ExchangeNode;
use crate::graph::GraphNode::ExchangeComputeNode;
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::Handle;

pub trait Exchange<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    fn intra_pipeline_exchange<L: Fn(&D) -> u64 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, S, D>;
    fn intra_pipeline_gather(&self, target_worker_index: u64, name: &str) -> Handle<'a, T, S, D>;
}

impl<'a, T, A, D> Exchange<'a, T, PipelineScope<A, T>, D> for Handle<'a, T, PipelineScope<A, T>, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    A: RelayConnectAllocate + 'static,
    D: ExchangeData
{
    fn intra_pipeline_exchange<L: Fn(&D) -> u64 + 'static>(&self, logic: L, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D> {
        let prev_id = self.id;
        let next_id = self.allocate_new_operator_id();
        let node = ExchangeNode::<_, _, PipelineScope<A, T>>::new(prev_id, logic);
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

    fn intra_pipeline_gather(&self, target_worker_index: u64, name: &str) -> Handle<'a, T, PipelineScope<A, T>, D> {
        self.intra_pipeline_exchange(move |_| target_worker_index, name)        
    }
}