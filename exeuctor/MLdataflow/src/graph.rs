use std::collections::HashMap;
use std::marker::PhantomData;

use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use crate::input::{GenericInputFeeder, ExchangeGenericInputFeeder};
use crate::node::{LocalOpBuilder, ExchangeOpBuilder};

pub enum GraphNode
{
    LocalInputNode(Box<dyn GenericInputFeeder>),
    ExchangeInputNode(Box<dyn ExchangeGenericInputFeeder>),
    LocalComputeNode(Box<dyn LocalOpBuilder>),
    ExchangeComputeNode(Box<dyn ExchangeOpBuilder>),
}

pub struct ComputeGraph<T>
where
    T: Timestamp + Refines<()> + TotalOrder 
{
    pub operators: HashMap<usize, GraphNode>,
    pub input_operator_indices: Vec<usize>,
    pub(crate) op_name_local_id_mapping: HashMap<String, usize>,
    pub(crate) op_local_id_name_mapping: HashMap<usize, String>,
    phantom_timestamp: PhantomData<T>
}

impl<T> ComputeGraph<T>
where
    T: Timestamp + Refines<()> + TotalOrder 
{
    pub fn new() -> Self {
        ComputeGraph {
            operators: HashMap::new(),
            input_operator_indices: Vec::new(),
            op_name_local_id_mapping: HashMap::new(),
            op_local_id_name_mapping: HashMap::new(),
            phantom_timestamp: PhantomData
        }
    }
}

#[derive(Clone, Debug)]
pub struct GraphConnections {
    pub outgoing_edges: HashMap<usize, Vec<usize>>,
    pub incoming_edges: HashMap<usize, Vec<usize>>
}