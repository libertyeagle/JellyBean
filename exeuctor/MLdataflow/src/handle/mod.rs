use std::cell::RefCell;
use std::marker::PhantomData;

use timely::Data;
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::order::TotalOrder;

pub mod map;
pub mod join;
pub mod aggregate;
pub mod filter;
pub mod inspect;
pub mod exchange;
pub mod union;

pub use map::{Map, MapLocal};
pub use join::{Join, JoinLocal};
pub use aggregate::{Aggregate, AggregateLocal};
pub use filter::{Filter, FilterLocal};
pub use inspect::{Inspect, InspectLocal};
pub use exchange::Exchange;
pub use union::{Union, UnionLocal};

use crate::graph::ComputeGraph;

pub struct Handle<'a, T, S, D> 
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data,
{
    pub(crate) graph: &'a RefCell<ComputeGraph<T>>,
    pub(crate) counter: &'a RefCell<usize>,
    pub(crate) id: usize,
    pub(crate) phantom_scope: PhantomData<S>,
    pub(crate) phantom_data: PhantomData<D>
}


impl<'a, T, S, D> Handle<'a, T, S, D>
where
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data
{
    pub fn get_name(&self) -> String {
        self.graph.borrow().op_local_id_name_mapping.get(&self.id).unwrap().to_owned()
    }

    fn allocate_new_operator_id(&self) -> usize {
        *self.counter.borrow_mut() += 1;
        *self.counter.borrow() - 1
    }
}

impl<'a, T, S, D> Clone for Handle<'a, T, S, D>
where 
    T: Timestamp + Refines<()> + TotalOrder,
    S: Scope<Timestamp = T> + 'static,
    D: Data 
{
    fn clone(&self) -> Self {
        Handle {
            graph: self.graph,
            counter: self.counter,
            id: self.id,
            phantom_scope: PhantomData,
            phantom_data: PhantomData,
        }
    }
}