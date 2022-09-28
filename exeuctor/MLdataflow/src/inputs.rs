use std::any::Any;
use std::collections::VecDeque;
use std::marker::PhantomData;

use timely::Data;
use timely::dataflow::operators::Input;
use timely::dataflow::{InputHandle, Scope, ScopeParent};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use crate::Result;
use crate::node::GenericStream;

pub(crate) trait InputBuilder<T: Timestamp, S: Scope> 
where S: Scope + ScopeParent<Timestamp = T>
{
    fn new_input_from_registry<D: Data, L>(&mut self, registry: &mut InputRegistry<T, D, L, S>) -> Box<dyn GenericStream>
    where L: FnMut(&D, &T) -> T + 'static;
}

impl<T: Timestamp + TotalOrder, S> InputBuilder<T, S> for S 
where S: Scope + ScopeParent<Timestamp=T> + 'static {
    fn new_input_from_registry<D: Data, L>(&mut self, registry: &mut InputRegistry<T, D, L, S>) -> Box<dyn GenericStream>
    where L: FnMut(&D, &T) -> T + 'static
    {
        let (handle, stream) = self.new_input();
        registry.handle = Some(handle);
        Box::new(stream)
    }
}

pub(crate) trait GenericScope {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<S: Scope + 'static> GenericScope for S {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any{
        self
    }
}

pub(crate) trait InputFeeder {
    fn build_stream(&mut self, scope: &mut dyn GenericScope) -> Result<Box<dyn GenericStream>>;
    fn step(&mut self) -> Result<bool>;
}

pub(crate) struct InputRegistry<T: Timestamp, D: Data, L, S: Scope>
where 
    L: FnMut(&D, &T) -> T + 'static,
    S: ScopeParent<Timestamp = T> + 'static
{
    index: usize,
    data_stream: VecDeque<D>,
    advance_logic: L,
    handle: Option<InputHandle<T, D>>,
    phantom: PhantomData<T>,
    phantom_scope: PhantomData<S>
}


impl<T: Timestamp + TotalOrder, D: Data, L, S> InputRegistry<T, D, L, S>
where
    L: FnMut(&D, &T) -> T + 'static,
    S: Scope + ScopeParent<Timestamp = T> + 'static
{
    pub fn new(index: usize, data_stream: VecDeque<D>, advance_logic: L) -> Self {
        InputRegistry {
            index,
            data_stream,
            advance_logic,
            handle: None,
            phantom: PhantomData,
            phantom_scope: PhantomData
        }
    }
}

impl<T: Timestamp + TotalOrder, D: Data, L, S> InputFeeder for InputRegistry<T, D, L, S>
where
    L: FnMut(&D, &T) -> T + 'static,
    S: Scope + ScopeParent<Timestamp = T> + 'static
{
    fn build_stream(&mut self, scope: &mut dyn GenericScope) -> Result<Box<dyn GenericStream>> {
        let scope = scope.as_any_mut().downcast_mut::<S>()
            .ok_or("unable to downcast in GenericScope building input stream.")?;
        Ok(scope.new_input_from_registry(self))
    }

    fn step(&mut self) -> Result<bool> {
        let handle = self.handle.as_mut().ok_or("unable to obtain InputHandle")?;
        let data = self.data_stream.pop_front();
        match data {
            Some(data) => {
                let curr_time = handle.time().to_owned();
                let step_timestamp = (self.advance_logic)(&data, &curr_time);
                handle.send(data);
                if curr_time.less_than(&step_timestamp) {
                    handle.advance_to(step_timestamp)
                }
                else if step_timestamp.less_than(&curr_time) {
                    return Err("timestamp to advance to is less than current timestamp")
                }
                Ok(true)
            },
            None => {
                self.handle = None;
                Ok(false)
            }
        }
    }
}