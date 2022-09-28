use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::Utc;
use timely::communication::RelayConnectAllocate;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::{Data, ExchangeData};
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::dataflow::operators::Input;
use timely::dataflow::InputHandle;
use timely::progress::timestamp::Refines;

use crate::TimestampData;
use crate::node::GenericStream;
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::{GenericScope, ExchangeGenericInputFeeder};
use super::GenericInputFeeder;

pub struct ClosureInputSource<T: Timestamp, D: Data, L, S: Scope>
where 
    L: FnMut() -> (Option<D>, Option<T>) + 'static,
    S: ScopeParent<Timestamp = T> + 'static
{
    emit_logic: L,
    request_interval: Option<i64>,
    last_request_timestamp: Option<i64>,
    handle: Option<InputHandle<T, TimestampData<D>>>,
    phantom: PhantomData<T>,
    phantom_scope: PhantomData<S>
}


impl<T: Timestamp + TotalOrder, D: Data, L, S: Scope> ClosureInputSource<T, D, L, S>
where
    L: FnMut() -> (Option<D>, Option<T>) + 'static,
    S: ScopeParent<Timestamp = T> + 'static
{
    pub fn new(emit_logic: L) -> Self {
        ClosureInputSource {
            emit_logic,
            request_interval: None,
            last_request_timestamp: None,
            handle: None,
            phantom: PhantomData,
            phantom_scope: PhantomData
        }
    }
}

impl<T: Timestamp + TotalOrder, D: Data, L, S: Scope> GenericInputFeeder for ClosureInputSource<T, D, L, S>
where
    L: FnMut() -> (Option<D>, Option<T>) + 'static,
    S: ScopeParent<Timestamp = T> + 'static
{
    fn build_stream(&mut self, scope: &mut dyn GenericScope, config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {
        if let Some(config) = config {
            let request_rate = config.get("request_rate").and_then(|val| val.downcast_ref::<f64>()).map(|val| *val);
            self.request_interval = request_rate.map(|x|  (1e9_f64 / x) as i64);
        }
        let scope = scope.as_any_mut().downcast_mut::<S>().unwrap();
        let (handle, stream) = scope.new_input();
        self.handle = Some(handle);
        Box::new(stream)
    }

    fn step(&mut self) -> bool {
        let curr_ts = Utc::now().timestamp_nanos();
        if let Some(last_ts) = self.last_request_timestamp {
            if let Some(req_interval) = self.request_interval {
                if curr_ts - last_ts < req_interval { return true }
            }
        }
        let handle = self.handle.as_mut().unwrap();
        let (data, step_time) = (self.emit_logic)();
        match data {
            Some(data) => {
                let curr_ts = Utc::now().timestamp_nanos();
                let timestamped_data = TimestampData {
                    data,
                    start_timestamp: curr_ts,
                    last_timestamp: curr_ts,
                    total_exec_net_latency: 0
                };                
                handle.send(timestamped_data);
                if let Some(step_time) = step_time {
                    let curr_time = handle.time();
                    if curr_time.less_than(&step_time) {
                        handle.advance_to(step_time);
                    }
                    else if step_time.less_than(curr_time) {
                        panic!("timestamp to advance to is less than current timestamp");
                    }
                }
                self.last_request_timestamp = Some(curr_ts);
                true
            },
            None => {
                self.handle = None;
                false
            }
        }
    }
}

impl<T, D: ExchangeData, L, A> ExchangeGenericInputFeeder for ClosureInputSource<T, D, L, PipelineScope<A, T>>
where
    T: Timestamp+ Refines<()> + TotalOrder,
    L: FnMut() -> (Option<D>, Option<T>) + 'static,
    A: RelayConnectAllocate + 'static,
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn crate::node::GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, scope: &mut dyn crate::node::GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let (handle, stream) = scope.new_input();
        self.handle = Some(handle);
        let stream: Box<dyn GenericStream> = Box::new(stream);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn crate::node::GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}