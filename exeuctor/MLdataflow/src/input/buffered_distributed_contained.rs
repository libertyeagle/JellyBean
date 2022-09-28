use std::any::Any;
use std::collections::{VecDeque, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::Utc;
use timely::communication::RelayConnectAllocate;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::{Data, ExchangeData};
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::dataflow::operators::Input;
use timely::dataflow::InputHandle;

use crate::TimestampData;
use crate::node::GenericStream;
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::{GenericScope, ExchangeGenericInputFeeder};
use super::GenericInputFeeder;


/// A buffered contained input source, that distributes the inputs (VecDeque<D>) across multiple workers
/// data_stream (VecDeque<D>) should contain some metadata (e.g., file paths)
/// users can provide a `emit_logic` that takes the metadata of type D and emits data of type D2
/// D2 will be the output data type
/// and the data will be buffered, with a given buffer_size
pub struct BufferedWorkerDistributedContainedInputSource<T: Timestamp, D: Data, D2: Data, L, S: Scope>
where 
    L: FnMut(D) -> (D2, T) + 'static,
    S: ScopeParent<Timestamp = T> + 'static
{
    data_stream: VecDeque<D>,
    buffer: VecDeque<(D2, T)>,
    buffer_size: usize,
    emit_logic: L,
    worker_index: usize,
    worker_peers: usize, 
    request_interval: Option<i64>,
    last_request_timestamp: Option<i64>,
    handle: Option<InputHandle<T, TimestampData<D2>>>,
    phantom: PhantomData<T>,
    phantom_scope: PhantomData<S>
}


impl<T: Timestamp + TotalOrder, D: Data, D2: Data, L, S> BufferedWorkerDistributedContainedInputSource<T, D, D2, L, S>
where
    L: FnMut(D) -> (D2, T) + 'static,
    S: Scope + ScopeParent<Timestamp = T> + 'static
{
    pub fn new(data_stream: VecDeque<D>, emit_logic: L, worker_index: usize, worker_peers: usize, buffer_size: usize) -> Self {
        BufferedWorkerDistributedContainedInputSource {
            data_stream,
            buffer: VecDeque::with_capacity(buffer_size),
            buffer_size: buffer_size,
            emit_logic,
            worker_index,
            worker_peers,
            request_interval: None,
            last_request_timestamp: None,
            handle: None,
            phantom: PhantomData,
            phantom_scope: PhantomData
        }
    }

    fn load_buffer(&mut self) {
        assert!(self.buffer.is_empty(), "buffer is not empty!");
        let drain_size = std::cmp::min(self.buffer_size * self.worker_peers, self.data_stream.len());
        for raw_input in self.data_stream.drain(0..drain_size).skip(self.worker_index).step_by(self.worker_peers) {
            let data_to_emit = (self.emit_logic)(raw_input);
            self.buffer.push_back(data_to_emit);
        }
    }
}

impl<T: Timestamp + TotalOrder, D: Data, D2: Data, L, S> GenericInputFeeder for BufferedWorkerDistributedContainedInputSource<T, D, D2, L, S>
where
    L: FnMut(D) -> (D2, T) + 'static,
    S: Scope + ScopeParent<Timestamp = T> + 'static
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
        if self.data_stream.is_empty() && self.buffer.is_empty() {
            self.handle = None;
            return false
        }
        else if self.buffer.is_empty() {
            self.load_buffer()
        }
        let curr_ts = Utc::now().timestamp_nanos();
        if let Some(last_ts) = self.last_request_timestamp {
            if let Some(req_interval) = self.request_interval {
                if curr_ts - last_ts < req_interval { return true }
            }
        }
        let handle = self.handle.as_mut().unwrap();
        let (data, step_timestamp) = self.buffer.pop_front().unwrap();
        let curr_time = handle.time().to_owned();
        let timestamped_data = TimestampData {
            data,
            start_timestamp: curr_ts,
            last_timestamp: curr_ts,
            total_exec_net_latency: 0
        };
        handle.send(timestamped_data);                    
        if curr_time.less_than(&step_timestamp) {
            handle.advance_to(step_timestamp);
        }
        else if step_timestamp.less_than(&curr_time) {
            panic!("timestamp to advance to is less than current timestamp");
        }
        self.last_request_timestamp = Some(curr_ts);
        true
    }
}

impl<T, D: Data, D2: ExchangeData, L, A> ExchangeGenericInputFeeder for BufferedWorkerDistributedContainedInputSource<T, D, D2, L, PipelineScope<A, T>>
where
    T: Timestamp+ Refines<()> + TotalOrder,
    L: FnMut(D) -> (D2, T) + 'static,
    A: RelayConnectAllocate + 'static,
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn crate::node::GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D2>>(input_idx);
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
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D2>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}