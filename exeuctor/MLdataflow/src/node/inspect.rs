use std::any::Any;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::Utc;
use timely::communication::RelayConnectAllocate;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::Map;

use crate::TimestampData;
use crate::metrics::{LatencyLogger, JCTLogger, ThroughputLogger, RcWrapper};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;
use crate::operators_timely::inspect::Inspect;

use super::{LocalOpBuilder, ExchangeOpBuilder};
use super::GenericStream;
use super::GenericPipelineScope;


const METRIC_KEEP_LAST_N: Option<usize> = None;
const THROUGHPUT_WINDOW_SIZE: usize = 10;
const WARMUP_ITERS: usize = 20;

/// Inspect a data stream
pub struct InspectNode<D, L, S>
where
    L: FnMut(&D) + 'static,
    S: Scope + 'static,
{
    prev_index: usize,
    logic: Option<L>,
    phantom: PhantomData<D>,
    phantom_scope: PhantomData<S>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>,
    warmup_count: Rc<RefCell<usize>>,
    warmup_start_timestamp: Rc<RefCell<Option<i64>>>,
    warmup_throughput: Rc<RefCell<Option<f64>>>,
    window_count: Rc<RefCell<usize>>,
    window_start_timestamp: Rc<RefCell<i64>>,
    throughput: Rc<RefCell<VecDeque<f64>>>,    
    execution_latencies: Rc<RefCell<VecDeque<i64>>>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    // Timestamp when wramup is finished
    warmed_timestamp: Rc<RefCell<Option<i64>>>,
    // Total number of requests processed after warmup
    total_warmed_count: Rc<RefCell<i64>>,
    // Overall throughput (no window)
    overall_throughput: Rc<RefCell<Option<f64>>>      
}

impl<D: Data, L, S> InspectNode<D, L, S>
where
    L: FnMut(&D) + 'static,
    S: Scope + 'static
{
    pub fn new(prev_index: usize, logic: L) -> Self {
        InspectNode {
            prev_index,
            logic: Some(logic),
            phantom: PhantomData,
            phantom_scope: PhantomData,
            data_start_timestamp: Rc::new(RefCell::new(None)),
            start_timestamp: Rc::new(RefCell::new(None)),
            end_timestamp: Rc::new(RefCell::new(None)),
            warmup_count: Rc::new(RefCell::new(0)),
            warmup_start_timestamp: Rc::new(RefCell::new(None)),
            warmup_throughput: Rc::new(RefCell::new(None)),
            window_count: Rc::new(RefCell::new(0)),
            window_start_timestamp: Rc::new(RefCell::new(0)),
            throughput: Rc::new(RefCell::new(VecDeque::new())),
            execution_latencies: Rc::new(RefCell::new(VecDeque::new())),
            edge_latencies: Rc::new(RefCell::new(VecDeque::new())),
            path_latencies: Rc::new(RefCell::new(VecDeque::new())),
            warmed_timestamp: Rc::new(RefCell::new(None)),
            total_warmed_count: Rc::new(RefCell::new(0)),
            overall_throughput: Rc::new(RefCell::new(None))
        }
    }
}

impl<D: Data, L, S> LocalOpBuilder for InspectNode<D, L, S>
where
    L: FnMut(&D) + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> { vec![self.prev_index] }

    fn build(&mut self, streams: &[&Box<dyn GenericStream>], _config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {
        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D>>>().unwrap();
        let mut logic = self.logic.take().unwrap();

        let execution_latency_metrics = self.execution_latencies.clone();
        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let window_start_timestamp = self.window_start_timestamp.clone();
        let window_count = self.window_count.clone();
        let throughput_metrics = self.throughput.clone();
        let warmup_start_timestamp = self.warmup_start_timestamp.clone();
        let warmup_count = self.warmup_count.clone();
        let warmup_throughput = self.warmup_throughput.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();
        let warmed_timestamp = self.warmed_timestamp.clone();
        let total_warmed_count = self.total_warmed_count.clone();
        let overall_throughput = self.overall_throughput.clone();        
                
        let stream_intermediate = stream_in.inspect(move |x, net_lat| {
            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                std::cmp::min(data_min_start_ts, x.start_timestamp)
            }   
            else {
                x.start_timestamp
            };
            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);
                        
            let op_start_ts = Utc::now().timestamp_nanos();
            (logic)(&x.data);
            let op_finish_ts = Utc::now().timestamp_nanos();

            let exec_lat = op_finish_ts - op_start_ts;
            execution_latency_metrics.borrow_mut().push_front(exec_lat);
            // edge_latency_metrics.borrow_mut().push_front(op_finish_ts - x.last_timestamp);
            // path_latency_metrics.borrow_mut().push_front(op_finish_ts - x.start_timestamp);
            edge_latency_metrics.borrow_mut().push_front(exec_lat + net_lat);
            path_latency_metrics.borrow_mut().push_front(exec_lat + net_lat + x.total_exec_net_latency);

            if let Some(keep_n) = METRIC_KEEP_LAST_N {
                execution_latency_metrics.borrow_mut().truncate(keep_n);
                edge_latency_metrics.borrow_mut().truncate(keep_n);
                path_latency_metrics.borrow_mut().truncate(keep_n);
            }
            if warmup_start_timestamp.borrow().is_none() {
                *warmup_count.borrow_mut() = 0;
                *start_timestamp.borrow_mut() = Some(op_start_ts);
                *warmup_start_timestamp.borrow_mut() = Some(op_finish_ts);
            }
            else if *warmup_count.borrow() < WARMUP_ITERS {
                *warmup_count.borrow_mut() += 1;
                if *warmup_count.borrow() == WARMUP_ITERS {
                    *window_start_timestamp.borrow_mut() = op_finish_ts;
                    *warmed_timestamp.borrow_mut() = Some(op_finish_ts);
                    *total_warmed_count.borrow_mut() = 0;                    
                }
                *warmup_throughput.borrow_mut() = Some(
                    *warmup_count.borrow() as f64 / ((op_finish_ts - warmup_start_timestamp.borrow().unwrap()) as f64 / 1e9_f64)
                );
            }
            else {
                *window_count.borrow_mut() += 1;
                if *window_count.borrow() >= THROUGHPUT_WINDOW_SIZE {
                    let ts = Utc::now().timestamp_nanos();
                    let tp = *window_count.borrow() as f64 / ((ts - *window_start_timestamp.borrow()) as f64 / 1e9_f64);
                    throughput_metrics.borrow_mut().push_front(tp);
                    if let Some(keep_n) = METRIC_KEEP_LAST_N {
                        throughput_metrics.borrow_mut().truncate(keep_n);
                    }
                    *window_count.borrow_mut() = 0;
                    *window_start_timestamp.borrow_mut() = ts;
                }
                *total_warmed_count.borrow_mut() += 1;
                *overall_throughput.borrow_mut() = Some(*total_warmed_count.borrow() as f64 / ((op_finish_ts - *warmed_timestamp.borrow().as_ref().unwrap()) as f64 / 1e9_f64))                
            }
            *end_timestamp.borrow_mut() = Some(op_finish_ts);   
            exec_lat         
        });
        
        let stream_out = stream_intermediate.map(|(x, exec_net_lat)| {
            let curr_ts = Utc::now().timestamp_nanos();
            TimestampData {
                data: x.data,
                start_timestamp: x.start_timestamp,
                last_timestamp: curr_ts,
                total_exec_net_latency: x.total_exec_net_latency + exec_net_lat
            }            
        });
        Box::new(stream_out)
    }

    fn get_throughput_logger(&self) -> Option<ThroughputLogger> {
        let logger = ThroughputLogger {
            throughput: RcWrapper::new(self.throughput.clone()),
            warmup_throughput: RcWrapper::new(self.warmup_throughput.clone()),
            overall_throughput: RcWrapper::new(self.overall_throughput.clone())
        };
        Some(logger)
    }

    fn get_flow_compute_latency_logger(&self) -> Option<LatencyLogger> {
        let logger = LatencyLogger {
            latencies: RcWrapper::new(self.execution_latencies.clone())
        };
        Some(logger)
    }

    fn get_flow_edge_latency_logger(&self) -> Option<LatencyLogger> {
        let logger = LatencyLogger {
            latencies: RcWrapper::new(self.edge_latencies.clone())
        };
        Some(logger)
    }

    fn get_flow_path_latency_logger(&self) -> Option<LatencyLogger> {
        let logger = LatencyLogger {
            latencies: RcWrapper::new(self.path_latencies.clone())
        };
        Some(logger)
    }

    fn get_jct_logger(&self) -> Option<JCTLogger> {
        let logger = JCTLogger {
            data_start_timestamp: RcWrapper::new(self.data_start_timestamp.clone()),
            op_start_timestamp: RcWrapper::new(self.start_timestamp.clone()),
            op_end_timestamp: RcWrapper::new(self.end_timestamp.clone())
        };
        Some(logger)
    }
}


impl<D: ExchangeData, L, A, T> ExchangeOpBuilder for InspectNode<D, L, PipelineScope<A, T>>
where
    L: FnMut(&D) + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}
