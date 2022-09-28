use std::any::Any;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use chrono::Utc;


use timely::communication::RelayConnectAllocate;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope};

use crate::TimestampData;
use crate::metrics::JCTLogger;
use crate::metrics::LatencyLogger;
use crate::metrics::RcWrapper;
use crate::metrics::ThroughputLogger;
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;
use crate::operators_timely::Map;

use super::{LocalOpBuilder, ExchangeOpBuilder};
use super::GenericStream;
use super::GenericPipelineScope;

const METRIC_KEEP_LAST_N: Option<usize> = None;
const THROUGHPUT_WINDOW_SIZE: usize = 10;
const WARMUP_ITERS: usize = 20;

const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_BUFFER_SIZE: usize = 512;

/// Map
/// Consumes each element of the stream and yields a new element.
pub struct MapNode<D1, D2, L, S> 
where
    L: FnMut(D1) -> D2 + 'static,
    S: Scope + 'static 
{
    prev_index: usize,
    logic: Option<L>,
    phantom: PhantomData<(D1, D2)>,
    phantom_scope: PhantomData<S>,
    // the minimal start timestamp of received requests
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    // the timestamp that first request arrived
    start_timestamp: Rc<RefCell<Option<i64>>>,
    // the timestamp that the last received request finished processing
    end_timestamp: Rc<RefCell<Option<i64>>>,
    // number of warmup requests received
    warmup_count: Rc<RefCell<usize>>,
    // the timestamp that the first request finished processing
    warmup_start_timestamp: Rc<RefCell<Option<i64>>>,
    // the throughput in the warmup period (will be computed as long as there are >=2 requests)
    warmup_throughput: Rc<RefCell<Option<f64>>>,
    // #requests processed in current window
    window_count: Rc<RefCell<usize>>,
    // the timestamp that current window starts
    window_start_timestamp: Rc<RefCell<i64>>,
    // throughput in #reqs/sec
    throughput: Rc<RefCell<VecDeque<f64>>>,    
    // operator execution latency in milliseconds
    execution_latencies: Rc<RefCell<VecDeque<i64>>>,
    // operator execution latency + delay since last operator emits the output
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    // latency that this operation 
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    // Timestamp when wramup is finished
    warmed_timestamp: Rc<RefCell<Option<i64>>>,
    // Total number of requests processed after warmup
    total_warmed_count: Rc<RefCell<i64>>,
    // Overall throughput (no window)
    overall_throughput: Rc<RefCell<Option<f64>>>
}

impl<D1, D2, L, S> MapNode<D1, D2, L, S>
where
    L: FnMut(D1) -> D2 + 'static,
    S: Scope + 'static 
{
    pub fn new(prev_index: usize, logic: L) -> Self {
        MapNode {
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

impl<D1: Data, D2: Data, L, S> LocalOpBuilder for MapNode<D1, D2, L, S>
where
    L: FnMut(D1) -> D2 + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.prev_index]
    }

    fn build(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {
        let reset_timestamp = if let Some(config) = &config {
            config.get("reset_timestamp").and_then(|val| val.downcast_ref::<bool>()).map(|val| *val)
        }
        else { None };
        let reset_timestamp = reset_timestamp.unwrap_or(false);
        let sim_network_latency = if let Some(config) = &config {
            config.get("simulate_network_latency").and_then(|val| val.downcast_ref::<i64>()).map(|val| *val)
        }
        else { None };

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
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

        // TODO: drop request that latency already exceeds SLO
        let stream_out = stream_in.map(move |x, mut net_lat| {
            if let Some(sim_net_lat) = sim_network_latency {
                net_lat = sim_net_lat;
            }
            if net_lat < 0 { net_lat = 0 }
            
            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                std::cmp::min(data_min_start_ts, x.start_timestamp)
            }   
            else {
                x.start_timestamp
            };
            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);

            let op_start_ts = Utc::now().timestamp_nanos();
            let mapped_data = (logic)(x.data);
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
            if reset_timestamp {
                TimestampData {
                    data: mapped_data,
                    start_timestamp: op_finish_ts,
                    last_timestamp: op_finish_ts,
                    total_exec_net_latency: 0
                }                
            }
            else {
                TimestampData {
                    data: mapped_data,
                    start_timestamp: x.start_timestamp,
                    last_timestamp: op_finish_ts,
                    total_exec_net_latency: exec_lat + net_lat + x.total_exec_net_latency
                }
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

impl<D1: Data, D2: ExchangeData, L, A, T> ExchangeOpBuilder for MapNode<D1, D2, L, PipelineScope<A, T>>
where
    L: FnMut(D1) -> D2 + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D2>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D2>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}

/// Flat map
/// Consumes each element of the stream and yields some number of new elements.
pub struct FlatMapNode<D, I, L, S>
where
    I: IntoIterator,
    L: FnMut(D) -> I + 'static,
    S: Scope + 'static
{
    prev_index: usize,
    logic: Option<L>,
    phantom: PhantomData<(D, I::Item)>,
    phantom_scope: PhantomData<S>,
    // the minimal start timestamp of received requests
    data_start_timestamp: Rc<RefCell<Option<i64>>>,    
    // the timestamp that first request arrived
    start_timestamp: Rc<RefCell<Option<i64>>>,
    // the timestamp that the last received request finished processing
    end_timestamp: Rc<RefCell<Option<i64>>>,
    // number of warmup requests received
    warmup_count: Rc<RefCell<usize>>,
    // the timestamp that the first request finished processing
    warmup_start_timestamp: Rc<RefCell<Option<i64>>>,
    // the throughput in the warmup period (will be computed as long as there are >=2 requests)
    warmup_throughput: Rc<RefCell<Option<f64>>>,
    // #requests processed in current window
    window_count: Rc<RefCell<usize>>,
    // the timestamp that current window starts
    window_start_timestamp: Rc<RefCell<i64>>,
    // throughput in #reqs/sec
    throughput: Rc<RefCell<VecDeque<f64>>>,    
    // operator execution latency in milliseconds
    execution_latencies: Rc<RefCell<VecDeque<i64>>>,
    // operator execution latency + delay since last operator emits the output
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    // latency that this operation 
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    // Timestamp when wramup is finished
    warmed_timestamp: Rc<RefCell<Option<i64>>>,
    // Total number of requests processed after warmup
    total_warmed_count: Rc<RefCell<i64>>,
    // Overall throughput (no window)
    overall_throughput: Rc<RefCell<Option<f64>>>    
}

impl<D: Data, I: IntoIterator + 'static, L, S> FlatMapNode<D, I, L, S>
where
    L: FnMut(D) -> I + 'static,
    S: Scope + 'static,
    I::Item: Data
{
    pub fn new(prev_index: usize, logic: L) -> Self {
        FlatMapNode {
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

impl<D: Data, I: IntoIterator + 'static, L, S> LocalOpBuilder for FlatMapNode<D, I, L, S>
where
    L: FnMut(D) -> I + 'static,
    S: Scope + 'static,
    I::Item: Data
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.prev_index]
    }

    fn build(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {
        let reset_timestamp = if let Some(config) = &config {
            config.get("reset_timestamp").and_then(|val| val.downcast_ref::<bool>()).map(|val| *val)
        }
        else { None };
        let reset_timestamp = reset_timestamp.unwrap_or(false);
        let sim_network_latency = if let Some(config) = &config {
            config.get("simulate_network_latency").and_then(|val| val.downcast_ref::<i64>()).map(|val| *val)
        }
        else { None };        

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

        let stream_out = stream_in.flat_map(move |x, mut net_lat| {      
            if let Some(sim_net_lat) = sim_network_latency {
                net_lat = sim_net_lat;
            }
            if net_lat < 0 { net_lat = 0 }

            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                std::cmp::min(data_min_start_ts, x.start_timestamp)
            }   
            else {
                x.start_timestamp
            };
            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);

            let x_start_timestamp = x.start_timestamp;
            let x_total_exec_net_lat = x.total_exec_net_latency;
            let op_start_ts = Utc::now().timestamp_nanos();
            let mapped_iter = (logic)(x.data);
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
            let mapped_iter_timestamped = mapped_iter.into_iter().map(move |item| {
                if reset_timestamp {
                    TimestampData {
                        data: item,
                        start_timestamp: x_start_timestamp,
                        last_timestamp: op_finish_ts,
                        total_exec_net_latency: exec_lat + net_lat + x_total_exec_net_lat,
                    }
                }
                else { 
                    TimestampData {
                        data: item,
                        start_timestamp: x_start_timestamp,
                        last_timestamp: op_finish_ts,
                        total_exec_net_latency: exec_lat + net_lat + x_total_exec_net_lat,
                    }
                }
            });
            mapped_iter_timestamped
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

impl<D: Data, I: IntoIterator + 'static, L, A, T> ExchangeOpBuilder for FlatMapNode<D, I, L, PipelineScope<A, T>>
where
    L: FnMut(D) -> I + 'static,
    I::Item: ExchangeData,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<I::Item>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<I::Item>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}


/// Batched map
/// Consumes each element of the stream and yields a new element.
pub struct BatchedMapNode<D1, D2, I2: IntoIterator<Item=D2>, L, S> 
where
    L: FnMut(Vec<D1>) -> I2 + 'static,
    S: Scope + 'static 
{
    prev_index: usize,
    logic: Option<L>,
    phantom: PhantomData<(D1, D2)>,
    phantom_scope: PhantomData<S>,
    // the minimal start timestamp of received requests
    data_start_timestamp: Rc<RefCell<Option<i64>>>,    
    // the timestamp that first request arrived
    start_timestamp: Rc<RefCell<Option<i64>>>,
    // the timestamp that the last received request finished processing
    end_timestamp: Rc<RefCell<Option<i64>>>,
    // number of warmup requests received
    warmup_count: Rc<RefCell<usize>>,
    // the timestamp that the first request finished processing
    warmup_start_timestamp: Rc<RefCell<Option<i64>>>,
    // the throughput in the warmup period (will be computed as long as there are >=2 requests)
    warmup_throughput: Rc<RefCell<Option<f64>>>,
    // #requests processed in current window
    window_count: Rc<RefCell<usize>>,
    // the timestamp that current window starts
    window_start_timestamp: Rc<RefCell<i64>>,
    // throughput in #reqs/sec
    throughput: Rc<RefCell<VecDeque<f64>>>,    
    // operator execution latency in milliseconds
    execution_latencies: Rc<RefCell<VecDeque<i64>>>,
    // operator execution latency + delay since last operator emits the output
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    // latency that this operation 
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    // Timestamp when wramup is finished
    warmed_timestamp: Rc<RefCell<Option<i64>>>,
    // Total number of requests processed after warmup
    total_warmed_count: Rc<RefCell<i64>>,
    // Overall throughput (no window)
    overall_throughput: Rc<RefCell<Option<f64>>>
}


impl<D1, D2, I2: IntoIterator<Item=D2>, L, S> BatchedMapNode<D1, D2, I2, L, S>
where
    L: FnMut(Vec<D1>) -> I2 + 'static,
    S: Scope + 'static 
{
    pub fn new(prev_index: usize, logic: L) -> Self {
        BatchedMapNode {
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

impl<D1: Data, D2: Data, I2: IntoIterator<Item=D2>, L, S> LocalOpBuilder for BatchedMapNode<D1, D2, I2, L, S>
where
    L: FnMut(Vec<D1>) -> I2 + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.prev_index]
    }

    fn build(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {        
        let reset_timestamp = if let Some(config) = &config {
            config.get("reset_timestamp").and_then(|val| val.downcast_ref::<bool>()).map(|val| *val)
        }
        else { None };
        let reset_timestamp = reset_timestamp.unwrap_or(false);
        let sim_network_latency = if let Some(config) = &config {
            config.get("simulate_network_latency").and_then(|val| val.downcast_ref::<i64>()).map(|val| *val)
        }
        else { None };

        let batch_size = if let Some(config) = &config {
            config.get("batch_size").and_then(|val| val.downcast_ref::<usize>()).map(|val| *val)
        }
        else { None };
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
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

        let stream_out = stream_in.batched_map(batch_size,move |data| {
            let all_latency = data.iter().map(|(_x, lat)| { 
                if let Some(sim_net_lat) = sim_network_latency { sim_net_lat } else { std::cmp::max(0, *lat) }
            }).collect::<Vec<_>>();
            let all_start_ts = data.iter().map(|(x, _lat)| x.start_timestamp).collect::<Vec<_>>();
            let all_total_lat = data.iter().map(|(x, _lat)| x.total_exec_net_latency).collect::<Vec<_>>();

            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                std::cmp::min(data_min_start_ts, *all_start_ts.iter().min().unwrap())
            }   
            else {
                *all_start_ts.iter().min().unwrap()
            };
            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);

            let input_vec = data.into_iter().map(|(x, _lat)| x.data).collect();
            let op_start_ts = Utc::now().timestamp_nanos();
            let mapped_data = (logic)(input_vec).into_iter();
            let op_finish_ts = Utc::now().timestamp_nanos();
            let mut processed_count = 0;

            let output_data = mapped_data.zip(all_latency).zip(all_start_ts).zip(all_total_lat).map(|(((x_out, net_lat), start_ts), total_exec_net_lat)| {
                // batch execution latency
                let exec_lat = op_finish_ts - op_start_ts;
                execution_latency_metrics.borrow_mut().push_front(exec_lat);
                edge_latency_metrics.borrow_mut().push_front(exec_lat + net_lat);
                path_latency_metrics.borrow_mut().push_front(exec_lat + net_lat + total_exec_net_lat);

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
                processed_count += 1;
                if reset_timestamp {
                    TimestampData {
                        data: x_out,
                        start_timestamp: op_finish_ts,
                        last_timestamp: op_finish_ts,
                        total_exec_net_latency: 0
                    }
                }
                else {
                    TimestampData {
                        data: x_out,
                        start_timestamp: start_ts,
                        last_timestamp: op_finish_ts,
                        total_exec_net_latency: exec_lat + net_lat + total_exec_net_lat
                    }
                }
            }).collect::<Vec<_>>();
            assert_eq!(batch_size, processed_count, "output batch size is not equal to the input batch size");
            output_data.into_iter()
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

impl<D1: Data, D2: ExchangeData, I2: IntoIterator<Item=D2>, L, A, T> ExchangeOpBuilder for BatchedMapNode<D1, D2, I2, L, PipelineScope<A, T>>
where
    L: FnMut(Vec<D1>) -> I2 + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D2>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D2>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}

/// Buffered map
pub struct BufferedMapNode<D1, D2, L, S> 
where
    L: FnMut(D1) -> D2 + 'static,
    S: Scope + 'static 
{
    prev_index: usize,
    logic: Option<L>,
    phantom: PhantomData<(D1, D2)>,
    phantom_scope: PhantomData<S>,
    // the minimal start timestamp of received requests
    data_start_timestamp: Rc<RefCell<Option<i64>>>,    
    // the timestamp that first request arrived
    start_timestamp: Rc<RefCell<Option<i64>>>,
    // the timestamp that the last received request finished processing
    end_timestamp: Rc<RefCell<Option<i64>>>,
    // number of warmup requests received
    warmup_count: Rc<RefCell<usize>>,
    // the timestamp that the first request finished processing
    warmup_start_timestamp: Rc<RefCell<Option<i64>>>,
    // the throughput in the warmup period (will be computed as long as there are >=2 requests)
    warmup_throughput: Rc<RefCell<Option<f64>>>,
    // #requests processed in current window
    window_count: Rc<RefCell<usize>>,
    // the timestamp that current window starts
    window_start_timestamp: Rc<RefCell<i64>>,
    // throughput in #reqs/sec
    throughput: Rc<RefCell<VecDeque<f64>>>,    
    // operator execution latency in milliseconds
    execution_latencies: Rc<RefCell<VecDeque<i64>>>,
    // operator execution latency + delay since last operator emits the output
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    // latency that this operation 
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    // Timestamp when wramup is finished
    warmed_timestamp: Rc<RefCell<Option<i64>>>,
    // Total number of requests processed after warmup
    total_warmed_count: Rc<RefCell<i64>>,
    // Overall throughput (no window)
    overall_throughput: Rc<RefCell<Option<f64>>>
}

impl<D1, D2, L, S> BufferedMapNode<D1, D2, L, S>
where
    L: FnMut(D1) -> D2 + 'static,
    S: Scope + 'static 
{
    pub fn new(prev_index: usize, logic: L) -> Self {
        BufferedMapNode {
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

impl<D1: Data, D2: Data, L, S> LocalOpBuilder for BufferedMapNode<D1, D2, L, S>
where
    L: FnMut(D1) -> D2 + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.prev_index]
    }

    fn build(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {
        let reset_timestamp = if let Some(config) = &config {            
            config.get("reset_timestamp").and_then(|val| val.downcast_ref::<bool>()).map(|val| *val)
        }
        else { None };
        let reset_timestamp = reset_timestamp.unwrap_or(false);
        let sim_network_latency = if let Some(config) = &config {
            config.get("simulate_network_latency").and_then(|val| val.downcast_ref::<i64>()).map(|val| *val)
        }
        else { None };

        let buffer_size = if let Some(config) = config {
            config.get("buffer_size").and_then(|val| val.downcast_ref::<usize>()).map(|val| *val)
        }
        else { None };
        let buffer_size = buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
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

        let stream_out = stream_in.buffered_map(buffer_size, move |x, mut net_lat| {
            if let Some(sim_net_lat) = sim_network_latency {
                net_lat = sim_net_lat;
            }
            if net_lat < 0 { net_lat = 0 }

            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                std::cmp::min(data_min_start_ts, x.start_timestamp)
            }   
            else {
                x.start_timestamp
            };
            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);

            let op_start_ts = Utc::now().timestamp_nanos();
            let mapped_data = (logic)(x.data);
            let op_finish_ts = Utc::now().timestamp_nanos();

            let exec_lat = op_finish_ts - op_start_ts;
            execution_latency_metrics.borrow_mut().push_front(exec_lat);
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
            if reset_timestamp {
                TimestampData {
                    data: mapped_data,
                    start_timestamp: op_finish_ts,
                    last_timestamp: op_finish_ts,
                    total_exec_net_latency: 0,
                }
            }
            else {
                TimestampData {
                    data: mapped_data,
                    start_timestamp: x.start_timestamp,
                    last_timestamp: op_finish_ts,
                    total_exec_net_latency: exec_lat + net_lat + x.total_exec_net_latency
                }
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

impl<D1: Data, D2: ExchangeData, L, A, T> ExchangeOpBuilder for BufferedMapNode<D1, D2, L, PipelineScope<A, T>>
where
    L: FnMut(D1) -> D2 + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D2>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D2>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}