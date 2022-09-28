use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use chrono::Utc;
use timely::communication::RelayConnectAllocate;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::dataflow::channels::pact::Pipeline;
use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::Operator;

use crate::TimestampData;
use crate::metrics::{LatencyLogger, RcWrapper, JCTLogger};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::{LocalOpBuilder, ExchangeOpBuilder};
use super::GenericStream;
use super::GenericPipelineScope;

const METRIC_KEEP_LAST_N: Option<usize> = None;

// EXTEREMLY IMPORTANT!
// NOTE: for aggregation / incremental aggregation operators, we have no concept of "latency"
// for this operator and its consecutive operators, we only have the concept of JCT (job completion time) 

// TODO: implement JCT logger for these aggregate operators

/// Aggregate data with the same key
/// Input a stream of data type D1
/// Output stream of data type D2
/// key_map_logic is the hash function that maps the data
/// to a key of type K, and provide the number of items associated with the that key
/// aggregate_logic gets a vector of data that has the same key,
/// and returns a record with type D2
pub struct AggregateNode<D1, D2, K, L, H, S>
where
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> (K, usize) + 'static,
    S: Scope + 'static,
{
    prev_index: usize,
    key_map_logic: Option<H>,
    aggregate_logic: Option<L>,
    phantom: PhantomData<(D1, D2, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>
}

impl<D1: Data, D2: Data, K, L, H, S> AggregateNode<D1, D2, K, L, H, S>
where
    K: Clone + Hash + Eq + 'static,
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> (K, usize) + 'static,
    S: Scope + 'static
{
    pub fn new(prev_index: usize, key_map_logic: H, agg_logic: L) -> Self {
        AggregateNode {
            prev_index,
            key_map_logic: Some(key_map_logic),
            aggregate_logic: Some(agg_logic),
            phantom: PhantomData,
            phantom_scope: PhantomData,
            edge_latencies: Rc::new(RefCell::new(VecDeque::new())),
            path_latencies: Rc::new(RefCell::new(VecDeque::new())),
            data_start_timestamp: Rc::new(RefCell::new(None)),
            start_timestamp: Rc::new(RefCell::new(None)),
            end_timestamp: Rc::new(RefCell::new(None))
        }
    }
}

impl<D1: Data, D2: Data, K, L, H, S> LocalOpBuilder for AggregateNode<D1, D2, K, L, H, S>
where
    K: Clone + Eq + Hash + 'static,
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> (K, usize) + 'static,
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

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();

        let key_map_logic = self.key_map_logic.take().unwrap();
        let mut aggregate_logic = self.aggregate_logic.take().unwrap();
        let stream_out = stream_in.unary(Pipeline, "Aggregate", |_capability, _info| {
            let mut buffer = HashMap::new();
            let mut vector = Vec::new();

            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for data_point in vector.drain(..) {
                        let (key, total_num) = (key_map_logic)(&data_point.data);
                        buffer.entry(key.clone()).or_insert(Vec::new()).push(data_point);
                        if buffer.get(&key).unwrap().len() == total_num {
                            let all_data_points_with_k = buffer.remove(&key).unwrap();
                            let min_start_ts = all_data_points_with_k.iter().map(|x| x.start_timestamp).min().unwrap();
                            let min_last_ts = all_data_points_with_k.iter().map(|x| x.last_timestamp).min().unwrap();
                            let all_data_points_raw = all_data_points_with_k.into_iter().map(|x| x.data).collect();
                            let aggregation_result = (aggregate_logic)(all_data_points_raw);
                            let curr_ts = Utc::now().timestamp_nanos();

                            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                                std::cmp::min(data_min_start_ts, min_start_ts)
                            }   
                            else {
                                min_start_ts
                            };
                            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);
                            if start_timestamp.borrow().is_none() {
                                *start_timestamp.borrow_mut() = Some(curr_ts);
                            }
                            *end_timestamp.borrow_mut() = Some(curr_ts);
                            edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                            path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                
                            if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                edge_latency_metrics.borrow_mut().truncate(keep_n);
                                path_latency_metrics.borrow_mut().truncate(keep_n);
                            }
                            session.give(if reset_timestamp { 
                                TimestampData {
                                    data: aggregation_result,
                                    start_timestamp: curr_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: 0
                                }
                            } else {
                                TimestampData {
                                    data: aggregation_result,
                                    start_timestamp: min_start_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: curr_ts - min_start_ts
                                }
                            });
                        }
                    }
                });
            }
        });
        Box::new(stream_out)
    }

    fn get_throughput_logger(&self) -> Option<crate::metrics::ThroughputLogger> {
        None
    }

    fn get_flow_compute_latency_logger(&self) -> Option<LatencyLogger> {
        None
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


impl<D1: Data, D2: ExchangeData, K, L, H, A, T> ExchangeOpBuilder for AggregateNode<D1, D2, K, L, H, PipelineScope<A, T>>
where
    K: Clone + Eq + Hash + 'static,
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> (K, usize) + 'static,
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

/// Aggregate data with the same key and bears the same timestamp
/// Input a stream of data type D1
/// Output stream of data type D2
/// key_map_logic is the hash function that maps the data
/// to a key of type K
/// aggregate_logic gets a vector of data that has the same key and the same timestamp,
/// and returns a record with type D2
pub struct TimestampAggregateNode<D1, D2, K, L, H, S>
where
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> K + 'static,
    S: Scope + 'static,
{
    prev_index: usize,
    key_map_logic: Option<H>,
    aggregate_logic: Option<L>,
    phantom: PhantomData<(D1, D2, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>    
}

impl<D1: Data, D2: Data, K, L, H, S> TimestampAggregateNode<D1, D2, K, L, H, S>
where
    K: Eq + Hash + 'static,
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> K + 'static,
    S: Scope + 'static
{
    pub fn new(prev_index: usize, key_map_logic: H, agg_logic: L) -> Self {
        TimestampAggregateNode {
            prev_index,
            key_map_logic: Some(key_map_logic),
            aggregate_logic: Some(agg_logic),
            phantom: PhantomData,
            phantom_scope: PhantomData,
            edge_latencies: Rc::new(RefCell::new(VecDeque::new())),
            path_latencies: Rc::new(RefCell::new(VecDeque::new())),
            data_start_timestamp: Rc::new(RefCell::new(None)),
            start_timestamp: Rc::new(RefCell::new(None)),
            end_timestamp: Rc::new(RefCell::new(None))
        }
    }
}

impl<D1: Data, D2: Data, K, L, H, S> LocalOpBuilder for TimestampAggregateNode<D1, D2, K, L, H, S>
where
    K: Eq + Hash + 'static,
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> K + 'static,
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

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();

        let key_map_logic = self.key_map_logic.take().unwrap();
        let mut aggregate_logic = self.aggregate_logic.take().unwrap();

        let mut aggregate_buffers = HashMap::new();
        let mut vector = Vec::new();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();
    
        let stream_out = stream_in.unary_notify(Pipeline, "Aggregate", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                let agg_time = aggregate_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                for data_point in vector.drain(..) {
                    let key = (key_map_logic)(&data_point.data);
                    agg_time.entry(key).or_insert(Vec::new()).push(data_point);
                }
                notificator.notify_at(time.retain());
            });

            notificator.for_each(|time, _, _| {
                if let Some(agg_time) = aggregate_buffers.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (_key, data_points) in agg_time {
                        let min_start_ts = data_points.iter().map(|x| x.start_timestamp).min().unwrap();
                        let min_last_ts = data_points.iter().map(|x| x.last_timestamp).min().unwrap();
                        let data_points_raw = data_points.into_iter().map(|x| x.data).collect();
                        let aggregated = (aggregate_logic)(data_points_raw);
                        let curr_ts = Utc::now().timestamp_nanos();

                        let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                            std::cmp::min(data_min_start_ts, min_start_ts)
                        }   
                        else {
                            min_start_ts
                        };
                        *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);
                        if start_timestamp.borrow().is_none() {
                            *start_timestamp.borrow_mut() = Some(curr_ts);
                        }
                        *end_timestamp.borrow_mut() = Some(curr_ts);                
                        edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                        path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                        
                        if let Some(keep_n) = METRIC_KEEP_LAST_N {
                            edge_latency_metrics.borrow_mut().truncate(keep_n);
                            path_latency_metrics.borrow_mut().truncate(keep_n);
                        }
                        session.give(if reset_timestamp {
                            TimestampData {
                                data: aggregated,
                                start_timestamp: curr_ts,
                                last_timestamp: curr_ts,
                                total_exec_net_latency: 0
                            }
                        } else {
                            TimestampData {
                                data: aggregated,
                                start_timestamp: min_start_ts,
                                last_timestamp: curr_ts,
                                total_exec_net_latency: curr_ts - min_start_ts
                            }
                        });
                    }
                }
            });
        });
        Box::new(stream_out)
    }

    fn get_throughput_logger(&self) -> Option<crate::metrics::ThroughputLogger> {
        None
    }

    fn get_flow_compute_latency_logger(&self) -> Option<LatencyLogger> {
        None
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

impl<D1: Data, D2: ExchangeData, K, L, H, A, T> ExchangeOpBuilder for TimestampAggregateNode<D1, D2, K, L, H, PipelineScope<A, T>>
where
    K: Eq + Hash + 'static,
    L: FnMut(Vec<D1>) -> D2 + 'static,
    H: Fn(&D1) -> K + 'static,
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


/// Incremental aggregate data with the same key
/// Input stream: data of type D
/// Intermediate value: data of type Di
/// Emit output stream of data type R
/// hash_logic hashes the data of type 
/// fold_logic takes in a reference to the key, the data
/// and mutate the intermediate aggregate result Di corresponds to that key
/// It also returns a bool flag to indicate whether all data with that key
/// have been aggregated
/// emit_logic takes the key, and the final intermediate result correspond to that key
/// And outputs data of type R
pub struct IncrementalAggregateNode<D, Di, R, K, H, F, E, S>
where
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) -> bool + 'static,
    E: Fn(K, Di) -> R + 'static,
    S: Scope + 'static
{
    prev_index: usize,
    hash_logic: Option<H>,
    fold_logic: Option<F>,
    emit_logic: Option<E>,
    phantom: PhantomData<(D, Di, R, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>    
}

impl<D: Data, Di: Default + 'static, R: Data, K, H, F, E, S> IncrementalAggregateNode<D, Di, R, K, H, F, E, S>
where
    K: Clone + Hash + Eq + 'static,
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) -> bool + 'static,
    E: Fn(K, Di) -> R + 'static,
    S: Scope + 'static,
{
    pub fn new(prev_index: usize, hash_logic: H, fold_logic: F, emit_logic: E) -> Self {
        IncrementalAggregateNode {
            prev_index,
            hash_logic: Some(hash_logic),
            fold_logic: Some(fold_logic),
            emit_logic: Some(emit_logic),
            phantom: PhantomData,
            phantom_scope: PhantomData,
            edge_latencies: Rc::new(RefCell::new(VecDeque::new())),
            path_latencies: Rc::new(RefCell::new(VecDeque::new())),
            data_start_timestamp: Rc::new(RefCell::new(None)),
            start_timestamp: Rc::new(RefCell::new(None)),
            end_timestamp: Rc::new(RefCell::new(None)),                   
        }
    }
}

impl<D: Data, Di: Default + 'static, R: Data, K, H, F, E, S> LocalOpBuilder for IncrementalAggregateNode<D, Di, R, K, H, F, E, S>
where
    K: Clone + Hash + Eq + 'static,
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) -> bool + 'static,
    E: Fn(K, Di) -> R + 'static,
    S: Scope + 'static,
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

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D>>>().unwrap();

        let hash_logic = self.hash_logic.take().unwrap();
        let fold_logic = self.fold_logic.take().unwrap();
        let emit_logic = self.emit_logic.take().unwrap();
        
        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();

        let stream_out = stream_in.unary(Pipeline, "IncrementalAggregate", |_capability, _info| {
            let mut aggregates = HashMap::new();
            let mut vector = Vec::new();
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut session = output.session(&time);
                    for data_point in vector.drain(..) {
                        let key = (hash_logic)(&data_point.data);
                        let (agg_intermediate, min_start_ts, min_last_ts) = aggregates.entry(key.clone()).or_insert((Default::default(), data_point.start_timestamp, data_point.last_timestamp));
                        *min_start_ts = std::cmp::min(data_point.start_timestamp, *min_start_ts);
                        *min_last_ts = std::cmp::min(data_point.last_timestamp, *min_last_ts);
                        if (fold_logic)(&key, data_point.data, agg_intermediate) {
                            let (completed_intermediate, min_start_ts, min_last_ts) = aggregates.remove(&key).unwrap();
                            let result = (emit_logic)(key, completed_intermediate);
                            let curr_ts = Utc::now().timestamp_nanos();

                            let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                                std::cmp::min(data_min_start_ts, min_start_ts)
                            }   
                            else {
                                min_start_ts
                            };
                            *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);
                            if start_timestamp.borrow().is_none() {
                                *start_timestamp.borrow_mut() = Some(curr_ts);
                            }
                            *end_timestamp.borrow_mut() = Some(curr_ts);
                            edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                            path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                
                            if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                edge_latency_metrics.borrow_mut().truncate(keep_n);
                                path_latency_metrics.borrow_mut().truncate(keep_n);
                            }                            
                            session.give(if reset_timestamp {
                                TimestampData {
                                    data: result,
                                    start_timestamp: curr_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: 0
                                }
                            } else {
                                TimestampData {
                                    data: result,
                                    start_timestamp: min_start_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: curr_ts - min_start_ts
                                }
                            });
                        };

                    }
                });
            }
        });
        Box::new(stream_out)
    }

    fn get_throughput_logger(&self) -> Option<crate::metrics::ThroughputLogger> {
        None
    }

    fn get_flow_compute_latency_logger(&self) -> Option<LatencyLogger> {
        None
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

impl<D: Data, Di: Default + 'static, R: ExchangeData, K, H, F, E, A, T> ExchangeOpBuilder for IncrementalAggregateNode<D, Di, R, K, H, F, E, PipelineScope<A, T>>
where
    K: Clone + Hash + Eq + 'static,
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) -> bool + 'static,
    E: Fn(K, Di) -> R + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<R>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<R>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}

/// Incremental aggregate data with the same key and with the same timesatmp
/// Input stream: data of type D
/// Intermediate value: data of type Di
/// Emit output stream of data type R
/// hash_logic hashes the data of type 
/// fold_logic takes in a reference to the key, the data
/// and mutate the intermediate aggregate result Di corresponds to that key
/// emit_logic takes the key, and the final intermediate result correspond to that key
/// And outputs data of type R
pub struct TimestampIncrementalAggregateNode<D, Di, R, K, H, F, E, S>
where
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) + 'static,
    E: Fn(K, Di) -> R + 'static,
    S: Scope + 'static
{
    prev_index: usize,
    hash_logic: Option<H>,
    fold_logic: Option<F>,
    emit_logic: Option<E>,
    phantom: PhantomData<(D, Di, R, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>    
}

impl<D: Data, Di: Default + 'static, R: Data, K, H, F, E, S> TimestampIncrementalAggregateNode<D, Di, R, K, H, F, E, S>
where
    K: Clone + Hash + Eq + 'static,
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) + 'static,
    E: Fn(K, Di) -> R + 'static,
    S: Scope + 'static,
{
    pub fn new(prev_index: usize, hash_logic: H, fold_logic: F, emit_logic: E) -> Self {
        TimestampIncrementalAggregateNode {
            prev_index,
            hash_logic: Some(hash_logic),
            fold_logic: Some(fold_logic),
            emit_logic: Some(emit_logic),
            phantom: PhantomData,
            phantom_scope: PhantomData,
            edge_latencies: Rc::new(RefCell::new(VecDeque::new())),
            path_latencies: Rc::new(RefCell::new(VecDeque::new())),
            data_start_timestamp: Rc::new(RefCell::new(None)),
            start_timestamp: Rc::new(RefCell::new(None)),
            end_timestamp: Rc::new(RefCell::new(None))            
        }
    }
}

impl<D: Data, Di: Default + 'static, R: Data, K, H, F, E, S> LocalOpBuilder for TimestampIncrementalAggregateNode<D, Di, R, K, H, F, E, S>
where
    K: Clone + Hash + Eq + 'static,
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) + 'static,
    E: Fn(K, Di) -> R + 'static,
    S: Scope + 'static,
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

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D>>>().unwrap();

        let hash_logic = self.hash_logic.take().unwrap();
        let fold_logic = self.fold_logic.take().unwrap();
        let emit_logic = self.emit_logic.take().unwrap();

        let mut aggregates = HashMap::new();
        let mut vector = Vec::new();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();

        let stream_out = stream_in.unary_notify(Pipeline, "TimestampIncrementalAggregate", vec![], move |input, output, notificator| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                let agg_time = aggregates.entry(time.time().clone()).or_insert_with(HashMap::new);
                for data_point in vector.drain(..) {
                    let key = (hash_logic)(&data_point.data);
                    let (intermediate, min_start_ts,min_last_ts) = agg_time.entry(key.clone()).or_insert((Default::default(), data_point.start_timestamp, data_point.last_timestamp));
                    *min_start_ts = std::cmp::min(data_point.start_timestamp, *min_start_ts);
                    *min_last_ts = std::cmp::min(data_point.last_timestamp, *min_last_ts);
                    (fold_logic)(&key, data_point.data, intermediate);
                }
                notificator.notify_at(time.retain());
            });

            notificator.for_each(|time, _, _| {
                if let Some(agg_time) = aggregates.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (key, (intermediate, min_start_ts, min_last_ts)) in agg_time {
                        let result = (emit_logic)(key, intermediate);
                        let curr_ts =  Utc::now().timestamp_nanos();

                        let data_min_start_ts = if let Some(data_min_start_ts) = data_start_timestamp.borrow().as_ref().copied() {
                            std::cmp::min(data_min_start_ts, min_start_ts)
                        }   
                        else {
                            min_start_ts
                        };
                        *data_start_timestamp.borrow_mut() = Some(data_min_start_ts);
                        if start_timestamp.borrow().is_none() {
                            *start_timestamp.borrow_mut() = Some(curr_ts);
                        }
                        *end_timestamp.borrow_mut() = Some(curr_ts);
                        edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                        path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
            
                        if let Some(keep_n) = METRIC_KEEP_LAST_N {
                            edge_latency_metrics.borrow_mut().truncate(keep_n);
                            path_latency_metrics.borrow_mut().truncate(keep_n);
                        }                        
                        session.give(if reset_timestamp {
                            TimestampData {
                                data: result,
                                start_timestamp: curr_ts,
                                last_timestamp: curr_ts,
                                total_exec_net_latency: 0
                            }
                        } else {
                            TimestampData {
                                data: result,
                                start_timestamp: min_start_ts,
                                last_timestamp: curr_ts,
                                total_exec_net_latency: curr_ts - min_start_ts
                            }
                        });
                    }
                }
            });
        });
        Box::new(stream_out)
    }

    fn get_throughput_logger(&self) -> Option<crate::metrics::ThroughputLogger> {
        None
    }

    fn get_flow_compute_latency_logger(&self) -> Option<LatencyLogger> {
        None
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

impl<D: Data, Di: Default + 'static, R: ExchangeData, K, H, F, E, A, T> ExchangeOpBuilder for TimestampIncrementalAggregateNode<D, Di, R, K, H, F, E, PipelineScope<A, T>>
where
    K: Clone + Hash + Eq + 'static,
    H: Fn(&D) -> K + 'static,
    F: Fn(&K, D, &mut Di) + 'static,
    E: Fn(K, Di) -> R + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<R>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<R>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}
