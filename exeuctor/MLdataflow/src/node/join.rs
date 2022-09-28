use std::any::Any;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use chrono::Utc;
use timely::communication::{RelayConnectAllocate, MessageLatency};
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::dataflow::channels::pact::Pipeline;
use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope, ScopeParent};
use timely::dataflow::operators::Operator;

use crate::TimestampData;
use crate::metrics::{LatencyLogger, RcWrapper, JCTLogger};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

use super::{LocalOpBuilder, ExchangeOpBuilder};
use super::GenericStream;
use super::GenericPipelineScope;

const METRIC_KEEP_LAST_N: Option<usize> = None;

/// Inner join
/// key_map_left, key_map_right are hash functions
/// that maps the data on the left stream
/// and the data on the right stream into a key of type K
/// These two hash functions also provide the total number of items
/// on the left/right stream that correspond with this key
pub struct JoinNode<D1, D2, K, H1, H2, S>
where
    H1: Fn(&D1) -> (K, usize) + 'static,
    H2: Fn(&D2) -> (K, usize) + 'static,
    S: Scope + 'static
{
    left_index: usize,
    right_index: usize,
    key_map_left: Option<H1>,
    key_map_right: Option<H2>,
    phantom: PhantomData<(D1, D2, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>
}


impl<D1: Data, D2: Data, K, H1, H2, S> JoinNode<D1, D2, K, H1, H2, S>
where
    K: Clone + Eq + Hash + 'static,
    H1: Fn(&D1) -> (K, usize) + 'static,
    H2: Fn(&D2) -> (K, usize) + 'static,
    S: Scope + 'static
{
    pub(crate) fn new(left_index: usize, right_index: usize, left_hash_logic: H1, right_hash_logic: H2)-> Self {
        JoinNode {
            left_index,
            right_index,
            key_map_left: Some(left_hash_logic),
            key_map_right: Some(right_hash_logic),
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


impl<D1: Data, D2: Data, K, H1, H2, S> LocalOpBuilder for JoinNode<D1, D2, K, H1, H2, S>
where
    K: Clone + Eq + Hash + 'static,
    H1: Fn(&D1) -> (K, usize) + 'static,
    H2: Fn(&D2) -> (K, usize) + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.left_index, self.right_index]
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

        let stream_left = streams[0];
        let stream_right = streams[1];
        let stream_left = stream_left.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
        let stream_right = stream_right.as_any().downcast_ref::<Stream<S, TimestampData<D2>>>().unwrap();
        
        let key_map_left = self.key_map_left.take().unwrap();
        let key_map_right = self.key_map_right.take().unwrap();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();
        
        let stream_out = stream_left.binary(
            stream_right,
            Pipeline,
            Pipeline,
            "InnerJoin",
            |_capability, _info| {
                let mut map_left: HashMap<K, Vec<(TimestampData<D1>, MessageLatency)>> = HashMap::new();
                let mut map_right: HashMap<K, Vec<(TimestampData<D2>, MessageLatency)>> = HashMap::new();

                let mut vector_left = Vec::new();
                let mut vector_right = Vec::new();

                let mut key_count_left = HashMap::new();
                let mut key_count_right = HashMap::new();

                move |input_left, input_right, output| {
                    input_left.for_each_with_latency(|time, data, net_lat| {
                        let mut left_net_lat = if let Some(lat) = net_lat { lat }
                        else { 0 };
                        if let Some(sim_net_lat) = sim_network_latency {
                            left_net_lat = sim_net_lat;
                        }
                        if left_net_lat < 0 { left_net_lat = 0 }
                        data.swap(&mut vector_left);
                        let mut session = output.session(&time);
                        for data_point_left in vector_left.drain(..) {
                            let (key, total_num_key) = (key_map_left)(&data_point_left.data);
                            key_count_left.entry(key.clone()).or_insert(total_num_key);
                            if let Some(values) = map_right.get(&key) {
                                for (val_right, right_net_lat) in values.iter() {
                                    let min_start_ts = std::cmp::min(data_point_left.start_timestamp, val_right.start_timestamp);
                                    // let min_last_ts = std::cmp::min(data_point_left.last_timestamp, val_right.last_timestamp);
                                    let curr_ts = Utc::now().timestamp_nanos();

                                    let max_net_lat = std::cmp::max(left_net_lat, *right_net_lat);
                                    let max_total_exec_net_lat = std::cmp::max(data_point_left.total_exec_net_latency + left_net_lat, val_right.total_exec_net_latency + *right_net_lat);
                                    // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                                    // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                                    edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                                    path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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
                                    
                                    if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                        edge_latency_metrics.borrow_mut().truncate(keep_n);
                                        path_latency_metrics.borrow_mut().truncate(keep_n);
                                    }
                                    let output_data = if reset_timestamp { 
                                        TimestampData {
                                            data: (data_point_left.data.clone(), val_right.data.clone()),
                                            start_timestamp: curr_ts,
                                            last_timestamp: curr_ts,
                                            total_exec_net_latency: 0,
                                        }
                                    }
                                    else {
                                        TimestampData {
                                            data: (data_point_left.data.clone(), val_right.data.clone()),
                                            start_timestamp: min_start_ts,
                                            last_timestamp: curr_ts,
                                            total_exec_net_latency: max_total_exec_net_lat,
                                        }
                                    };
                                    session.give(output_data);
                                }
                            }
                            map_left.entry(key.clone()).or_insert(Vec::new()).push((data_point_left, left_net_lat));
                            if let Some(total_elements_right) = key_count_right.get(&key).copied() {
                                if (Some(total_num_key) == map_left.get(&key).map(|x| x.len())) &&
                                    (Some(total_elements_right) == map_right.get(&key).map(|x| x.len())) {
                                    map_left.remove(&key);
                                    map_right.remove(&key);
                                }
                            }
                        }
                    });

                    input_right.for_each_with_latency(|time, data, net_lat| {
                        let mut right_net_lat = if let Some(lat) = net_lat { lat }
                        else { 0 };
                        if let Some(sim_net_lat) = sim_network_latency {
                            right_net_lat = sim_net_lat;
                        }
                        if right_net_lat < 0 { right_net_lat = 0 }
                        data.swap(&mut vector_right);
                        let mut session = output.session(&time);
                        for data_point_right in vector_right.drain(..) {
                            let (key, total_num_key) = (key_map_right)(&data_point_right.data);
                            key_count_right.entry(key.clone()).or_insert(total_num_key);
                            if let Some(values) = map_left.get(&key) {
                                for (val_left, left_net_lat) in values.iter() {
                                    let min_start_ts = std::cmp::min(val_left.start_timestamp, data_point_right.start_timestamp);
                                    // let min_last_ts = std::cmp::min(val_left.last_timestamp, data_point_right.last_timestamp);
                                    let curr_ts = Utc::now().timestamp_nanos();
                                    
                                    let max_net_lat = std::cmp::max(*left_net_lat, right_net_lat);
                                    let max_total_exec_net_lat = std::cmp::max(val_left.total_exec_net_latency + *left_net_lat, data_point_right.total_exec_net_latency + right_net_lat);
                                    // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                                    // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                                    edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                                    path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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
                                    
                                    if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                        edge_latency_metrics.borrow_mut().truncate(keep_n);
                                        path_latency_metrics.borrow_mut().truncate(keep_n);
                                    }
                                    let output_data = if reset_timestamp { 
                                        TimestampData {
                                            data: (val_left.data.clone(), data_point_right.data.clone()),
                                            start_timestamp: curr_ts,
                                            last_timestamp: curr_ts,
                                            total_exec_net_latency: 0,
                                        }
                                    }
                                    else {
                                        TimestampData {
                                            data: (val_left.data.clone(), data_point_right.data.clone()),
                                            start_timestamp: min_start_ts,
                                            last_timestamp: curr_ts,
                                            total_exec_net_latency: max_total_exec_net_lat,
                                        }
                                    };
                                    session.give(output_data);
                                }
                            }
                            map_right.entry(key.clone()).or_insert(Vec::new()).push((data_point_right, right_net_lat));
                            if let Some(total_elements_left) = key_count_left.get(&key).copied() {
                                if (Some(total_num_key) == map_right.get(&key).map(|x| x.len())) &&
                                    (Some(total_elements_left) == map_left.get(&key).map(|x| x.len())) {
                                    map_left.remove(&key);
                                    map_right.remove(&key);
                                }
                            }
                        }
                    })
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


impl<D1: ExchangeData, D2: ExchangeData, K, H1, H2, A, T> ExchangeOpBuilder for JoinNode<D1, D2, K, H1, H2, PipelineScope<A, T>>
where
    K: Clone + Eq + Hash + 'static,
    H1: Fn(&D1) -> (K, usize) + 'static,
    H2: Fn(&D2) -> (K, usize) + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<(D1, D2)>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<(D1, D2)>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}


/// Timestamped inner join
/// key_map_left, key_map_right are hash functions
/// that maps the data on the left stream
/// and the data on the right stream into a key of type K
/// The total number of items are not required to provide, 
/// the buffer will be cleared when the frontier moves forward
pub struct TimestampJoinNode<D1, D2, K, H1, H2, S>
where
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    left_index: usize,
    right_index: usize,
    key_map_left: Option<H1>,
    key_map_right: Option<H2>,
    phantom: PhantomData<(D1, D2, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>    
}

impl<D1: Data, D2: Data, K, H1, H2, S> TimestampJoinNode<D1, D2, K, H1, H2, S>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    pub fn new(left_index: usize, right_index: usize, left_hash_logic: H1, right_hash_logic: H2)-> Self {
        TimestampJoinNode {
            left_index,
            right_index,
            key_map_left: Some(left_hash_logic),
            key_map_right: Some(right_hash_logic),
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

impl<D1: Data, D2: Data, K, H1, H2, S> LocalOpBuilder for TimestampJoinNode<D1, D2, K, H1, H2, S>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.left_index, self.right_index]
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
        
        let stream_left = streams[0];
        let stream_right = streams[1];
        let stream_left = stream_left.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
        let stream_right = stream_right.as_any().downcast_ref::<Stream<S, TimestampData<D2>>>().unwrap();
        
        let key_map_left = self.key_map_left.take().unwrap();
        let key_map_right = self.key_map_right.take().unwrap();

        let mut left_buffers: HashMap<<S as ScopeParent>::Timestamp, HashMap<K, Vec<(TimestampData<D1>, MessageLatency)>>> = HashMap::new();
        let mut right_buffers: HashMap<<S as ScopeParent>::Timestamp, HashMap<K, Vec<(TimestampData<D2>, MessageLatency)>>> = HashMap::new();

        let mut vector_left = Vec::new();
        let mut vector_right = Vec::new();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();

        let stream_out = stream_left.binary_notify(
            stream_right,
            Pipeline,
            Pipeline,
            "InnerJoin",
            vec![],
            move |input_left, input_right, output, notificator| {
                input_left.for_each_with_latency(|time, data, net_lat| {
                    let mut left_net_lat = if let Some(lat) = net_lat { lat }
                    else { 0 };
                    if let Some(sim_net_lat) = sim_network_latency {
                        left_net_lat = sim_net_lat;
                    }
                    if left_net_lat < 0 { left_net_lat = 0 }
                    data.swap(&mut vector_left);
                    let mut session = output.session(&time);
                    let map_left = left_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    let map_right = right_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    for data_point_left in vector_left.drain(..) {
                        let key = (key_map_left)(&data_point_left.data);
                        if let Some(values) = map_right.get(&key) {
                            for (val_right, right_net_lat) in values.iter() {
                                let min_start_ts = std::cmp::min(data_point_left.start_timestamp, val_right.start_timestamp);
                                // let min_last_ts = std::cmp::min(data_point_left.last_timestamp, val_right.last_timestamp);
                                let curr_ts = Utc::now().timestamp_nanos();

                                let max_net_lat = std::cmp::max(left_net_lat, *right_net_lat);
                                let max_total_exec_net_lat = std::cmp::max(data_point_left.total_exec_net_latency + left_net_lat, val_right.total_exec_net_latency + *right_net_lat);
                                // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                                // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                                edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                                path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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

                                if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                    edge_latency_metrics.borrow_mut().truncate(keep_n);
                                    path_latency_metrics.borrow_mut().truncate(keep_n);
                                }
                                let output_data = if reset_timestamp { 
                                    TimestampData {
                                        data: (data_point_left.data.clone(), val_right.data.clone()),
                                        start_timestamp: curr_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: 0,
                                    }
                                }
                                else {
                                    TimestampData {
                                        data: (data_point_left.data.clone(), val_right.data.clone()),
                                        start_timestamp: min_start_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: max_total_exec_net_lat,
                                    }  
                                };
                                session.give(output_data);
                            }
                        }
                        map_left.entry(key).or_insert(Vec::new()).push((data_point_left, left_net_lat));
                    }
                    notificator.notify_at(time.retain());
                });

                input_right.for_each_with_latency(|time, data, net_lat| {
                    let mut right_net_lat = if let Some(lat) = net_lat { lat }
                    else { 0 };
                    if let Some(sim_net_lat) = sim_network_latency {
                        right_net_lat = sim_net_lat;
                    }
                    if right_net_lat < 0 { right_net_lat = 0 }
                    data.swap(&mut vector_right);
                    let mut session = output.session(&time);
                    let map_left = left_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    let map_right = right_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    for data_point_right in vector_right.drain(..) {
                        let key = (key_map_right)(&data_point_right.data);
                        if let Some(values) = map_left.get(&key) {
                            for (val_left, left_net_lat) in values.iter() {
                                let min_start_ts = std::cmp::min(val_left.start_timestamp, data_point_right.start_timestamp);
                                // let min_last_ts = std::cmp::min(val_left.last_timestamp, data_point_right.last_timestamp);
                                let curr_ts = Utc::now().timestamp_nanos();
                                
                                let max_net_lat = std::cmp::max(*left_net_lat, right_net_lat);
                                let max_total_exec_net_lat = std::cmp::max(val_left.total_exec_net_latency + *left_net_lat, data_point_right.total_exec_net_latency + right_net_lat);
                                // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                                // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                                edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                                path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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
                                
                                if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                    edge_latency_metrics.borrow_mut().truncate(keep_n);
                                    path_latency_metrics.borrow_mut().truncate(keep_n);
                                }                                
                                let output_data = if reset_timestamp { 
                                    TimestampData {
                                        data: (val_left.data.clone(), data_point_right.data.clone()),
                                        start_timestamp: curr_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: 0,
                                    }
                                }
                                else {
                                    TimestampData {
                                        data: (val_left.data.clone(), data_point_right.data.clone()),
                                        start_timestamp: min_start_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: max_total_exec_net_lat,
                                    }
                                };
                                session.give(output_data);
                            }
                        }
                        map_right.entry(key).or_insert(Vec::new()).push((data_point_right, right_net_lat));
                    }
                    notificator.notify_at(time.retain());
                });

                notificator.for_each(|time, _, _| {
                    left_buffers.remove(time.time());
                    right_buffers.remove(time.time());
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


impl<D1: ExchangeData, D2: ExchangeData, K, H1, H2, A, T> ExchangeOpBuilder for TimestampJoinNode<D1, D2, K, H1, H2, PipelineScope<A, T>>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<(D1, D2)>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<(D1, D2)>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}

/// Single item join
/// There will be a single item with the key `k` in the left stream,
/// and a single item with the key `k` in the right stream
/// Output a single tuple for each key
/// key_map_left, key_map_right are hash functions that maps the data into key
pub struct SingleItemJoinNode<D1, D2, K, H1, H2, S>
where
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    left_index: usize,
    right_index: usize,
    key_map_left: Option<H1>,
    key_map_right: Option<H2>,
    phantom: PhantomData<(D1, D2, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>       
}


impl<D1: Data, D2: Data, K, H1, H2, S> SingleItemJoinNode<D1, D2, K, H1, H2, S>
where
    K:  Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    pub(crate) fn new(left_index: usize, right_index: usize, left_hash_logic: H1, right_hash_logic: H2)-> Self {
        SingleItemJoinNode {
            left_index,
            right_index,
            key_map_left: Some(left_hash_logic),
            key_map_right: Some(right_hash_logic),
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

impl<D1: Data, D2: Data, K, H1, H2, S> LocalOpBuilder for SingleItemJoinNode<D1, D2, K, H1, H2, S>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.left_index, self.right_index]
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

        let stream_left = streams[0];
        let stream_right = streams[1];
        let stream_left = stream_left.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
        let stream_right = stream_right.as_any().downcast_ref::<Stream<S, TimestampData<D2>>>().unwrap();

        let key_map_left = self.key_map_left.take().unwrap();
        let key_map_right = self.key_map_right.take().unwrap();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();

        let stream_out = stream_left.binary(
            stream_right,
            Pipeline,
            Pipeline,
            "SingleItemInnerJoin",
            |_capability, _info| {
                let mut map_left: HashMap<K, (TimestampData<D1>, MessageLatency)> = HashMap::new();
                let mut map_right: HashMap<K, (TimestampData<D2>, MessageLatency)> = HashMap::new();

                let mut vector_left = Vec::new();
                let mut vector_right = Vec::new();

                move |input_left, input_right, output| {
                    input_left.for_each_with_latency(|time, data, net_lat| {
                        let mut left_net_lat = if let Some(lat) = net_lat { lat }
                        else { 0 };
                        if let Some(sim_net_lat) = sim_network_latency {
                            left_net_lat = sim_net_lat;
                        }
                        if left_net_lat < 0 { left_net_lat = 0 }
                        data.swap(&mut vector_left);
                        let mut session = output.session(&time);
                        for data_point_left in vector_left.drain(..) {
                            let key = (key_map_left)(&data_point_left.data);
                            if let Some((data_point_right, right_net_lat)) = map_right.remove(&key) {
                                let min_start_ts = std::cmp::min(data_point_left.start_timestamp, data_point_right.start_timestamp);
                                // let min_last_ts = std::cmp::min(data_point_left.last_timestamp, data_point_right.last_timestamp);
                                let curr_ts = Utc::now().timestamp_nanos();

                                let max_net_lat = std::cmp::max(left_net_lat, right_net_lat);
                                let max_total_exec_net_lat = std::cmp::max(data_point_left.total_exec_net_latency + left_net_lat, data_point_right.total_exec_net_latency + right_net_lat);
                                // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                                // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                                edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                                path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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
                                                    
                                if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                    edge_latency_metrics.borrow_mut().truncate(keep_n);
                                    path_latency_metrics.borrow_mut().truncate(keep_n);
                                }
                                let output_data = if reset_timestamp { 
                                    TimestampData {
                                        data: (data_point_left.data, data_point_right.data),
                                        start_timestamp: curr_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: 0,
                                    }
                                }
                                else {
                                    TimestampData {
                                        data: (data_point_left.data, data_point_right.data),
                                        start_timestamp: min_start_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: max_total_exec_net_lat,
                                    }  
                                };
                                session.give(output_data);
                            } else {
                                map_left.insert(key, (data_point_left, left_net_lat));
                            }
                        }
                    });

                    input_right.for_each_with_latency(|time, data, net_lat| {
                        let mut right_net_lat = if let Some(lat) = net_lat { lat }
                        else { 0 };
                        if let Some(sim_net_lat) = sim_network_latency {
                            right_net_lat = sim_net_lat;
                        }
                        if right_net_lat < 0 { right_net_lat = 0 }
                        data.swap(&mut vector_right);
                        let mut session = output.session(&time);
                        for data_point_right in vector_right.drain(..) {
                            let key = (key_map_right)(&data_point_right.data);
                            if let Some((data_point_left, left_net_lat)) = map_left.remove(&key) {
                                let min_start_ts = std::cmp::min(data_point_left.start_timestamp, data_point_right.start_timestamp);
                                // let min_last_ts = std::cmp::min(data_point_left.last_timestamp, data_point_right.last_timestamp);
                                let curr_ts = Utc::now().timestamp_nanos();

                                let max_net_lat = std::cmp::max(left_net_lat, right_net_lat);
                                let max_total_exec_net_lat = std::cmp::max(data_point_left.total_exec_net_latency + left_net_lat, data_point_right.total_exec_net_latency + right_net_lat);
                                // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                                // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                                edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                                path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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
                                                    
                                if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                    edge_latency_metrics.borrow_mut().truncate(keep_n);
                                    path_latency_metrics.borrow_mut().truncate(keep_n);
                                }
                                let output_data = if reset_timestamp { 
                                    TimestampData {
                                        data: (data_point_left.data, data_point_right.data),
                                        start_timestamp: curr_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: 0,
                                    }
                                }
                                else {
                                    TimestampData {
                                        data: (data_point_left.data, data_point_right.data),
                                        start_timestamp: min_start_ts,
                                        last_timestamp: curr_ts,
                                        total_exec_net_latency: max_total_exec_net_lat,
                                    }  
                                };
                                session.give(output_data);
                            } else {
                                map_right.insert(key, (data_point_right, right_net_lat));
                            }
                        }
                    })
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

impl<D1: ExchangeData, D2: ExchangeData, K, H1, H2, A, T> ExchangeOpBuilder for SingleItemJoinNode<D1, D2, K, H1, H2, PipelineScope<A, T>>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<(D1, D2)>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<(D1, D2)>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}

/// Timestamped single item join
/// There will be a single item with the key `k` in the left stream,
/// and a single item with the key `k` in the right stream at each timestamp
/// Output a single tuple for each key at each timestamp
/// key_map_left, key_map_right are hash functions that maps the data into key
pub struct TimestampSingleItemJoinNode<D1, D2, K, H1, H2, S>
where
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    left_index: usize,
    right_index: usize,
    key_map_left: Option<H1>,
    key_map_right: Option<H2>,
    phantom: PhantomData<(D1, D2, K)>,
    phantom_scope: PhantomData<S>,
    edge_latencies: Rc<RefCell<VecDeque<i64>>>,
    path_latencies: Rc<RefCell<VecDeque<i64>>>,
    data_start_timestamp: Rc<RefCell<Option<i64>>>,
    start_timestamp: Rc<RefCell<Option<i64>>>,
    end_timestamp: Rc<RefCell<Option<i64>>>    
}


impl<D1: Data, D2: Data, K, H1, H2, S> TimestampSingleItemJoinNode<D1, D2, K, H1, H2, S>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    pub(crate) fn new(left_index: usize, right_index: usize, left_hash_logic: H1, right_hash_logic: H2)-> Self {
        TimestampSingleItemJoinNode {
            left_index,
            right_index,
            key_map_left: Some(left_hash_logic),
            key_map_right: Some(right_hash_logic),
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

impl<D1: Data, D2: Data, K, H1, H2, S> LocalOpBuilder for TimestampSingleItemJoinNode<D1, D2, K, H1, H2, S>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.left_index, self.right_index]
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

        let stream_left = streams[0];
        let stream_right = streams[1];
        let stream_left = stream_left.as_any().downcast_ref::<Stream<S, TimestampData<D1>>>().unwrap();
        let stream_right = stream_right.as_any().downcast_ref::<Stream<S, TimestampData<D2>>>().unwrap();

        let key_map_left = self.key_map_left.take().unwrap();
        let key_map_right = self.key_map_right.take().unwrap();

        let mut left_buffers: HashMap<<S as ScopeParent>::Timestamp, HashMap<K, (TimestampData<D1>, MessageLatency)>> = HashMap::new();
        let mut right_buffers: HashMap<<S as ScopeParent>::Timestamp, HashMap<K, (TimestampData<D2>, MessageLatency)>> = HashMap::new();

        let mut vector_left = Vec::new();
        let mut vector_right = Vec::new();

        let edge_latency_metrics = self.edge_latencies.clone();
        let path_latency_metrics = self.path_latencies.clone();
        let data_start_timestamp = self.data_start_timestamp.clone();
        let start_timestamp = self.start_timestamp.clone();
        let end_timestamp = self.end_timestamp.clone();

        let stream_out = stream_left.binary_notify(
            stream_right,
            Pipeline,
            Pipeline,
            "SingleItemInnerJoin",
            vec![],
            move |input_left, input_right, output, notificator| {
                input_left.for_each_with_latency(|time, data, net_lat| {
                    let mut left_net_lat = if let Some(lat) = net_lat { lat }
                    else { 0 };
                    if let Some(sim_net_lat) = sim_network_latency {
                        left_net_lat = sim_net_lat;
                    }
                    if left_net_lat < 0 { left_net_lat = 0 }
                    data.swap(&mut vector_left);
                    let mut session = output.session(&time);
                    let map_left = left_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    let map_right = right_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    for data_point_left in vector_left.drain(..) {
                        let key = (key_map_left)(&data_point_left.data);
                        if let Some((data_point_right, right_net_lat)) = map_right.remove(&key) {
                            let min_start_ts = std::cmp::min(data_point_left.start_timestamp, data_point_right.start_timestamp);
                            // let min_last_ts = std::cmp::min(data_point_left.last_timestamp, data_point_right.last_timestamp);
                            let curr_ts = Utc::now().timestamp_nanos();

                            let max_net_lat = std::cmp::max(left_net_lat, right_net_lat);
                            let max_total_exec_net_lat = std::cmp::max(data_point_left.total_exec_net_latency + left_net_lat, data_point_right.total_exec_net_latency + right_net_lat);
                            // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                            // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                            edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                            path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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
                                            
                            if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                edge_latency_metrics.borrow_mut().truncate(keep_n);
                                path_latency_metrics.borrow_mut().truncate(keep_n);
                            }
                            let output_data = if reset_timestamp { 
                                TimestampData {
                                    data: (data_point_left.data, data_point_right.data),
                                    start_timestamp: curr_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: 0,
                                }
                            }
                            else {
                                TimestampData {
                                    data: (data_point_left.data, data_point_right.data),
                                    start_timestamp: min_start_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: max_total_exec_net_lat,
                                }  
                            };
                            session.give(output_data);
                        } else {
                            map_left.insert(key, (data_point_left, left_net_lat));
                        }
                    }
                    notificator.notify_at(time.retain());
                });

                input_right.for_each_with_latency(|time, data, net_lat| {
                    let mut right_net_lat = if let Some(lat) = net_lat { lat }
                    else { 0 };
                    if let Some(sim_net_lat) = sim_network_latency {
                        right_net_lat = sim_net_lat;
                    }
                    if right_net_lat < 0 { right_net_lat = 0 }
                    data.swap(&mut vector_right);
                    let mut session = output.session(&time);
                    let map_left = left_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    let map_right = right_buffers.entry(time.time().clone()).or_insert_with(HashMap::new);
                    for data_point_right in vector_right.drain(..) {
                        let key = (key_map_right)(&data_point_right.data);
                        if let Some((data_point_left, left_net_lat)) = map_left.remove(&key) {
                            let min_start_ts = std::cmp::min(data_point_left.start_timestamp, data_point_right.start_timestamp);
                            // let min_last_ts = std::cmp::min(data_point_left.last_timestamp, data_point_right.last_timestamp);
                            let curr_ts = Utc::now().timestamp_nanos();

                            let max_net_lat = std::cmp::max(left_net_lat, right_net_lat);
                            let max_total_exec_net_lat = std::cmp::max(data_point_left.total_exec_net_latency + left_net_lat, data_point_right.total_exec_net_latency + right_net_lat);
                            // edge_latency_metrics.borrow_mut().push_front(curr_ts - min_last_ts);
                            // path_latency_metrics.borrow_mut().push_front(curr_ts - min_start_ts);
                            edge_latency_metrics.borrow_mut().push_front(max_net_lat);
                            path_latency_metrics.borrow_mut().push_front(max_total_exec_net_lat);
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

                            if let Some(keep_n) = METRIC_KEEP_LAST_N {
                                edge_latency_metrics.borrow_mut().truncate(keep_n);
                                path_latency_metrics.borrow_mut().truncate(keep_n);
                            }
                            let output_data = if reset_timestamp { 
                                TimestampData {
                                    data: (data_point_left.data, data_point_right.data),
                                    start_timestamp: curr_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: 0,
                                }
                            }
                            else {
                                TimestampData {
                                    data: (data_point_left.data, data_point_right.data),
                                    start_timestamp: min_start_ts,
                                    last_timestamp: curr_ts,
                                    total_exec_net_latency: max_total_exec_net_lat,
                                }  
                            };
                            session.give(output_data);
                        } else {
                            map_right.insert(key, (data_point_right, right_net_lat));
                        }
                    }
                    notificator.notify_at(time.retain());
                });


                notificator.for_each(|time, _, _| {
                    left_buffers.remove(time.time());
                    right_buffers.remove(time.time());
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

impl<D1: ExchangeData, D2: ExchangeData, K, H1, H2, A, T> ExchangeOpBuilder for TimestampSingleItemJoinNode<D1, D2, K, H1, H2, PipelineScope<A, T>>
where
    K: Eq + Hash + 'static,
    H1: Fn(&D1) -> K + 'static,
    H2: Fn(&D2) -> K + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<(D1, D2)>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<(D1, D2)>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}
