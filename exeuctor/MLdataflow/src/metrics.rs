use std::cell::RefCell;
use std::collections::{VecDeque, HashMap, BTreeMap};
use std::rc::Rc;
use std::ops::Deref;

use serde::{Serialize, Deserialize};
use statrs::statistics::{OrderStatistics, Median, Min, Max, Distribution};
use statrs::statistics::Data as StatData;

pub struct MetricsLogger {
    // op global id -> logger
    pub(crate) throughput_loggers: HashMap<usize, ThroughputLogger>,
    pub(crate) execution_latency_loggers: HashMap<usize, LatencyLogger>,
    pub(crate) edge_latency_loggers: HashMap<usize, LatencyLogger>,
    pub(crate) path_latency_loggers: HashMap<usize, LatencyLogger>,
    pub(crate) jct_loggers: HashMap<usize, JCTLogger>
}


#[derive(Debug, Clone)]
/// Metrics for an operator
#[derive(Serialize, Deserialize)]
pub struct OperatorMetricsStats {
    /// throuhgput in #req/s
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput: Option<BTreeMap<String, f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overall_throughput: Option<f64>,
    /// execution latency, edge latency, path latency
    /// in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<BTreeMap<String, BTreeMap<String, f64>>>,
    /// Operator job completion time (in seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator_jct: Option<f64>,
    /// Dataflow path job completion time (in seconds)
    pub path_jct: Option<f64>
}

/// Metrics for all logged operators
pub type MetricsStats = HashMap<String, OperatorMetricsStats>;

#[derive(Debug)]
pub(crate) struct RcWrapper<T> {
    inner: Rc<T>
}

impl<T: Send> RcWrapper<T> {
    pub(crate) fn new(data: Rc<T>) -> RcWrapper<T> {
        RcWrapper {
            inner: data
        }
    }
}

unsafe impl<T> Send for RcWrapper<T> {}
unsafe impl<T> Sync for RcWrapper<T> {}

impl<T> Deref for RcWrapper<T> {
    type Target = Rc<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct ThroughputLogger {
    pub(crate) throughput: RcWrapper<RefCell<VecDeque<f64>>>,
    pub(crate) warmup_throughput: RcWrapper<RefCell<Option<f64>>>,
    pub(crate) overall_throughput: RcWrapper<RefCell<Option<f64>>>,
}

pub struct LatencyLogger {
    pub(crate) latencies: RcWrapper<RefCell<VecDeque<i64>>>
}

pub struct JCTLogger {
    pub(crate) data_start_timestamp: RcWrapper<RefCell<Option<i64>>>,
    pub(crate) op_start_timestamp: RcWrapper<RefCell<Option<i64>>>,
    pub(crate) op_end_timestamp: RcWrapper<RefCell<Option<i64>>>
}

impl LatencyLogger {
    pub fn compute_latency(&self) -> Option<BTreeMap<String, f64>> {
        if self.latencies.borrow().is_empty() {
            None
        }
        else {
            let latency_data = self.latencies.borrow().iter().map(|x| *x as f64).collect::<Vec<_>>();
            let mut latency_data = StatData::new(latency_data);
            let thresholds = [10, 50, 75, 90, 95, 99, 999];
            let mut stats = BTreeMap::new();
            for p in thresholds {
                let lat = latency_data.percentile(p);
                stats.insert(format!("P{}", p), lat / 1e6_f64);
            }  
        Some(stats)
        }
    }

    pub fn get_all_latencies(&self) -> Vec<i64> {
        let latency_data = self.latencies.borrow().clone().into_iter().collect::<Vec<_>>();
        latency_data
    }
}

impl ThroughputLogger {
    pub fn compute_throughput(&self) -> Option<BTreeMap<String, f64>> {
        if self.throughput.borrow().is_empty() {
            if let Some(warmup_tp) = *self.warmup_throughput.borrow() {
                let mut stats = BTreeMap::new();
                stats.insert(String::from("Overall"), warmup_tp);
                Some(stats)
            }
            else {
                None
            }
        }
        else {
            let throughput_data = self.throughput.borrow().iter().map(|x| *x).collect::<Vec<_>>();
            let mut throughput_data = StatData::new(throughput_data);
            let max_tp = throughput_data.max();
            let min_tp = throughput_data.min();
            let mean_tp = throughput_data.mean().unwrap();
            let std_tp = throughput_data.std_dev().unwrap();
            let median_tp = throughput_data.median();
            let mut stats = BTreeMap::new();
            stats.insert(String::from("Mean"), mean_tp);
            stats.insert(String::from("Std"), std_tp);            
            stats.insert(String::from("Min"), min_tp);
            stats.insert(String::from("Max"), max_tp);
            stats.insert(String::from("Median"), median_tp);
            stats.insert(String::from("P25"),  throughput_data.percentile(25));
            stats.insert(String::from("P75"), throughput_data.percentile(75));
            stats.insert(String::from("P90"), throughput_data.percentile(90));
            Some(stats)
        }
    }

    pub fn compute_overall_throughput(&self) -> Option<f64> {
        let overall_throughput = self.overall_throughput.borrow().clone();
        overall_throughput
    }
}

impl JCTLogger {
    /// Return operator JCT in seconds (from when the first request is received at this operator to the last request finished processing)
    pub fn compute_operator_job_completion_time(&self) -> Option<f64> {
        if self.op_start_timestamp.borrow().is_none() || self.op_end_timestamp.borrow().is_none() {
            None
        }
        else {
            let start_ts = self.op_start_timestamp.borrow().unwrap();
            let end_ts = self.op_end_timestamp.borrow().unwrap();
            Some((end_ts - start_ts) as f64 / 1e9_f64)
        }
    }
    
    /// Return path JCT in seconds
    /// from when the first request launched (the minimal start_timestamp ever seen) to the last request finished processing
    pub fn compute_path_job_completion_time(&self) -> Option<f64> {
        if self.data_start_timestamp.borrow().is_none() || self.op_end_timestamp.borrow().is_none() {
            None
        }
        else {
            let start_ts = self.data_start_timestamp.borrow().unwrap();
            let end_ts = self.op_end_timestamp.borrow().unwrap();
            Some((end_ts - start_ts) as f64 / 1e9_f64)
        }
    }
}