mod node;
mod input;
mod graph;

pub mod static_timely;
pub mod operators_timely;
pub mod builder;
pub mod handle;
pub mod execute;
pub mod config;
pub mod utils;
pub mod metrics;

pub use builder::{PipelineGraphBuilder, GraphBuilder};
pub use config::{PipelineConfigGUID, ExecutionConfigGUID};
pub use config::{PipelineConfig, ExecutionConfig};
pub use handle::Handle;
pub use handle::{Map, MapLocal};
pub use handle::{Filter, FilterLocal};
pub use handle::{Join, JoinLocal};
pub use handle::{Aggregate, AggregateLocal};
pub use handle::{Inspect, InspectLocal};
pub use handle::Exchange;

pub use execute::{pipeline_worker_execute, pipeline_relay_execute};
pub use execute::{local_execute, local_execute_thread, local_execute_process};

#[cfg(feature = "bincode")]
use serde::{Serialize, Deserialize};
#[cfg(not(feature = "bincode"))]
use abomonation_derive::Abomonation;

use timely::communication::{MessageTimestamp, MessageLatency};

/// Stream data with timestamp to measure latency
#[derive(Debug, Clone)]
#[cfg_attr(feature="bincode", derive(Serialize, Deserialize))]
#[cfg_attr(not(feature="bincode"), derive(Abomonation))]
pub struct TimestampData<D>{
    /// Data packet
    pub data: D,
    /// Timestamp that the request related to this data arrives
    /// i.e., starts processing
    /// timestamp at the source
    pub start_timestamp: MessageTimestamp,
    /// Timestamp when the last operation finished
    /// i.e., the timestamp that this output `data` is emitted
    pub last_timestamp: MessageTimestamp,
    /// Total operator execution and cross pipeline 
    /// network transmission latency along the
    pub total_exec_net_latency: MessageLatency
}