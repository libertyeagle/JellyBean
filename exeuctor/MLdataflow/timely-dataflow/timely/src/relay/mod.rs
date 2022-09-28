//! Building mechanism for relay node

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::communication::RelayNodeConfig as RelayNodeCommConfig;

pub use execute::{execute_from_config, execute_from_udf};

pub mod execute;
pub mod registry;
mod connector;


/// Define for each scope input
/// When receiving a message (data) from the input pipeline
/// How do we distribute it to the timely workers
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum InputToWorkerExchangePattern {
    /// Randomly distribute it to one of the timely worker
    Random,
    /// Evenly distribute (balance) the messages to the timely workers
    Balance,
    /// Broadcast the message to all timely workers
    Broadcast
}

/// Define how the output message (data)
/// should be distributed to the relay nodes in the output pipeline
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum RelayToOutputExchangePattern {
    /// Randomly distribute it to one of the relay node
    Random,
    /// Balance the load to each relay node
    Balance
}

pub struct RelayConfig {
    /// Config for the communication infra
    pub comm_config: RelayNodeCommConfig,
    /// HashMap to map the input from the output's index of the input pipelines
    /// to the input index in the current pipeline
    /// One such HashMap for each of the input pipeline
    /// We acquire a part of the scope inputs from each input pipeline
    pub input_index_mapping: Vec<HashMap<usize, usize>>,
    /// The scope output (indices) required by the output pipelines
    /// Define the set of the required output indices for each of the output pipeline
    pub required_outputs: Vec<Vec<usize>>,
    /// Define the message exchange pattern (relay->worker) for each of the scope input
    pub input_to_worker_exchange_patterns: Option<HashMap<usize, InputToWorkerExchangePattern>>,
    /// Define how should the output messages be sent to the relay nodes in output pipelines
    pub relay_to_output_exchange_pattern: Option<RelayToOutputExchangePattern>,
    /// Since different workers in an output pipeline have different computation capability
    /// We need to distribute the data to the workers according to some ratios
    /// This is a map that maps the output pipeline index to a vector of ratios to distribute
    /// to each output pipeline relay node.
    pub output_pipelines_relay_load_balance_ratios: HashMap<usize, Vec<f64>>
}