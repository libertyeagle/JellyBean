use std::collections::HashMap;

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct PipelineSpecification {
    pub worker_addrs: Vec<String>,
    pub relay_addrs: Vec<String>,
    // Weight of each relay/worker for load balancing.
    // Assume the number of relay nodes and the number of workers are the same each pipeline
    pub relay_weights: Option<Vec<f64>>,
    pub model_assignments: HashMap<String, String>,
    // operator_name -> device placement (a Vec, the device to use for each worker)
    pub device_placements: HashMap<String, Vec<String>>,
    // simulate cross-pipeline network latency (operator name -> network latency), optional
    pub simulate_network_latency: Option<HashMap<String, i64>>,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct VQAWorkflowConfig {
    pub request_rate: f64,
    pub dataset_path: String,
    pub logging_dir: String,
    pub pipeline_specs: HashMap<String, PipelineSpecification>,
    pub buffer_read: Option<bool>,
    // Number of instances to use in the VQA dataset
    pub num_instances: Option<usize>
}