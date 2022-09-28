use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use crate::graph::GraphConnections;

/// Config for a pipeline
/// Including the nodes to execute on this pipeline
/// The required inputs operators (indices) to be obtained from input pipelines
/// And operators to pass to the output pipelines
#[derive(Clone, Debug)]
pub struct PipelineConfigGUID {
    pub pipeline_index: usize,
    pub assigned_ops: Vec<usize>,
    pub required_input_ops: Option<Vec<usize>>,
    pub output_ops: Option<Vec<usize>>,
    pub worker_addrs: Vec<String>,
    pub relay_addrs: Vec<String>,
    pub relay_load_balance_weights: Option<Vec<f64>>,
    pub input_pipelines: Vec<usize>,
    pub output_pipelines: Vec<usize>,
    pub builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>>,
    pub operator_configs: HashMap<usize, HashMap<String, Arc<dyn Any + Send + Sync>>>,
    /// Request sending rate for input sources (request rate per timely worker)
    pub request_rates: HashMap<usize, f64>,
    /// Source operators of the dataflow that we mark the start timestamp
    pub source_operators: HashSet<usize>,
}

/// Execution config for the entire dataflow (all pipelines)
/// Map from pipeline index to pipeline config
#[derive(Clone, Debug)]
pub struct ExecutionConfigGUID {
    pub pipeline_configs: HashMap<usize, PipelineConfigGUID>,
    pub op_name_guid_mapping: Option<HashMap<String, usize>>,
    pub graph_connections: Option<GraphConnections>,
    pub message_buffer_size: Option<usize>
}

#[derive(Clone, Debug)]
pub struct PipelineConfig {
    pub pipeline_index: usize,
    pub assigned_ops: Vec<String>,
    pub required_input_ops: Option<Vec<String>>,
    pub output_ops: Option<Vec<String>>,
    pub worker_addrs: Vec<String>,
    pub relay_addrs: Vec<String>,
    pub relay_load_balance_weights: Option<Vec<f64>>,
    pub input_pipelines: Vec<usize>,
    pub output_pipelines: Vec<usize>,
    pub builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>>,
    pub operator_configs: HashMap<String, HashMap<String, Arc<dyn Any + Send + Sync>>>,
    /// Request sending rate for input sources
    pub request_rates: HashMap<String, f64>,
    /// Source operators of the dataflow that we mark the start timestamp
    pub source_operators: HashSet<String>,    
}

#[derive(Clone, Debug)]
pub struct ExecutionConfig {
    pub pipeline_configs: HashMap<usize, PipelineConfig>,
    pub op_name_guid_mapping: HashMap<String, usize>,
    pub graph_connections: Option<GraphConnections>,
    pub metrics_logging_dir: Option<PathBuf>,
    pub message_buffer_size: Option<usize>
}

impl ExecutionConfig {
    pub fn new_with_default_mapping(pipeline_configs: HashMap<usize, PipelineConfig>, metrics_logging_dir: Option<PathBuf>, message_buffer_size: Option<usize>) -> Self {
        let mut all_ops_set = HashSet::new();
        for pipeline_config in pipeline_configs.values() {
            for op in &pipeline_config.assigned_ops {
                all_ops_set.insert(op.clone());
            }
        }
        let mut all_ops = Vec::from_iter(all_ops_set);
        all_ops.sort();
        let mapping = all_ops.into_iter().enumerate().map(|(i, x)| (x, i)).collect::<HashMap<_, _>>();
        
        ExecutionConfig {
            pipeline_configs,
            op_name_guid_mapping: mapping,
            graph_connections: None,
            metrics_logging_dir,
            message_buffer_size
        }
    }

    pub fn to_guid(&self) -> ExecutionConfigGUID {
        let mut pipeline_configs_guid = HashMap::with_capacity(self.pipeline_configs.len());
        let op_name_guid_mapping = self.op_name_guid_mapping.clone();
        for (pipeline_idx, pipeline_config) in self.pipeline_configs.clone() {
            let required_input_ops = pipeline_config.required_input_ops.map(
                |ops| ops.into_iter().map(
                    |x| *op_name_guid_mapping.get(&x).unwrap()
                ).collect::<Vec<_>>()
            );
            let output_ops = pipeline_config.output_ops.map(
                |ops| ops.into_iter().map(
                    |x| *op_name_guid_mapping.get(&x).unwrap()
                ).collect::<Vec<_>>()
            );            
            let assigned_ops = pipeline_config.assigned_ops.into_iter().map(
                |x| *op_name_guid_mapping.get(&x).unwrap()
            ).collect::<Vec<_>>();

            let operators_configs = pipeline_config.operator_configs.into_iter().map(
                |(k, v)| (*op_name_guid_mapping.get(&k).unwrap(), v)
            ).collect();

            let request_rates = pipeline_config.request_rates.into_iter().map(
                |(k, v)| (*op_name_guid_mapping.get(&k).unwrap(), v)
            ).collect();

            let source_operators = pipeline_config.source_operators.into_iter().map(
                |op| *op_name_guid_mapping.get(&op).unwrap()
            ).collect();

            let pipeline_config_guid = PipelineConfigGUID {
                pipeline_index: pipeline_config.pipeline_index,
                assigned_ops,
                required_input_ops,
                output_ops,
                worker_addrs: pipeline_config.worker_addrs,
                relay_addrs: pipeline_config.relay_addrs,
                relay_load_balance_weights: pipeline_config.relay_load_balance_weights,
                input_pipelines: pipeline_config.input_pipelines,
                output_pipelines: pipeline_config.output_pipelines,
                builder_configs: pipeline_config.builder_configs,
                operator_configs: operators_configs,
                request_rates,
                source_operators,
            };
            pipeline_configs_guid.insert(pipeline_idx, pipeline_config_guid);
        }

        ExecutionConfigGUID {
            pipeline_configs: pipeline_configs_guid,
            op_name_guid_mapping: Some(self.op_name_guid_mapping.clone()),
            graph_connections: self.graph_connections.clone(),
            message_buffer_size: self.message_buffer_size
        }
    }
}