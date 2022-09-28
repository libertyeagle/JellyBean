mod relay;
mod worker;

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

use structopt::StructOpt;

use mlflow::{PipelineConfigGUID, ExecutionConfigGUID};

use crate::relay::run_pipeline_relay;
use crate::worker::run_pipeline_worker;

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages pipeline MLflow API test")]
pub struct Opts {
    #[structopt(short, long)]
    pub worker: bool,
    #[structopt(short, long)]
    pub relay: bool,
    /// Which pipeline, pipeline 0 or 1
    #[structopt(short, long)]
    pub pipeline: usize
}


fn main() {
    let pipeline_0_config = PipelineConfigGUID {
        pipeline_index: 0,
        assigned_ops: vec![0, 1, 2, 3],
        required_input_ops: Some(vec![]),
        output_ops: Some(vec![1, 2, 3]),
        worker_addrs: vec![String::from("127.0.0.1:5000")],
        relay_addrs: vec![String::from("127.0.0.1:6000")],
        relay_load_balance_weights: None,
        input_pipelines: vec![],
        output_pipelines: vec![1],
        builder_configs: HashMap::new(),
        operator_configs: HashMap::new(),
        request_rates: HashMap::new(),
        source_operators: HashSet::new(),
    };

    let pipeline_1_config = PipelineConfigGUID {
        pipeline_index: 1,
        assigned_ops: vec![4, 5, 6],
        required_input_ops: Some(vec![1, 2]),
        output_ops: Some(vec![]),
        worker_addrs: vec![String::from("127.0.0.1:5001")],
        relay_addrs: vec![String::from("127.0.0.1:6001")],
        relay_load_balance_weights: None,
        input_pipelines: vec![0],
        output_pipelines: vec![],
        builder_configs: HashMap::new(),
        operator_configs: HashMap::new(),
        request_rates: HashMap::new(),
        source_operators: HashSet::new(),
    };

    let config = HashMap::from_iter([
        (0, pipeline_0_config),
        (1, pipeline_1_config)
    ]);

    let config = ExecutionConfigGUID {
        pipeline_configs: config,
        op_name_guid_mapping: None,
        graph_connections: None,
        message_buffer_size: None
    };
    
    let opt = Opts::from_args();
    assert!(opt.worker ^ opt.relay, "run either pipeline worker or relay");
    if opt.worker {
        run_pipeline_worker(&config, opt.pipeline);
    }
    else {
        run_pipeline_relay(&config, opt.pipeline);
    }
}
