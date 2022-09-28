use std::collections::HashMap;
use std::iter::FromIterator;

use structopt::StructOpt;

use timely::communication::RelayNodeConfig;
use timely::relay::{execute_from_config, InputToWorkerExchangePattern, RelayConfig, RelayToOutputExchangePattern};

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages pipeline timely dataflow relay node")]
pub struct Opts {
    #[structopt(short, long, required_if("pipeline", "0"), required_if("pipeline", "1"))]
    pub relay: Option<usize>,
    /// Which pipeline, pipeline 0 or 1
    #[structopt(short, long)]
    pub pipeline: i32
}

fn run_relay_pipeline_0(relay_index: usize) {
    let my_addr = if relay_index == 0 {
        String::from("127.0.0.1:6001")
    }
    else {
        String::from("127.0.0.1:6002")
    };
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![],
            output_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6003"), String::from("127.0.0.1:6004")]],
            timely_workers_addresses: vec![String::from("127.0.0.1:5001")],
            threads_per_timely_worker_process: 1,
            my_addr,
            my_index: relay_index,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        },
        input_index_mapping: vec![],
        required_outputs: vec![vec![0, 1]],
        input_to_worker_exchange_patterns: Some(HashMap::new()),
        relay_to_output_exchange_pattern: Some(RelayToOutputExchangePattern::Random),
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };
    execute_from_config(config);
}


fn run_relay_pipeline_1(relay_index: usize) {
    let my_addr = if relay_index == 0 {
        String::from("127.0.0.1:6003")
    }
    else {
        String::from("127.0.0.1:6004")
    };
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6001"), String::from("127.0.0.1:6002")]],
            output_relay_nodes_addresses: vec![],
            timely_workers_addresses: vec![String::from("127.0.0.1:5002")],
            threads_per_timely_worker_process: 1,
            my_addr,
            my_index: relay_index,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        },
        input_index_mapping: vec![HashMap::from_iter([
            (0, 0),
            (1, 1)
        ])],
        required_outputs: vec![],
        input_to_worker_exchange_patterns: Some(HashMap::from_iter([
            (0, InputToWorkerExchangePattern::Random),
            (1, InputToWorkerExchangePattern::Random)
        ])),
        relay_to_output_exchange_pattern: Some(RelayToOutputExchangePattern::Random),
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };

    execute_from_config(config);
}

fn main() {
    let opt = Opts::from_args();
    if opt.pipeline == 0 {
        let my_index = opt.relay.unwrap();
        run_relay_pipeline_0(my_index);
    }
    else {
        let my_index = opt.relay.unwrap();
        run_relay_pipeline_1(my_index);
    }
}