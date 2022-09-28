use std::collections::HashMap;
use std::iter::FromIterator;

use structopt::StructOpt;

use timely::communication::RelayNodeConfig;
use timely::relay::{execute_from_config, InputToWorkerExchangePattern, RelayConfig, RelayToOutputExchangePattern};

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages pipeline timely dataflow relay node")]
pub struct Opts {
    /// Which pipeline, pipeline 0/1/2
    #[structopt(short, long)]
    pub pipeline: i32,
}

fn run_relay_pipeline_0() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![],
            output_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6003")]],
            timely_workers_addresses: vec![String::from("127.0.0.1:5001")],
            threads_per_timely_worker_process: 1,
            my_addr: String::from("127.0.0.1:6001"),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None),
        },
        input_index_mapping: vec![],
        required_outputs: vec![vec![0]],
        input_to_worker_exchange_patterns: Some(HashMap::new()),
        relay_to_output_exchange_pattern: Some(RelayToOutputExchangePattern::Random),
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };
    execute_from_config(config);
}


fn run_relay_pipeline_1() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![],
            output_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6003")]],
            timely_workers_addresses: vec![String::from("127.0.0.1:5002")],
            threads_per_timely_worker_process: 1,
            my_addr: String::from("127.0.0.1:6002"),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None),
        },
        input_index_mapping: vec![],
        required_outputs: vec![vec![0]],
        input_to_worker_exchange_patterns: Some(HashMap::new()),
        relay_to_output_exchange_pattern: Some(RelayToOutputExchangePattern::Random),
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };

    execute_from_config(config);
}

fn run_relay_pipeline_2() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6001")], vec![String::from("127.0.0.1:6002")]],
            output_relay_nodes_addresses: vec![],
            timely_workers_addresses: vec![String::from("127.0.0.1:5003")],
            threads_per_timely_worker_process: 1,
            my_addr: String::from("127.0.0.1:6003"),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None),
        },
        input_index_mapping: vec![HashMap::from_iter([
            (0, 0),
        ]), HashMap::from_iter([
            (0, 0)
        ])],
        required_outputs: vec![],
        input_to_worker_exchange_patterns: Some(HashMap::from_iter([
            (0, InputToWorkerExchangePattern::Random),
        ])),
        relay_to_output_exchange_pattern: Some(RelayToOutputExchangePattern::Random),
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };

    execute_from_config(config);
}


fn main() {
    let opt = Opts::from_args();
    if opt.pipeline == 0 {
        run_relay_pipeline_0();
    } else if opt.pipeline == 1 {
        run_relay_pipeline_1();
    } else {
        run_relay_pipeline_2();
    }
}