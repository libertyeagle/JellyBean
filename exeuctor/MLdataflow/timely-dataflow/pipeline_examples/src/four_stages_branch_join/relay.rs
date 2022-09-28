use std::collections::HashMap;
use std::iter::FromIterator;

use structopt::StructOpt;

use timely::communication::RelayNodeConfig;
use timely::relay::{execute_from_config, RelayConfig};

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Four-pipelines branch-join timely dataflow relay node")]
pub struct Opts {
    /// Which pipeline, pipeline 0/1/2/3
    #[structopt(short, long)]
    pub pipeline: i32
}

fn run_relay_pipeline_0() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![],
            output_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6100")], vec![String::from("127.0.0.1:6200")]],
            timely_workers_addresses: vec![String::from("127.0.0.1:5000"), String::from("127.0.0.1:5001")],
            threads_per_timely_worker_process: 2,
            my_addr: "127.0.0.1:6000".to_string(),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        },
        input_index_mapping: vec![],
        required_outputs: vec![vec![0, 1], vec![1]],
        input_to_worker_exchange_patterns: None,
        relay_to_output_exchange_pattern: None,
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };
    execute_from_config(config);
}


fn run_relay_pipeline_1() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6000")]],
            output_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6300")]],
            timely_workers_addresses: vec![String::from("127.0.0.1:5100")],
            threads_per_timely_worker_process: 2,
            my_addr: "127.0.0.1:6100".to_string(),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        },
        input_index_mapping: vec![HashMap::from_iter([
            (0, 1),
            (1, 0)
        ])],
        required_outputs: vec![vec![0]],
        input_to_worker_exchange_patterns: None,
        relay_to_output_exchange_pattern: None,
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };

    execute_from_config(config);
}

fn run_relay_pipeline_2() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6000")]],
            output_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6300")]],
            timely_workers_addresses: vec![String::from("127.0.0.1:5200")],
            threads_per_timely_worker_process: 1,
            my_addr: "127.0.0.1:6200".to_string(),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        },
        input_index_mapping: vec![HashMap::from_iter([
            (1, 0),
        ])],
        required_outputs: vec![vec![0]],
        input_to_worker_exchange_patterns: None,
        relay_to_output_exchange_pattern: None,
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };

    execute_from_config(config);
}

fn run_relay_pipeline_3() {
    let config = RelayConfig {
        comm_config: RelayNodeConfig {
            input_relay_nodes_addresses: vec![vec![String::from("127.0.0.1:6100")], vec![String::from("127.0.0.1:6200")]],
            output_relay_nodes_addresses: vec![],
            timely_workers_addresses:  vec![String::from("127.0.0.1:5300"), String::from("127.0.0.1:5301")],
            threads_per_timely_worker_process: 1,
            my_addr: "127.0.0.1:6300".to_string(),
            my_index: 0,
            num_relay_nodes_peers: 1,
            report: true,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        },
        input_index_mapping: vec![
            HashMap::from_iter([(0, 1)]),
            HashMap::from_iter([(0, 0)])
        ],
        required_outputs: vec![vec![0]],
        input_to_worker_exchange_patterns: None,
        relay_to_output_exchange_pattern: None,
        output_pipelines_relay_load_balance_ratios: HashMap::new(),
    };

    execute_from_config(config);
}


fn main() {
    let opt = Opts::from_args();
    assert!(opt.pipeline >= 0 && opt.pipeline <= 3, "pipeline must be 0/1/2/3");
    if opt.pipeline == 0 {
        run_relay_pipeline_0();
    }
    else if opt.pipeline == 1 {
        run_relay_pipeline_1();
    }
    else if opt.pipeline == 2 {
        run_relay_pipeline_2();
    }
    else {
        run_relay_pipeline_3();
    }
}