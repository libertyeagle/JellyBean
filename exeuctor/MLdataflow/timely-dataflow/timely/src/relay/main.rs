use std::collections::HashMap;
use std::path::PathBuf;

use serde_json;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use timely::relay::{execute_from_config, InputToWorkerExchangePattern, RelayToOutputExchangePattern};
use timely::relay::RelayConfig;
use timely::communication::RelayNodeConfig as RelayNodeCommConfig;


#[derive(Serialize, Deserialize)]
struct JSONConfig {
    /// Addresses of input pipeline relay nodes
    input_pipelines_relay_nodes: Vec<Vec<String>>,
    /// Addresses of output pipeline relay nodes
    output_pipelines_relay_nodes: Vec<Vec<String>>,
    /// Addresses of current pipeline relay nodes
    current_pipeline_relay_nodes: Vec<String>,
    /// Addresses of timely workers in current pipeline
    timely_workers: Vec<String>,
    /// Number of threads per timely worker process
    threads_per_timely_worker_process: usize,
    /// HashMap to map the input from the output's index of the input pipelines
    /// to the input index in the current pipeline
    /// One such HashMap for each of the input pipeline
    input_index_mapping: Vec<HashMap<usize, usize>>,
    /// The scope output (indices) required by the output pipelines
    /// Define the set of the required output indices for each of the output pipeline
    required_outputs: Vec<Vec<usize>>,
    /// Index of current pipeline relay node
    my_index: Option<usize>,
    /// Define the message exchange pattern (relay->worker) for each of the scope input
    input_to_worker_exchange_patterns: Option<HashMap<usize, InputToWorkerExchangePattern>>,
    /// Define how should the output messages be sent to the relay nodes in output pipelines
    relay_to_output_exchange_pattern: Option<RelayToOutputExchangePattern>
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "timely relay")]
struct Opts {
    /// Current relay node index
    #[structopt(short, long)]
    pub my_index: Option<usize>,

    /// Path to JSON config file
    #[structopt(short, long)]
    pub config: PathBuf
}

fn main() {
    let opts = Opts::from_args();

    let file = std::fs::File::open(opts.config).expect("failed to open config file");
    let reader = ::std::io::BufReader::new(file);
    let json_config = serde_json::from_reader::<_, JSONConfig>(reader).expect("failed to parse config");

    assert!(
        opts.my_index.is_some() || json_config.my_index.is_some(),
        "Current relay node index must be provided in either command-line argument, or config file."
    );

    let index = match opts.my_index {
      Some(index) => index,
      None => json_config.my_index.unwrap()
    };
    let num_relay_peers = json_config.current_pipeline_relay_nodes.len();
    let my_addr = json_config.current_pipeline_relay_nodes.get(index).unwrap().clone();

    let comm_config = RelayNodeCommConfig {
        input_relay_nodes_addresses: json_config.input_pipelines_relay_nodes,
        output_relay_nodes_addresses: json_config.output_pipelines_relay_nodes,
        timely_workers_addresses: json_config.timely_workers,
        threads_per_timely_worker_process: json_config.threads_per_timely_worker_process,
        my_addr,
        my_index: index,
        num_relay_nodes_peers: num_relay_peers,
        report: true,
        relay_log_sender: Box::new(|_| None),
        timely_log_sender: Box::new(|_| None)
    };

    let relay_config = RelayConfig {
        comm_config,
        input_index_mapping: json_config.input_index_mapping,
        required_outputs: json_config.required_outputs,
        input_to_worker_exchange_patterns: json_config.input_to_worker_exchange_patterns,
        relay_to_output_exchange_pattern: json_config.relay_to_output_exchange_pattern,
        output_pipelines_relay_load_balance_ratios: HashMap::new()
    };

    execute_from_config(relay_config);
}