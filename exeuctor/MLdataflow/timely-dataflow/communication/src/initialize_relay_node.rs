//! Initialization logic for relay node, from closures that takes in InputRelayWorkerAllocator and OutputRelayWorkerAllocator

use std::any::Any;
use std::sync::Arc;
use std::thread;

#[cfg(feature = "getopts")]
use getopts;
use serde::{Deserialize, Serialize};
use serde_json;

use logging_core::Logger;

use crate::allocator::relay::{relay_initialize_networking, RelayCommunicationEvent, RelayCommunicationSetup, RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup};
use crate::allocator::relay::relay_allocator::{InputRelayWorkerAllocator, InputRelayWorkerBuilder, OutputRelayWorkerAllocator, OutputRelayWorkerBuilder};

#[derive(Serialize, Deserialize)]
struct JSONConfig {
    input_pipelines_relay_nodes: Vec<Vec<String>>,
    output_pipelines_relay_nodes: Vec<Vec<String>>,
    timely_workers: Vec<String>,
    threads_per_timely_worker_process: usize,
    current_pipeline_relay_nodes: Vec<String>
}

/// Configuration for the relay node infrastructure.
pub struct Config {
    /// Addresses of input pipeline relay nodes
    pub input_relay_nodes_addresses: Vec<Vec<String>>,
    /// Addresses of output pipeline relay nodes
    pub output_relay_nodes_addresses: Vec<Vec<String>>,
    /// Addresses of timely workers in current pipeline
    pub timely_workers_addresses: Vec<String>,
    /// Number of threads per timely worker process
    pub threads_per_timely_worker_process: usize,
    /// Address of current relay node
    pub my_addr: String,
    /// Index of current relay node
    pub my_index: usize,
    /// Number of peer relay nodes in current pipeline
    pub num_relay_nodes_peers: usize,
    /// Verbosely report connection process
    pub report: bool,
    /// Closure to create a new logger for a communication (network) thread to relay nodes input/output pipelines
    pub relay_log_sender: Box<dyn Fn(RelayCommunicationSetup)->Option<Logger<RelayCommunicationEvent, RelayCommunicationSetup>> + Send + Sync>,
    /// Closure to create a new logger for a communication (network) thread to timely worker processes
    pub timely_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>> + Send + Sync>
}

impl Config {
    /// Installs options into a [`getopts::Options`] struct that corresponds
    /// to the parameters in the configuration.
    #[cfg(feature = "getopts")]
    pub fn install_options(opts: &mut getopts::Options) {
        opts.optopt("i", "index", "index of this relay node", "IDX");
        opts.reqopt("c", "config", "config of the relay", "FILE");
        opts.optflag("r", "report", "reports connection progress");
    }

    /// Instantiates a configuration based upon the parsed options in `matches`.
    #[cfg(feature = "getopts")]
    pub fn from_matches(matches: &getopts::Matches) -> Result<Config, String> {
        let index = matches.opt_get_default("index", 0_usize).map_err(|e| e.to_string())?;
        let report = matches.opt_present("report");

        let config_filename = matches.opt_str("config").ok_or("no config file provided".to_owned())?;
        let file = ::std::fs::File::open(config_filename.clone()).map_err(|e| e.to_string())?;
        let reader = ::std::io::BufReader::new(file);
        let mut json_config = serde_json::from_reader::<_, JSONConfig>(reader).map_err(|e| e.to_string())?;

        let num_relays = json_config.current_pipeline_relay_nodes.len();
        let my_addr = json_config.current_pipeline_relay_nodes.swap_remove(index);

        Ok(Config {
            input_relay_nodes_addresses: json_config.input_pipelines_relay_nodes,
            output_relay_nodes_addresses: json_config.output_pipelines_relay_nodes,
            timely_workers_addresses: json_config.timely_workers,
            threads_per_timely_worker_process: json_config.threads_per_timely_worker_process,
            my_addr,
            my_index: index,
            num_relay_nodes_peers: num_relays,
            report,
            relay_log_sender: Box::new(|_| None),
            timely_log_sender: Box::new(|_| None)
        })
    }

    /// Constructs a new configuration by parsing the supplied text arguments.
    /// Most commonly, callers supply `std::env::args()` as the iterator.
    #[cfg(feature = "getopts")]
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Result<Config, String> {
        let mut opts = getopts::Options::new();
        Config::install_options(&mut opts);
        let matches = opts.parse(args).map_err(|e| e.to_string())?;
        Config::from_matches(&matches)
    }

    /// Attempts to assemble the described relay node communication infrastructure.
    pub fn try_build(self) -> Result<(Vec<InputRelayWorkerBuilder>, Vec<OutputRelayWorkerBuilder>, Box<dyn Any+Send>), String> {
        let (input_builders, output_builders, comm_guard) = relay_initialize_networking(
            self.input_relay_nodes_addresses,
            self.output_relay_nodes_addresses,
            self.timely_workers_addresses,
            self.my_addr,
            self.my_index,
            self.num_relay_nodes_peers,
            self.threads_per_timely_worker_process,
            self.report,
            self.relay_log_sender,
            self.timely_log_sender
        ).map_err(|err| format!("failed to init networking: {}", err))?;
        Ok((input_builders, output_builders, Box::new(comm_guard)))
    }
}

/// Initialize communication and spin up input/output relay workers
/// TODO: add more doc comments
pub fn initialize_with_input_output<T1, T2, F1, F2>(
    config: Config,
    func_input_pipeline: F1,
    func_output_pipeline: F2
) -> Result<WorkerGuards<T1, T2>, String>
where
    T1: Send+'static,
    T2: Send+'static,
    F1: Fn(InputRelayWorkerAllocator) -> T1+Send+Sync+'static,
    F2: Fn(OutputRelayWorkerAllocator) -> T2+Send+Sync+'static
{
    let (input_relay_builders, output_relay_builders, comm_guards) = config.try_build()?;
    initialize_with_input_output_from(
        input_relay_builders,
        output_relay_builders,
        comm_guards,
        func_input_pipeline,
        func_output_pipeline
    )
}

/// Initialize communication and spin up input relay workers
/// TODO: add more doc comments
pub fn initialize_with_input_only<T, F>(
    config: Config,
    func: F,
) -> Result<WorkerGuards<T, T>, String>
where
    T: Send+'static,
    F: Fn(InputRelayWorkerAllocator) -> T+Send+Sync+'static,
{
    let (input_relay_builders, _output_relay_builders, comm_guards) = config.try_build()?;
    initialize_with_input_only_from(
        input_relay_builders,
        comm_guards,
        func
    )
}

/// Initialize communication and spin up output relay workers
/// TODO: add more doc comments
pub fn initialize_with_output_only<T, F>(
    config: Config,
    func: F,
) -> Result<WorkerGuards<T, T>, String>
where
    T: Send+'static,
    F: Fn(OutputRelayWorkerAllocator) -> T+Send+Sync+'static,
{
    let (_input_relay_builders, output_relay_builders, comm_guards) = config.try_build()?;
    initialize_with_output_only_from(
        output_relay_builders,
        comm_guards,
        func
    )
}

fn initialize_with_input_output_from<T1, T2, F1, F2>(
    input_relay_worker_builders: Vec<InputRelayWorkerBuilder>,
    output_relay_worker_builders: Vec<OutputRelayWorkerBuilder>,
    others: Box<dyn Any+Send>,
    func_input_pipeline: F1,
    func_output_pipeline: F2
) -> Result<WorkerGuards<T1, T2>, String>
where
    T1: Send+'static,
    T2: Send+'static,
    F1: Fn(InputRelayWorkerAllocator) -> T1+Send+Sync+'static,
    F2: Fn(OutputRelayWorkerAllocator) -> T2+Send+Sync+'static
{
    let logic = Arc::new(func_input_pipeline);
    let mut input_relay_guards = Vec::new();
    for (index, builder) in input_relay_worker_builders.into_iter().enumerate() {
        let clone = logic.clone();
        input_relay_guards.push(thread::Builder::new()
            .name(format!("relay:input-pipeline-{}", index))
            // builder is moved into the thread
            .spawn(move || {
                let communicator = builder.build();
                (*clone)(communicator)
            })
            .map_err(|e| format!("{:?}", e))?);
    }

    let logic = Arc::new(func_output_pipeline);
    let mut output_relay_guards = Vec::new();
    for (index, builder) in output_relay_worker_builders.into_iter().enumerate() {
        let clone = logic.clone();
        output_relay_guards.push(thread::Builder::new()
            .name(format!("relay:output-pipeline-{}", index))
            // builder is moved into the thread
            .spawn(move || {
                let communicator = builder.build();
                (*clone)(communicator)
            })
            .map_err(|e| format!("{:?}", e))?);
    }

    Ok(WorkerGuards { input_relay_guards, output_relay_guards, others })
}

fn initialize_with_input_only_from<T, F>(
    input_relay_worker_builders: Vec<InputRelayWorkerBuilder>,
    others: Box<dyn Any+Send>,
    func: F,
) -> Result<WorkerGuards<T, T>, String>
where
    T: Send+'static,
    F: Fn(InputRelayWorkerAllocator) -> T+Send+Sync+'static,
{
    let logic = Arc::new(func);
    let mut input_relay_guards = Vec::new();
    for (index, builder) in input_relay_worker_builders.into_iter().enumerate() {
        let clone = logic.clone();
        input_relay_guards.push(thread::Builder::new()
            .name(format!("relay:input-pipeline-{}", index))
            .spawn(move || {
                let communicator = builder.build();
                (*clone)(communicator)
            })
            .map_err(|e| format!("{:?}", e))?);
    }

    Ok(WorkerGuards { input_relay_guards, output_relay_guards: Vec::new(), others })
}

fn initialize_with_output_only_from<T, F>(
    output_relay_worker_builders: Vec<OutputRelayWorkerBuilder>,
    others: Box<dyn Any+Send>,
    func: F
) -> Result<WorkerGuards<T, T>, String>
where
    T: Send+'static,
    F: Fn(OutputRelayWorkerAllocator) -> T+Send+Sync+'static
{
    let logic = Arc::new(func);
    let mut output_relay_guards = Vec::new();
    for (index, builder) in output_relay_worker_builders.into_iter().enumerate() {
        let clone = logic.clone();
        output_relay_guards.push(thread::Builder::new()
            .name(format!("relay:output-pipeline-{}", index))
            .spawn(move || {
                let communicator = builder.build();
                (*clone)(communicator)
            })
            .map_err(|e| format!("{:?}", e))?);
    }

    Ok(WorkerGuards { input_relay_guards: Vec::new(), output_relay_guards, others })
}

/// Maintains join handles for input relay workers and output relay workers,
/// while accommodate comm guards (guards for network threads)
pub struct WorkerGuards<T1: Send+'static, T2: Send+'static> {
    input_relay_guards: Vec<thread::JoinHandle<T1>>,
    output_relay_guards: Vec<thread::JoinHandle<T2>>,
    others: Box<dyn Any+Send>,
}

impl<T1: Send+'static, T2: Send+'static> WorkerGuards<T1, T2> {
    /// Returns a reference to the indexed guard.
    pub fn guards(&self) -> (&[std::thread::JoinHandle<T1>], &[std::thread::JoinHandle<T2>])  {
        (&self.input_relay_guards[..], &self.output_relay_guards)
    }
    /// Provides access to handles that are not worker threads.
    pub fn others(&self) -> &Box<dyn Any+Send> {
        &self.others
    }
    /// Waits on the worker threads and returns the results they produce.
    pub fn join(mut self) -> (Vec<Result<T1, String>>, Vec<Result<T2, String>>) {
        let input_relay_results = self.input_relay_guards
            .drain(..)
            // join returns type Result<T> = crate::result::Result<T, Box<dyn Any + Send + 'static>>;
            .map(|guard| guard.join().map_err(|e| format!("{:?}", e)))
            .collect();
        let output_relay_results = self.output_relay_guards
            .drain(..)
            .map(|guard| guard.join().map_err(|e| format!("{:?}", e)))
            .collect();
        (input_relay_results, output_relay_results)
    }
}

impl<T1: Send+'static, T2: Send+'static> Drop for WorkerGuards<T1, T2> {
    fn drop(&mut self) {
        for guard in self.input_relay_guards.drain(..) {
            guard.join().expect("input relay worker panic");
        }
        for guard in self.output_relay_guards.drain(..) {
            guard.join().expect("output relay worker panic");
        }
    }
}
