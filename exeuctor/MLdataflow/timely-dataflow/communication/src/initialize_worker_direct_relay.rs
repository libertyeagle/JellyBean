//! Initialization logic for timely workers from a generic instance of the `Allocate` channel allocation trait.
//! with communication to relay nodes

use std::any::Any;
use std::sync::Arc;
use std::thread;

#[cfg(feature = "getopts")]
use getopts;
use serde::{Deserialize, Serialize};
use serde_json;

use logging_core::Logger;

use crate::allocator::AllocateBuilder;
use crate::allocator::direct_relay::{DirectRelayCommunicationEvent, DirectRelayCommunicationSetup};
use crate::allocator::direct_relay::{initialize_networking_with_relay, initialize_networking_with_relay_single_worker_process};
use crate::allocator::direct_relay::generic::{GenericDirectRelay, GenericDirectRelayBuilder};
use crate::logging::{CommunicationEvent, CommunicationSetup};
use crate::WorkerGuards;

#[derive(Serialize, Deserialize)]
struct JSONConfig {
    /// Address of peer workers
    peer_workers_addrs: Vec<String>,
    /// Number of threads per worker process in current pipeline
    threads_per_worker_process: usize,
    /// Address of the worker processes in each input pipeline
    input_pipeline_workers_addrs: Vec<Vec<String>>,
    /// Address of the worker processes in each output pipeline
    output_pipeline_workers_addrs: Vec<Vec<String>>,
    /// Number of worker threads in each worker process for each input pipeline
    num_input_pipelines_threads_per_process: Vec<usize>,
    /// Number of worker threads in each worker process for each output pipeline
    num_output_pipelines_threads_per_process: Vec<usize>,
}


/// Possible configurations for the communication infrastructure.
pub enum Config {
    /// Use one process with an indicated number of threads.
    Process {
        /// Number of worker threads in the current pipeline
        threads: usize,
        /// Address of the worker processes in each input pipeline
        input_pipeline_workers_addrs: Vec<Vec<String>>,
        /// Address of the worker processes in each output pipeline
        output_pipeline_workers_addrs: Vec<Vec<String>>,
        /// Number of worker threads in each worker process for each input pipeline
        num_input_pipelines_threads_per_process: Vec<usize>,
        /// Number of worker threads in each worker process for each output pipeline
        num_output_pipelines_threads_per_process: Vec<usize>,
        /// Current pipeline worker process addr
        my_addr: String,
        /// Verbosely report connection process
        report: bool,
        /// Closure to create a new logger for a communication (network) thread to relay nodes
        relay_log_fn: Box<dyn Fn(DirectRelayCommunicationSetup)->Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>> + Send + Sync>
    },
    /// Expect multiple processes.
    Cluster {
        /// Number of per-process worker threads
        threads: usize,
        /// Identity of this worker process
        process: usize,
        /// Addresses of all worker processes
        peer_worker_addresses: Vec<String>,
        /// Address of the worker processes in each input pipeline
        input_pipeline_workers_addrs: Vec<Vec<String>>,
        /// Address of the worker processes in each output pipeline
        output_pipeline_workers_addrs: Vec<Vec<String>>,
        /// Number of worker threads in each worker process for each input pipeline
        num_input_pipelines_threads_per_process: Vec<usize>,
        /// Number of worker threads in each worker process for each output pipeline
        num_output_pipelines_threads_per_process: Vec<usize>,
        /// Verbosely report connection process
        report: bool,
        /// Closure to create a new logger for a communication thread to other timely worker processes
        worker_log_fn: Box<dyn Fn(CommunicationSetup) -> Option<Logger<CommunicationEvent, CommunicationSetup>> + Send + Sync>,
        /// Closure to create a new logger for a communication (network) thread to relay nodes
        relay_log_fn: Box<dyn Fn(DirectRelayCommunicationSetup)->Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>> + Send + Sync>
    }
}

impl Config {
    /// Installs options into a [`getopts::Options`] struct that corresponds
    /// to the parameters in the configuration.
    #[cfg(feature = "getopts")]
    pub fn install_options(opts: &mut getopts::Options) {
        opts.optopt("p", "process", "identity of this worker process", "IDX");
        opts.reqopt("c", "config", "config ", "FILE");
        opts.optflag("r", "report", "reports connection progress");
    }

    /// Instantiates a configuration based upon the parsed options in `matches`.
    #[cfg(feature = "getopts")]
    pub fn from_matches(matches: &getopts::Matches) -> Result<Config, String> {
        // this worker process id
        let process = matches.opt_get_default("p", 0_usize).map_err(|e| e.to_string())?;
        let report = matches.opt_present("report");

        let config_filename = matches.opt_str("config").ok_or("no config file provided".to_owned())?;

        let file = ::std::fs::File::open(config_filename.clone()).map_err(|e| e.to_string())?;
        let reader = ::std::io::BufReader::new(file);
        let json_config = serde_json::from_reader::<_, JSONConfig>(reader).map_err(|e| e.to_string())?;

        let threads = json_config.threads_per_worker_process;
        let mut peer_worker_addrs = json_config.peer_workers_addrs;
        let input_pipeline_workers_addrs = json_config.input_pipeline_workers_addrs;
        let output_pipeline_workers_addrs = json_config.output_pipeline_workers_addrs;
        let num_input_pipelines_threads_per_process = json_config.num_input_pipelines_threads_per_process;
        let num_output_pipelines_threads_per_process = json_config.num_output_pipelines_threads_per_process;

        if peer_worker_addrs.len() > 1 {
            Ok(Config::Cluster {
                threads,
                process,
                peer_worker_addresses: peer_worker_addrs,
                input_pipeline_workers_addrs,
                output_pipeline_workers_addrs,
                num_input_pipelines_threads_per_process,
                num_output_pipelines_threads_per_process,
                report,
                worker_log_fn: Box::new(|_| None),
                relay_log_fn: Box::new(|_| None),
            })
        } else {
            Ok(Config::Process {
                threads,
                input_pipeline_workers_addrs,
                output_pipeline_workers_addrs,
                num_input_pipelines_threads_per_process,
                num_output_pipelines_threads_per_process,
                my_addr: peer_worker_addrs.pop().unwrap(),
                report,
                relay_log_fn: Box::new(|_| None),
            })
        }
    }
    /// Constructs a new configuration by parsing the supplied text arguments.
    #[cfg(feature = "getopts")]
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Result<Config, String> {
        let mut opts = getopts::Options::new();
        Config::install_options(&mut opts);
        let matches = opts.parse(args).map_err(|e| e.to_string())?;
        Config::from_matches(&matches)
    }

    /// Attempts to assemble the described communication infrastructure.
    pub fn try_build(self) -> Result<(Vec<GenericDirectRelayBuilder>, Box<dyn Any+Send>), String> {
        match self {
            Config::Process {threads, input_pipeline_workers_addrs, output_pipeline_workers_addrs, num_input_pipelines_threads_per_process, num_output_pipelines_threads_per_process, my_addr, report, relay_log_fn} => {
                match initialize_networking_with_relay_single_worker_process(
                    input_pipeline_workers_addrs,
                    output_pipeline_workers_addrs,
                    num_input_pipelines_threads_per_process,
                    num_output_pipelines_threads_per_process,
                    my_addr,
                    threads,
                    report,
                    relay_log_fn
                ) {
                    Ok((builders, guard)) => Ok((builders.into_iter().map(|x| GenericDirectRelayBuilder::Process(x)).collect(), Box::new(guard))),
                    Err(err) => Err(format!("failed to initialize networking: {}", err))
                }
            },
            Config::Cluster { threads, process, peer_worker_addresses, input_pipeline_workers_addrs, output_pipeline_workers_addrs, num_input_pipelines_threads_per_process, num_output_pipelines_threads_per_process, report, worker_log_fn, relay_log_fn } => {
                match initialize_networking_with_relay(
                    peer_worker_addresses,
                    input_pipeline_workers_addrs,
                    output_pipeline_workers_addrs,
                    num_input_pipelines_threads_per_process,
                    num_output_pipelines_threads_per_process,
                    process,
                    threads,
                    report,
                    worker_log_fn,
                    relay_log_fn
                ) {
                    Ok((stuff, guard)) => {
                        Ok((stuff.into_iter().map(|x| GenericDirectRelayBuilder::ZeroCopy(x)).collect(), Box::new(guard)))
                    },
                    Err(err) => Err(format!("failed to initialize networking: {}", err))
                }
            },
        }
    }
}
/// Initializes communication and executes a distributed computation.
pub fn initialize<T:Send+'static, F: Fn(GenericDirectRelay)->T+Send+Sync+'static>(
    config: Config,
    func: F,
) -> Result<WorkerGuards<T>,String> {
    let (allocators, others) = config.try_build()?;
    initialize_from(allocators, others, func)
}

/// Initializes computation and runs a distributed computation.
pub fn initialize_from<T, F>(
    builders: Vec<GenericDirectRelayBuilder>,
    others: Box<dyn Any+Send>,
    func: F,
) -> Result<WorkerGuards<T>,String>
    where
        T: Send+'static,
        F: Fn(GenericDirectRelay)->T+Send+Sync+'static
{
    let logic = Arc::new(func);
    let mut guards = Vec::new();
    for (index, builder) in builders.into_iter().enumerate() {
        // copy the atomic shared pointer to the closure
        let clone = logic.clone();
        // note that JoinHandle<T> also has a type T, when we call .join() on it
        // we can get the result the thread returns
        guards.push(thread::Builder::new()
            .name(format!("timely:work-{}", index))
            // builder is moved into the thread
            .spawn(move || {
                let communicator = builder.build();
                (*clone)(communicator)
            })
            .map_err(|e| format!("{:?}", e))?);
    }

    Ok(WorkerGuards { guards, others })
}
