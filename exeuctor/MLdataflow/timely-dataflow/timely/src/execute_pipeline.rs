//! Starts a timely dataflow execution from configuration information and per-worker logic.

use timely_communication::allocator::GenericToRelayBuilder;
use crate::communication::worker_initialize_with_relay_from as initialize_from;
use crate::communication::{AllocatorWithRelay, WorkerGuards};
use crate::worker::Worker;
use crate::{CommunicationWithRelayConfig, WorkerConfig};

/// Configures the execution of a timely dataflow computation.
pub struct Config {
    /// Configuration for the communication infrastructure.
    pub communication: CommunicationWithRelayConfig,
    /// Configuration for the worker threads.
    pub worker: WorkerConfig,
}

impl Config {
    /// Installs options into a [`getopts::Options`] struct that correspond
    /// to the parameters in the configuration.
    ///
    /// It is the caller's responsibility to ensure that the installed options
    /// do not conflict with any other options that may exist in `opts`, or
    /// that may be installed into `opts` in the future.
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn install_options(opts: &mut getopts_dep::Options) {
        CommunicationWithRelayConfig::install_options(opts);
        WorkerConfig::install_options(opts);
    }

    /// Instantiates a configuration based upon the parsed options in `matches`.
    ///
    /// The `matches` object must have been constructed from a
    /// [`getopts::Options`] which contained at least the options installed by
    /// [`Self::install_options`].
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn from_matches(matches: &getopts_dep::Matches) -> Result<Config, String> {
        Ok(Config {
            communication: CommunicationWithRelayConfig::from_matches(matches)?,
            worker: WorkerConfig::from_matches(matches)?,
        })
    }

    /// Constructs a new configuration by parsing the supplied text arguments.
    ///
    /// Most commonly, callers supply `std::env::args()` as the iterator.
    #[cfg(feature = "getopts")]
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Result<Config, String> {
        let mut opts = getopts_dep::Options::new();
        Config::install_options(&mut opts);
        let matches = opts.parse(args).map_err(|e| e.to_string())?;
        Config::from_matches(&matches)
    }

}

/// Executes a timely dataflow from a configuration and per-communicator logic.
pub fn execute<T, F>(
    mut config: Config,
    func: F
) -> Result<WorkerGuards<T>,String>
where
    T:Send+'static,
    F: Fn(&mut Worker<AllocatorWithRelay>)->T+Send+Sync+'static
{

    if let CommunicationWithRelayConfig::Cluster { ref mut worker_log_fn, .. } = config.communication {

        *worker_log_fn = Box::new(|events_setup| {

            let mut result = None;
            if let Ok(addr) = ::std::env::var("TIMELY_COMM_LOG_ADDR") {

                use ::std::net::TcpStream;
                use crate::logging::BatchLogger;
                use crate::dataflow::operators::capture::EventWriter;

                eprintln!("enabled COMM logging to {}", addr);

                if let Ok(stream) = TcpStream::connect(&addr) {
                    let writer = EventWriter::new(stream);
                    let mut logger = BatchLogger::new(writer);
                    result = Some(crate::logging_core::Logger::new(
                        ::std::time::Instant::now(),
                        ::std::time::Duration::default(),
                        events_setup,
                        move |time, data| logger.publish_batch(time, data)
                    ));
                }
                else {
                    panic!("Could not connect to communication log address: {:?}", addr);
                }
            }
            result
        });
    }

    let (allocators, other) = config.communication.try_build()?;

    let worker_config = config.worker;
    initialize_from(allocators, other, move |allocator| {

        let mut worker = Worker::new(worker_config.clone(), allocator);

        // If an environment variable is set, use it as the default timely logging.
        if let Ok(addr) = ::std::env::var("TIMELY_WORKER_LOG_ADDR") {

            use ::std::net::TcpStream;
            use crate::logging::{BatchLogger, TimelyEvent};
            use crate::dataflow::operators::capture::EventWriter;

            if let Ok(stream) = TcpStream::connect(&addr) {
                let writer = EventWriter::new(stream);
                let mut logger = BatchLogger::new(writer);
                worker.log_register()
                    .insert::<TimelyEvent,_>("timely", move |time, data|
                        logger.publish_batch(time, data)
                    );
            }
            else {
                panic!("Could not connect logging stream to: {:?}", addr);
            }
        }

        let result = func(&mut worker);
        while worker.pipeline_step_or_park(None) { }
        result
    })
}

/// Executes a timely dataflow from supplied arguments and per-communicator logic.
#[cfg(feature = "getopts")]
pub fn execute_from_args<I, T, F>(iter: I, func: F) -> Result<WorkerGuards<T>,String>
where I: Iterator<Item=String>,
      T:Send+'static,
      F: Fn(&mut Worker<AllocatorWithRelay>)->T+Send+Sync+'static
{
    let config = Config::from_args(iter)?;
    execute(config, func)
}

/// Executes a timely dataflow from supplied allocators and logging.
pub fn execute_from<T, F>(
    builders: Vec<GenericToRelayBuilder>,
    others: Box<dyn ::std::any::Any+Send>,
    worker_config: WorkerConfig,
    func: F,
) -> Result<WorkerGuards<T>, String>
where
    T: Send+'static,
    F: Fn(&mut Worker<AllocatorWithRelay>) -> T+Send+Sync+'static
{
    initialize_from(builders, others, move |allocator| {
        let mut worker = Worker::new(worker_config.clone(), allocator);
        let result = func(&mut worker);
        while worker.pipeline_step_or_park(None) { }
        result
    })
}
