use structopt::StructOpt;

use timely::CommunicationWithRelayConfig::Process;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Inspect, Probe, Map};
use timely::execute_pipeline::{Config, execute};
use timely::WorkerConfig;


#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages pipeline timely dataflow worker")]
pub struct Opts {
    /// Which pipeline, pipeline 0 or 1
    #[structopt(short, long)]
    pub pipeline: i32
}

fn run_pipeline_0_worker() {
    let worker_config = Config {
        communication: Process {
            threads: 1,
            relay_addresses: vec![String::from("127.0.0.1:6001"), String::from("127.0.0.1:6002")],
            report: true,
            relay_log_fn: Box::new(|_| None)
        },
        worker: WorkerConfig::default()
    };
    execute(worker_config, |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.pipeline_dataflow(|scope| {
            let stream = scope.input_from(&mut input)
                .inspect(move |x| println!("worker in pipeline 0: {} generated", x))
                .probe_with(&mut probe);
            let stream_out_1 = stream.map(|x| x * 2);
            let stream_out_2 = stream.map(|x| String::from(format!("{}-th hello", x)));
            scope.register_pipeline_output(&stream_out_1, 0, |x| *x as u64);
            scope.register_pipeline_output_random_exchange(&stream_out_2, 1);
        });

        for round in 0..10 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.pipeline_step();
            }
        }
    }).unwrap();
}

fn run_pipeline_1_worker() {
    let worker_config = Config {
        communication: Process {
            threads: 1,
            relay_addresses: vec![String::from("127.0.0.1:6003"), String::from("127.0.0.1:6004")],
            report: true,
            relay_log_fn: Box::new(|_| None)
        },
        worker: WorkerConfig::default()
    };
    execute(worker_config, |worker| {
        worker.pipeline_dataflow::<i32, _, _>(|scope| {
            let stream_in_1 = scope.acquire_pipeline_input::<i32>(0);
            let stream_in_2 = scope.acquire_pipeline_input::<String>(1);
            stream_in_1.map(|x| x + 1).inspect(
                |x| println! ("worker in pipeline 1 see: {}", x)
            );
            stream_in_2.inspect(
                |x| println! ("worker in pipeline 1 receive: {}", x)
            );
        });
    }).unwrap();
}

fn main() {
    let opt = Opts::from_args();
    if opt.pipeline == 0 {
        run_pipeline_0_worker()
    }
    else {
        run_pipeline_1_worker()
    }
}