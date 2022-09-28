use structopt::StructOpt;

use timely::CommunicationWithRelayConfig::Process;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Inspect, Map, Probe};
use timely::execute_pipeline::{Config, execute};
use timely::WorkerConfig;

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages pipeline timely dataflow worker")]
pub struct Opts {
    /// Which pipeline, pipeline 0/1/2
    #[structopt(short, long)]
    pub pipeline: i32,
}

fn run_pipeline_0_worker() {
    let worker_config = Config {
        communication: Process {
            threads: 1,
            relay_addresses: vec![String::from("127.0.0.1:6001")],
            report: true,
            relay_log_fn: Box::new(|_| None),
        },
        worker: WorkerConfig::default(),
    };
    execute(worker_config, |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.pipeline_dataflow(|scope| {
            let stream_out = scope.input_from(&mut input)
                .inspect(move |x| println!("worker in pipeline 0: {} generated", x))
                .probe_with(&mut probe);
            scope.register_pipeline_output_balanced_exchange(&stream_out, 0);
        });

        for round in 0..5 {
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
            relay_addresses: vec![String::from("127.0.0.1:6002")],
            report: true,
            relay_log_fn: Box::new(|_| None),
        },
        worker: WorkerConfig::default(),
    };
    execute(worker_config, |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.pipeline_dataflow(|scope| {
            let stream_out = scope.input_from(&mut input)
                .inspect(move |x| println!("worker in pipeline 0: {} generated", x))
                .probe_with(&mut probe);
            scope.register_pipeline_output_balanced_exchange(&stream_out, 0);
        });

        for round in 5..10 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.pipeline_step();
            }
        }
    }).unwrap();
}


fn run_pipeline_2_worker() {
    let worker_config = Config {
        communication: Process {
            threads: 1,
            relay_addresses: vec![String::from("127.0.0.1:6003")],
            report: true,
            relay_log_fn: Box::new(|_| None),
        },
        worker: WorkerConfig::default(),
    };
    execute(worker_config, |worker| {
        worker.pipeline_dataflow::<i32, _, _>(|scope| {
            let stream_in = scope.acquire_pipeline_input::<i32>(0);
            stream_in.map(|x| x + 1).inspect(
                |x| println!("worker in pipeline 1 see: {}", x)
            );
        });
    }).unwrap();
}

fn main() {
    let opt = Opts::from_args();
    if opt.pipeline == 0 {
        run_pipeline_0_worker()
    } else if opt.pipeline == 1 {
        run_pipeline_1_worker()
    } else {
        run_pipeline_2_worker()
    }
}