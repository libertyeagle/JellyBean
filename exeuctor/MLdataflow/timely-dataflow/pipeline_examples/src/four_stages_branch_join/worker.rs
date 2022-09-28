use structopt::StructOpt;

use timely::CommunicationWithRelayConfig;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Inspect, Probe, Map, Exchange};
use timely::execute_pipeline::{Config, execute};
use timely::WorkerConfig;


#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Four-pipelines branch-join timely dataflow worker")]
pub struct Opts {
    /// Worker process index (for pipeline 0 and pipeline 3)
    #[structopt(short, long, required_if("pipeline", "0"), required_if("pipeline", "3"))]
    pub worker: Option<usize>,
    /// Which pipeline, pipeline 0/1/2/3
    #[structopt(short, long)]
    pub pipeline: i32
}

fn run_pipeline_0_worker(process_index: usize) {
    assert!(process_index == 0 || process_index == 1, "worker process index must be either 0 or 1");
    let worker_config = Config {
        communication: CommunicationWithRelayConfig::Cluster {
            threads: 2,
            process: process_index,
            worker_addresses: vec![String::from("127.0.0.1:5000"), String::from("127.0.0.1:5001")],
            relay_addresses: vec![String::from("127.0.0.1:6000")],
            report: true,
            worker_log_fn: Box::new(|_| None),
            relay_log_fn: Box::new(|_| None)
        },
        worker: WorkerConfig::default()
    };
    execute(worker_config, |worker| {
        let worker_index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.pipeline_dataflow(|scope| {
            let stream = scope.input_from(&mut input)
                .inspect(move |x| println!("worker {} in pipeline 1: {} generated", worker_index, x))
                .probe_with(&mut probe);
            let stream_out_1 = stream.map(|x| x + 1);
            let stream_out_2 = stream.map(|x| String::from(format!("hello with {}", x)));
            scope.register_pipeline_output(&stream_out_1, 0, |x| *x as u64);
            scope.register_pipeline_output(&stream_out_2, 1, |_x| 0);
        });

        for i in 0..20 {
            if i % 4 == worker_index {
                input.advance_to(i as usize);
                input.send(i);
            }
            while probe.less_than(input.time()) {
                worker.pipeline_step();
            }
        }
    }).unwrap();
}

fn run_pipeline_1_worker() {
    let worker_config = Config {
        communication: CommunicationWithRelayConfig::Process {
            threads: 2,
            relay_addresses: vec![String::from("127.0.0.1:6100")],
            report: true,
            relay_log_fn: Box::new(|_| None)
        },
        worker: WorkerConfig::default()
    };
    execute(worker_config, |worker| {
        let worker_index = worker.index();
        worker.pipeline_dataflow::<usize, _, _>(|scope| {
            let stream = scope.acquire_pipeline_input::<i32>(1);
            let stream_2 = scope.acquire_pipeline_input::<String>(0);
            stream_2.inspect(move |x| println!("worker {} in pipeline 1 inspected: {}", worker_index, x));
            let stream = stream.inspect(move |x| println!("worker {} in pipeline 1 recv: {}", worker_index, x))
                .map(|x| x * 2);
            scope.register_pipeline_output_random_exchange(&stream, 0);
        });
    }).unwrap();
}

fn run_pipeline_2_worker() {
    let worker_config = Config {
        communication: CommunicationWithRelayConfig::Process {
            threads: 1,
            relay_addresses: vec![String::from("127.0.0.1:6200")],
            report: true,
            relay_log_fn: Box::new(|_| None)
        },
        worker: WorkerConfig::default()
    };
    execute(worker_config, |worker| {
        worker.pipeline_dataflow::<usize, _, _>(|scope| {
            let stream = scope.acquire_pipeline_input::<String>(0);
            let stream = stream.inspect(|x| println!("worker in pipeline 2 recv: {}", x))
                .map(|x| x + " [stamped by pipeline 2]");
            scope.register_pipeline_output_random_exchange(&stream, 0);
        });
    }).unwrap();
}

fn run_pipeline_3_worker(process_index: usize) {
    let worker_config = Config {
        communication: CommunicationWithRelayConfig::Cluster {
            threads: 1,
            process: process_index,
            worker_addresses: vec![String::from("127.0.0.1:5300"), String::from("127.0.0.1:5301")],
            relay_addresses: vec![String::from("127.0.0.1:6300")],
            report: true,
            worker_log_fn: Box::new(|_| None),
            relay_log_fn: Box::new(|_| None)
        },
        worker: WorkerConfig::default()
    };
    execute(worker_config, |worker| {
        let worker_index = worker.index();
        worker.pipeline_dataflow::<usize, _, _>(|scope| {
            let stream_in_0 = scope.acquire_pipeline_input::<String>(0);
            let stream_in_1 = scope.acquire_pipeline_input::<i32>(1);
            let stream_in_0 = stream_in_0.exchange(|_| 0);
            let stream_in_1 = stream_in_1.exchange(|_| 0);
            stream_in_0.inspect(move |x| println!("worker {} in pipeline 3 recv: {}", worker_index, x));
            stream_in_1.inspect(move |x| println!("worker {} in pipeline 3 recv: {}", worker_index, x));
        });
    }).unwrap();
}

fn main() {
    let opt = Opts::from_args();
    assert!(opt.pipeline >= 0 && opt.pipeline <= 3, "pipeline must be 0/1/2/3");
    if opt.pipeline == 0 {
        let process = opt.worker.unwrap();
        run_pipeline_0_worker(process);
    }
    else if opt.pipeline == 1 {
        run_pipeline_1_worker();
    }
    else if opt.pipeline == 2 {
        run_pipeline_2_worker();
    }
    else {
        let process = opt.worker.unwrap();
        run_pipeline_3_worker(process);
    }
}