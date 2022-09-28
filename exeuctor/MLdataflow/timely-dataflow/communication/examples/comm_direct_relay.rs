use std::ops::Deref;

use structopt::StructOpt;

use timely_communication::{DirectRelayAllocate, Message, WorkerDirectRelayConfig};

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages direct relay communication")]
pub struct Opts {
    /// Which pipeline, pipeline 1 or 2
    #[structopt(short, long)]
    pub pipeline: i32,
}

fn run_pipeline_1_worker() {
    let config = WorkerDirectRelayConfig::Process {
        threads: 1,
        input_pipeline_workers_addrs: vec![],
        output_pipeline_workers_addrs: vec![vec![String::from("127.0.0.1:6001")]],
        num_input_pipelines_threads_per_process: vec![],
        num_output_pipelines_threads_per_process: vec![1],
        report: true,
        relay_log_fn: Box::new(|_| None),
        my_addr: "127.0.0.1:5001".to_string()
    };

    let guards = timely_communication::worker_initialize_direct_relay(config, |mut allocator| {
        let mut senders = allocator.allocate_output_pipeline_pushers(0, 0);

        senders[0].send(Message::from_typed(format!("hello world from 1st pipeline!")));
        senders[0].done();

        println!("worker in 1st pipeline sent hello world message.");

        allocator.release_output_pipelines_send_all();
    });

    if let Ok(guards) = guards {
        guards.join();
        println!("worker in pipeline 1 completed");
    } else { println!("error in computation"); }
}

fn run_pipeline_2_worker() {
    let config = WorkerDirectRelayConfig::Process {
        threads: 1,
        input_pipeline_workers_addrs: vec![vec![String::from("127.0.0.1:5001")]],
        output_pipeline_workers_addrs: vec![],
        num_input_pipelines_threads_per_process: vec![1],
        num_output_pipelines_threads_per_process: vec![],
        report: true,
        relay_log_fn: Box::new(|_| None),
        my_addr: "127.0.0.1:6001".to_string()
    };

    let guards = timely_communication::worker_initialize_direct_relay(config, |mut allocator| {
        let mut receiver = allocator.allocate_input_pipeline_puller::<String>(0, 0);
        allocator.finish_input_pipeline_channel_allocation_all();

        loop {
            allocator.receive_from_all_input_pipelines();
            if let Some(message) = receiver.recv() {
                println!("worker node in 2nd pipeline received: {}", message.deref());
                println!("workflow checked!");
                break;
            }
        }
        allocator.release_output_pipelines_send_all();
    });

    if let Ok(guards) = guards {
        guards.join();
        println!("worker in pipeline 2 completed");
    } else { println!("error in computation"); }
}

fn main() {
    let opt = Opts::from_args();
    if opt.pipeline == 1 {
        run_pipeline_1_worker()
    } else {
        run_pipeline_2_worker()
    }
}