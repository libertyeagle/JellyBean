extern crate timely_communication;

use std::ops::Deref;

use structopt::StructOpt;

use timely_communication::{Message, RelayConnectAllocate, WorkerWithRelayConfig};

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages relay communication - worker")]
pub struct Opts {
    /// Which pipeline, pipeline 1 or 2
    #[structopt(short, long)]
    pub pipeline: i32,
}

fn run_pipeline_1_worker() {
    let config = WorkerWithRelayConfig::Process {
        threads: 1,
        relay_addresses: vec!["127.0.0.1:6001".to_string()],
        report: true,
        relay_log_fn: Box::new(|_| None),
    };

    let guards = timely_communication::worker_initialize_with_relay(config, |mut allocator| {
        let (mut senders, _receiver) = allocator.allocate_channel_to_relay(0);
        std::mem::drop(_receiver);

        senders[0].send(Message::from_typed(format!("hello world from 1st pipeline!")));
        senders[0].done();

        println!("worker in 1st pipeline sent hello world message.");

        allocator.release_relay();
    });

    if let Ok(guards) = guards {
        guards.join();
        println!("worker in pipeline 1 completed");
    } else { println!("error in computation"); }
}

fn run_pipeline_2_worker() {
    let config = WorkerWithRelayConfig::Process {
        threads: 1,
        relay_addresses: vec!["127.0.0.1:6002".to_string()],
        report: true,
        relay_log_fn: Box::new(|_| None),
    };

    let guards = timely_communication::worker_initialize_with_relay(config, |mut allocator| {
        let (_senders, mut receiver) = allocator.allocate_channel_to_relay::<String>(0);
        std::mem::drop(_senders);

        loop {
            allocator.receive_from_relay();
            if let Some(message) = receiver.recv() {
                println!("worker node in 2nd pipeline received: {}", message.deref());
                println!("workflow checked!");
                break;
            }
        }
        allocator.release_relay();
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