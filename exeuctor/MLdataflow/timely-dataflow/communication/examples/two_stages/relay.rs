extern crate timely_communication;

use std::ops::Deref;

use structopt::StructOpt;

use timely_communication::{Push, RelayNodeConfig};
use timely_communication::allocator::relay::{InputRelayAllocate, OutputRelayAllocate};

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "Simple two stages relay communication - worker")]
pub struct Opts {
    /// Which pipeline, pipeline 1 or 2
    #[structopt(short, long)]
    pub pipeline: i32,
}

fn run_pipeline_1_relay() {
    let config = RelayNodeConfig {
        input_relay_nodes_addresses: vec![],
        output_relay_nodes_addresses: vec![vec!["127.0.0.1:6002".to_string()]],
        timely_workers_addresses: vec!["127.0.0.1:5000".to_string()],
        threads_per_timely_worker_process: 1,
        my_addr: "127.0.0.1:6001".to_string(),
        my_index: 0,
        num_relay_nodes_peers: 1,
        report: true,
        relay_log_sender: Box::new(|_| None),
        timely_log_sender: Box::new(|_| None),
    };

    let guards = timely_communication::relay_initialize_with_output_only(config, |mut allocator| {
        let pipeline_index = allocator.pipeline_index();
        assert_eq!(pipeline_index, 0);

        let mut receiver = allocator.allocate_timely_workers_bytes_puller(0);
        let mut pushers = allocator.allocate_output_pipeline_bytes_pushers(0);
        loop {
            allocator.receive_from_timely_workers();
            if let Some(message) = receiver.recv() {
                println!("relay node in 1st pipeline received raw bytes: {:?}", message.deref());
                pushers[0].send(message);
                pushers[0].done();
                println!("relay node in 1st pipeline sent message");
                break;
            }
        }
        allocator.release();
    });

    if let Ok(guards) = guards {
        guards.join();
        println!("relay node in pipeline 1 completed");
    } else { println!("error in computation"); }
}

fn run_pipeline_2_relay() {
    let config = RelayNodeConfig {
        input_relay_nodes_addresses: vec![vec!["127.0.0.1:6001".to_string()]],
        output_relay_nodes_addresses: vec![],
        timely_workers_addresses: vec!["127.0.0.1:5001".to_string()],
        threads_per_timely_worker_process: 1,
        my_addr: "127.0.0.1:6002".to_string(),
        my_index: 0,
        num_relay_nodes_peers: 1,
        report: true,
        relay_log_sender: Box::new(|_| None),
        timely_log_sender: Box::new(|_| None),
    };

    let guards = timely_communication::relay_initialize_with_input_only(config, |mut allocator| {
        let pipeline_index = allocator.pipeline_index();
        assert_eq!(pipeline_index, 0);
        let mut receiver = allocator.allocate_input_pipeline_bytes_puller(0);
        let mut pushers = allocator.allocate_timely_workers_bytes_pushers(0);
        loop {
            allocator.receive_pipeline_input();
            if let Some(message) = receiver.recv() {
                println!("relay node in 2nd pipeline received raw bytes: {:?}", message.deref());
                pushers[0].send(message);
                pushers[0].done();
                println!("relay node in 2nd pipeline sent message");
                break;
            }
        }
        allocator.release();
    });

    if let Ok(guards) = guards {
        guards.join();
        println!("relay node in pipeline 2 completed");
    } else { println!("error in computation"); }
}

fn main() {
    let opt = Opts::from_args();
    if opt.pipeline == 1 {
        run_pipeline_1_relay()
    } else {
        run_pipeline_2_relay()
    }
}