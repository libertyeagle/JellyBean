//! initialize networks of timely workers with communication to relay nodes
use std::net::TcpStream;
use std::sync::Arc;
use logging_core::Logger;
use crate::allocator::process::ProcessBuilder;
use crate::allocator::relay::logging::{RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup};
use crate::allocator::relay::timely_network_utlis::create_sockets_to_relay;
use crate::allocator::relay::timely_tcp::{recv_loop_from_relay, send_loop_to_relay};
use crate::allocator::zero_copy::allocator::{new_vector_with_relay_connection, TcpBuilder};
use crate::allocator::zero_copy::initialize::CommsGuard;
use crate::allocator::zero_copy::tcp::{recv_loop, send_loop};
use crate::logging::{CommunicationEvent, CommunicationSetup};
use crate::networking::create_sockets;


/// Initialize timely worker's network connections
/// create sockets to both timely workers and relay nodes
/// return builders and join guards
pub fn initialize_networking_to_relay(
    worker_addresses: Vec<String>,
    relay_addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    worker_log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>,
    relay_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    let sockets_to_workers = create_sockets(worker_addresses, my_index, noisy)?;
    let sockets_to_relay = create_sockets_to_relay(relay_addresses, my_index, noisy)?;
    initialize_networking_to_relay_from_sockets(
        sockets_to_workers,
        sockets_to_relay,
        my_index,
        threads,
        worker_log_sender,
        relay_log_sender
    )
}

fn initialize_networking_to_relay_from_sockets(
    // sockets to other workers
    sockets_to_workers: Vec<Option<TcpStream>>,
    // sockets to the relay nodes
    sockets_to_relay: Vec<TcpStream>,
    // index of current worker process
    my_index: usize,
    // num worker threads per worker process
    threads: usize,
    worker_log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>,
    relay_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    // we need to use an Arc pointer to wrap the log_sender to share between network threads
    let worker_log_sender = Arc::new(worker_log_sender);
    let relay_log_sender = Arc::new(relay_log_sender);
    let processes = sockets_to_workers.len();
    let num_relay_nodes = sockets_to_relay.len();

    let process_allocators = crate::allocator::process::Process::new_vector(threads);
    let (builders, promises, futures, relay_promises, relay_futures) = new_vector_with_relay_connection(
        process_allocators,
        num_relay_nodes,
        my_index,
        processes
    );

    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    let mut send_guards = Vec::with_capacity(sockets_to_workers.len() + sockets_to_relay.len());
    let mut recv_guards = Vec::with_capacity(sockets_to_workers.len() + sockets_to_relay.len());

    for (index, stream) in sockets_to_workers.into_iter().enumerate().filter_map(|(i, s)| s.map(|s| (i, s))) {
        let remote_recv = promises_iter.next().unwrap();
        {
            let log_sender = worker_log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("timely:send-{}", index))
                    .spawn(move || {
                        let logger = log_sender(CommunicationSetup {
                            process: my_index,
                            sender: true,
                            remote: Some(index),
                        });
                        send_loop(stream, remote_recv, my_index, index, logger);
                    })?;

            send_guards.push(join_guard);
        }

        let remote_send = futures_iter.next().unwrap();
        {
            // let remote_sends = remote_sends.clone();
            let log_sender = worker_log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("timely:recv-{}", index))
                    .spawn(move || {
                        let logger = log_sender(CommunicationSetup {
                            process: my_index,
                            sender: false,
                            remote: Some(index),
                        });
                        recv_loop(stream, remote_send, threads * my_index, my_index, index, logger);
                    })?;

            recv_guards.push(join_guard);
        }
    }


    let mut promises_iter = relay_promises.into_iter();
    let mut futures_iter = relay_futures.into_iter();

    for (index, stream) in sockets_to_relay.into_iter().enumerate() {
        let remote_recv = promises_iter.next().unwrap();
        {
            let log_sender = relay_log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("timely-to-relay:send-{}", index))
                    .spawn(move || {
                        let logger = log_sender(RelayTimelyCommunicationSetup {
                            sender: true,
                            relay_node_index: index,
                            timely_worker_process_index: my_index,
                        });
                        send_loop_to_relay(stream, remote_recv, my_index, index, logger);
                    })?;

            send_guards.push(join_guard);
        }

        let remote_send = futures_iter.next().unwrap();
        {
            let log_sender = relay_log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("timely-to-relay:recv-{}", index))
                    .spawn(move || {
                        let logger = log_sender(RelayTimelyCommunicationSetup {
                            sender: false,
                            relay_node_index: index,
                            timely_worker_process_index: my_index,
                        });
                        recv_loop_from_relay(stream, remote_send, threads * my_index, my_index, index, logger);
                    })?;

            recv_guards.push(join_guard);
        }
    }

    // returns #threads/process TcpBuilders
    Ok((builders, CommsGuard { send_guards, recv_guards }))
}


/// Initialize timely worker's network connections
/// for a single process allocator (Process)
pub fn initialize_networking_to_relay_single_worker_process(
    relay_addresses: Vec<String>,
    num_worker_threads: usize,
    noisy: bool,
    relay_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>+Send+Sync>)
    -> ::std::io::Result<(Vec<ProcessBuilder>, CommsGuard)>
{
    let sockets_to_relay = create_sockets_to_relay(relay_addresses, 0, noisy)?;
    initialize_networking_to_relay_from_sockets_single_worker_process(
        sockets_to_relay,
        num_worker_threads,
        relay_log_sender
    )
}

fn initialize_networking_to_relay_from_sockets_single_worker_process(
    // sockets to the relay nodes
    sockets_to_relay: Vec<TcpStream>,
    worker_threads: usize,
    // log sender
    relay_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>+Send+Sync>)
    -> ::std::io::Result<(Vec<ProcessBuilder>, CommsGuard)>
{
    let relay_log_sender = Arc::new(relay_log_sender);
    let num_relay_nodes = sockets_to_relay.len();

    let (builders, promises, futures)  = crate::allocator::process::Process::new_vector_with_relay_connection(worker_threads, num_relay_nodes);

    let mut send_guards = Vec::with_capacity(sockets_to_relay.len());
    let mut recv_guards = Vec::with_capacity(sockets_to_relay.len());


    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    for (index, stream) in sockets_to_relay.into_iter().enumerate() {
        let remote_recv = promises_iter.next().unwrap();
        {
            let log_sender = relay_log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("timely-to-relay:send-{}", index))
                    .spawn(move || {
                        let logger = log_sender(RelayTimelyCommunicationSetup {
                            sender: true,
                            relay_node_index: index,
                            timely_worker_process_index: 0,
                        });
                        send_loop_to_relay(stream, remote_recv, 0, index, logger);
                    })?;

            send_guards.push(join_guard);
        }

        let remote_send = futures_iter.next().unwrap();
        {
            let log_sender = relay_log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
                ::std::thread::Builder::new()
                    .name(format!("timely-to-relay:recv-{}", index))
                    .spawn(move || {
                        let logger = log_sender(RelayTimelyCommunicationSetup {
                            sender: false,
                            relay_node_index: index,
                            timely_worker_process_index: 0,
                        });
                        recv_loop_from_relay(stream, remote_send, 0, 0, index, logger);
                    })?;

            recv_guards.push(join_guard);
        }
    }

    // returns #threads/process TcpBuilders
    Ok((builders, CommsGuard { send_guards, recv_guards }))
}