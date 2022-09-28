//! initialize networks of relay nodes
use std::net::TcpStream;
use std::sync::Arc;
use crate::allocator::relay::relay_allocator::{InputRelayWorkerBuilder, new_vector, OutputRelayWorkerBuilder};
use logging_core::Logger;
use crate::allocator::relay::logging::{RelayCommunicationEvent, RelayCommunicationSetup, RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup};
use crate::allocator::relay::relay_network_utils::relay_create_sockets;
use crate::allocator::relay::relay_tcp::{recv_input_pipeline_loop, send_output_pipeline_loop, send_timely_loop, recv_passthrough_broadcast_timely_loop};
use crate::allocator::zero_copy::initialize::CommsGuard;

/// initialize relay node's network connections
/// create sockets, spin up network threads for each socket
/// return builder to build allocators for input pipelines and output pipelines
/// and join guards to the network threads
pub fn initialize_relay_node_networking(
    input_pipelines_relay_node_addresses: Vec<Vec<String>>,
    output_pipelines_relay_node_addresses: Vec<Vec<String>>,
    timely_workers_addresses: Vec<String>,
    // address of the current relay node
    relay_node_addr: String,
    // index of the current relay node
    relay_node_index: usize,
    // number of peer relay nodes in this pipeline
    num_relay_nodes: usize,
    // number of worker threads per timely worker process
    threads_per_timely_worker: usize,
    noisy: bool,
    relay_log_sender: Box<dyn Fn(RelayCommunicationSetup)->Option<Logger<RelayCommunicationEvent, RelayCommunicationSetup>>+Send+Sync>,
    timely_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>+Send+Sync>
) -> ::std::io::Result<(Vec<InputRelayWorkerBuilder>, Vec<OutputRelayWorkerBuilder>, CommsGuard)> {
    let (input_sockets, output_sockets, timely_sockets) = relay_create_sockets(
        input_pipelines_relay_node_addresses,
        output_pipelines_relay_node_addresses,
        timely_workers_addresses,
        relay_node_addr,
        relay_node_index,
        noisy
    )?;
    if noisy {
        println!("all connections established.");
    }
    initialize_relay_node_networking_from_sockets(
        input_sockets,
        output_sockets,
        timely_sockets,
        relay_node_index,
        num_relay_nodes,
        threads_per_timely_worker,
        relay_log_sender,
        timely_log_sender
    )
}

fn initialize_relay_node_networking_from_sockets(
    mut sockets_to_input_pipeline_relays: Vec<Vec<TcpStream>>,
    mut sockets_to_output_pipeline_relays: Vec<Vec<TcpStream>>,
    mut sockets_to_workers: Vec<TcpStream>,
    relay_node_index: usize,
    num_relay_nodes: usize,
    threads_per_timely_worker: usize,
    relay_log_sender: Box<dyn Fn(RelayCommunicationSetup)->Option<Logger<RelayCommunicationEvent, RelayCommunicationSetup>>+Send+Sync>,
    timely_log_sender: Box<dyn Fn(RelayTimelyCommunicationSetup)->Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>+Send+Sync>
) -> ::std::io::Result<(Vec<InputRelayWorkerBuilder>, Vec<OutputRelayWorkerBuilder>, CommsGuard)>
{
    for socket in sockets_to_input_pipeline_relays.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }
    for socket in sockets_to_output_pipeline_relays.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }
    for socket in sockets_to_workers.iter_mut() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }

    let relay_log_sender = Arc::new(relay_log_sender);
    let timely_log_sender = Arc::new(timely_log_sender);

    let mut num_relay_nodes_input_pipelines = Vec::with_capacity(sockets_to_input_pipeline_relays.len());
    let mut num_relay_nodes_output_pipelines = Vec::with_capacity(sockets_to_output_pipeline_relays.len());

    sockets_to_input_pipeline_relays.iter()
        .for_each(|x| {
            num_relay_nodes_input_pipelines.push(x.len());
        });
    sockets_to_output_pipeline_relays.iter()
        .for_each(|x| {
            num_relay_nodes_output_pipelines.push(x.len());
        });

    let relay_builder = new_vector(
        relay_node_index,
        num_relay_nodes,
        num_relay_nodes_input_pipelines.clone(),
        num_relay_nodes_output_pipelines.clone(),
        sockets_to_workers.len(),
        threads_per_timely_worker,
    );
    let input_relay_builders = relay_builder.input_relay_worker_builders;
    let output_relay_builders = relay_builder.output_relay_worker_builders;

    let mut send_loop_guards = Vec::with_capacity(num_relay_nodes_input_pipelines.iter().sum::<usize>() + sockets_to_workers.len());
    let mut recv_loop_gurads = Vec::with_capacity(num_relay_nodes_output_pipelines.iter().sum::<usize>() + sockets_to_workers.len());

    let relay_futures = relay_builder.network_input_relay_worker_futures;
    for (pipeline_index, (pipeline_relay_sockets, futures)) in sockets_to_input_pipeline_relays.into_iter().zip(relay_futures).enumerate() {
        for (node_index, (socket, future)) in pipeline_relay_sockets.into_iter().zip(futures).enumerate() {
            let log_sender = relay_log_sender.clone();
            let join_guard = std::thread::Builder::new()
                .name(format!("input-pipeline-{}:receiver", pipeline_index))
                .spawn(move || {
                    let logger = log_sender(RelayCommunicationSetup {
                        sender: false,
                        local_relay_node_idx: relay_node_index,
                        remote_pipeline_index: pipeline_index,
                        remote_relay_node_idx: node_index
                    });

                    recv_input_pipeline_loop(
                        socket,
                        future,
                        pipeline_index,
                        relay_node_index,
                        logger
                    );
                })?;
            recv_loop_gurads.push(join_guard);
        }
    }

    let relay_promises = relay_builder.network_output_relay_worker_promises;
    for (pipeline_index, (pipeline_relay_sockets, promises)) in sockets_to_output_pipeline_relays.into_iter().zip(relay_promises).enumerate() {
        for (node_index, (socket, promise)) in pipeline_relay_sockets.into_iter().zip(promises).enumerate() {
            let log_sender = relay_log_sender.clone();
            let join_guard = std::thread::Builder::new()
                .name(format!("output-pipeline-{}:sender", pipeline_index))
                .spawn(move || {
                    let logger = log_sender(RelayCommunicationSetup{
                        sender: true,
                        local_relay_node_idx: relay_node_index,
                        remote_pipeline_index: pipeline_index,
                        remote_relay_node_idx: node_index
                    });

                    send_output_pipeline_loop(
                        socket,
                        promise,
                        pipeline_index,
                        relay_node_index,
                        logger
                    )
                })?;
            send_loop_guards.push(join_guard);
        }
    }

    let worker_promises = relay_builder.network_timely_workers_promises;
    let worker_futures = relay_builder.network_timely_workers_futures;

    let timely_worker_network_thread_resources = sockets_to_workers
        .into_iter()
        .zip(worker_promises)
        .zip(worker_futures)
        .enumerate();

    for (worker_process_idx, ((socket, promises), futures)) in timely_worker_network_thread_resources {
        {
            let log_sender = timely_log_sender.clone();
            let stream = socket.try_clone()?;
            let join_guard = std::thread::Builder::new()
                .name(format!("timely-connector-{}:sender", worker_process_idx))
                .spawn(move || {
                    let logger = log_sender(RelayTimelyCommunicationSetup {
                        sender: true,
                        relay_node_index,
                        timely_worker_process_index: worker_process_idx
                    });

                    send_timely_loop(
                        stream,
                        promises,
                        worker_process_idx,
                        relay_node_index,
                        logger
                    );
                })?;
            send_loop_guards.push(join_guard);
        }

        {
            let log_sender = timely_log_sender.clone();
            let stream = socket.try_clone()?;
            let join_guard = std::thread::Builder::new()
                .name(format!("timely-connector-{}:receiver", worker_process_idx))
                .spawn(move || {
                    let logger = log_sender(RelayTimelyCommunicationSetup {
                        sender: false,
                        relay_node_index,
                        timely_worker_process_index: worker_process_idx
                    });

                    recv_passthrough_broadcast_timely_loop(
                        stream,
                        futures,
                        worker_process_idx,
                        relay_node_index,
                        logger
                    );
                })?;
            recv_loop_gurads.push(join_guard);
        }
    }

    Ok((input_relay_builders, output_relay_builders, CommsGuard{
        send_guards: send_loop_guards,
        recv_guards: recv_loop_gurads
    }))
}
