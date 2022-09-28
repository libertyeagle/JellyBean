//! initialize networks of timely workers with direct relay communication
use std::net::TcpStream;
use std::sync::Arc;

use logging_core::Logger;

use crate::allocator::direct_relay::network_utils::{create_sockets, create_sockets_single_process_worker};
use crate::allocator::direct_relay::tcp::{input_pipeline_recv_loop, output_pipeline_send_loop};
use crate::allocator::direct_relay::tcp_allocator::{new_vector, TcpBuilder};
use crate::allocator::zero_copy::initialize::CommsGuard;
use crate::allocator::zero_copy::tcp::{recv_loop, send_loop};
use crate::logging::{CommunicationEvent, CommunicationSetup};

use super::logging::{DirectRelayCommunicationEvent, DirectRelayCommunicationSetup};
use super::process::{Process, ProcessBuilder};

/// Initialize timely worker's network connections
/// for a cluster of workers (Cluster allocator)
pub fn initialize_networking_with_relay(
    peer_workers_addrs: Vec<String>,
    input_pipeline_workers_addrs: Vec<Vec<String>>,
    output_pipeline_workers_addrs: Vec<Vec<String>>,
    num_input_pipelines_threads_per_process: Vec<usize>,
    num_output_pipelines_threads_per_process: Vec<usize>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    worker_log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>,
    relay_log_sender: Box<dyn Fn(DirectRelayCommunicationSetup)->Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>>+Send+Sync>)
    -> ::std::io::Result<(Vec<TcpBuilder>, CommsGuard)>
{
    let (sockets_to_workers, sockets_to_input_pipelines, sockets_to_output_pipelines) = create_sockets(
        peer_workers_addrs,
        input_pipeline_workers_addrs,
        output_pipeline_workers_addrs,
        my_index,
        noisy
    )?;

    initialize_networking_with_relay_from_sockets(
        sockets_to_workers,
        sockets_to_input_pipelines,
        sockets_to_output_pipelines,
        num_input_pipelines_threads_per_process,
        num_output_pipelines_threads_per_process,
        my_index,
        threads,
        worker_log_sender,
        relay_log_sender
    )
}

/// Initialize timely worker's network connections
/// for a single process allocator (Process)
pub fn initialize_networking_with_relay_single_worker_process(
    input_pipeline_workers_addrs: Vec<Vec<String>>,
    output_pipeline_workers_addrs: Vec<Vec<String>>,
    num_input_pipelines_threads_per_process: Vec<usize>,
    num_output_pipelines_threads_per_process: Vec<usize>,
    // current pipeline worker process's address
    current_pipeline_addr: String,
    // num of current pipeline worker threads (single process, multiple threads)
    num_current_pipeline_worker_threads: usize,
    noisy: bool,
    relay_log_sender: Box<dyn Fn(DirectRelayCommunicationSetup)->Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>>+Send+Sync>)
    -> ::std::io::Result<(Vec<ProcessBuilder>, CommsGuard)>
{
    let (sockets_to_input_pipelines, sockets_to_output_pipelines) = create_sockets_single_process_worker(
        input_pipeline_workers_addrs,
        output_pipeline_workers_addrs,
        current_pipeline_addr,
        noisy
    )?;

    initialize_networking_with_relay_from_sockets_single_worker_process(
        sockets_to_input_pipelines,
        sockets_to_output_pipelines,
        num_input_pipelines_threads_per_process,
        num_output_pipelines_threads_per_process,
        num_current_pipeline_worker_threads,
        relay_log_sender
    )
}

fn initialize_networking_with_relay_from_sockets(
    // sockets to workers
    mut sockets_to_workers: Vec<Option<TcpStream>>,
    // sockets to the input and output pipelines
    mut sockets_to_input_pipelines: Vec<Vec<TcpStream>>,
    mut sockets_to_output_pipelines: Vec<Vec<TcpStream>>,
    num_input_pipelines_threads_per_process: Vec<usize>,
    num_output_pipelines_threads_per_process: Vec<usize>,
    // index of current worker process
    my_index: usize,
    // num worker threads per worker process
    threads: usize,
    // peer worker log sender
    worker_log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>,
    // relay communication log sender
    relay_log_sender: Box<dyn Fn(DirectRelayCommunicationSetup)->Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>>+Send+Sync>)
    -> ::std::io::Result<(Vec<TcpBuilder>, CommsGuard)>
{
    let worker_log_sender = Arc::new(worker_log_sender);
    let relay_log_sender = Arc::new(relay_log_sender);
    let processes = sockets_to_workers.len();

    let process_allocators = crate::allocator::process::Process::new_vector(threads);

    for socket in sockets_to_workers.iter_mut() {
        if let Some(socket) = socket {
            // we only set_nondelay when we create sockets.
            socket.set_nonblocking(false).expect("failed to set socket to blocking");
        }
    }
    for socket in sockets_to_input_pipelines.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }
    for socket in sockets_to_output_pipelines.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }


    let mut num_worker_processes_input_pipelines = Vec::with_capacity(sockets_to_input_pipelines.len());
    let mut num_worker_processes_output_pipelines = Vec::with_capacity(sockets_to_output_pipelines.len());

    sockets_to_input_pipelines.iter()
        .for_each(|x| {
            num_worker_processes_input_pipelines.push(x.len());
        });
    sockets_to_output_pipelines.iter()
        .for_each(|x| {
            num_worker_processes_output_pipelines.push(x.len());
        });

    let builder = new_vector(
        process_allocators,
        my_index,
        processes,
        &num_worker_processes_input_pipelines[..],
        &num_worker_processes_output_pipelines[..],
        &num_input_pipelines_threads_per_process[..],
        &num_output_pipelines_threads_per_process[..]
    );

    let worker_builders = builder.worker_thread_builders;

    let mut promises_iter = builder.network_peer_promises.into_iter();
    let mut futures_iter = builder.network_peer_futures.into_iter();

    let mut send_loop_guards = Vec::with_capacity(num_worker_processes_input_pipelines.iter().sum::<usize>() + sockets_to_workers.len());
    let mut recv_loop_gurads = Vec::with_capacity(num_worker_processes_output_pipelines.iter().sum::<usize>() + sockets_to_workers.len());

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

            send_loop_guards.push(join_guard);
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

            recv_loop_gurads.push(join_guard);
        }
    }


    let input_pipeline_futures = builder.network_input_pipelines_futures;
    for (pipeline_index, (pipeline_sockets, pipeline_futures)) in sockets_to_input_pipelines.into_iter().zip(input_pipeline_futures).enumerate() {
        for (node_index, (stream, node_futures)) in pipeline_sockets.into_iter().zip(pipeline_futures).enumerate() {
            let log_sender = relay_log_sender.clone();
            let join_guard = std::thread::Builder::new()
                .name(format!("input-pipeline-{}:receiver", pipeline_index))
                .spawn(move || {
                    let logger = log_sender(DirectRelayCommunicationSetup {
                        sender: false,
                        local_worker_process: my_index,
                        remote_worker_process: node_index,
                        remote_pipeline_index: pipeline_index
                    });
                    input_pipeline_recv_loop(
                        stream,
                        node_futures,
                        my_index * threads,
                        my_index,
                        pipeline_index,
                        node_index,
                        logger
                    );
                })?;
            recv_loop_gurads.push(join_guard);
        }
    }

    let output_pipeline_promises = builder.network_output_pipelines_promises;
    for (pipeline_index, (pipeline_sockets, pipeline_promises)) in sockets_to_output_pipelines.into_iter().zip(output_pipeline_promises).enumerate() {
        for (node_index, (stream, node_promises)) in pipeline_sockets.into_iter().zip(pipeline_promises).enumerate() {
            let log_sender = relay_log_sender.clone();
            let join_guard = std::thread::Builder::new()
                .name(format!("output-pipeline-{}:sender", pipeline_index))
                .spawn(move || {
                    let logger = log_sender(DirectRelayCommunicationSetup {
                        sender: true,
                        local_worker_process: my_index,
                        remote_worker_process: node_index,
                        remote_pipeline_index: pipeline_index
                    });
                    output_pipeline_send_loop(
                        stream,
                        node_promises,
                        my_index,
                        pipeline_index,
                        node_index,
                        logger
                    );
                })?;
            send_loop_guards.push(join_guard);
        }
    }


    // returns #threads/process TcpBuilders
    Ok((worker_builders, CommsGuard {
        send_guards: send_loop_guards,
        recv_guards: recv_loop_gurads
    }))
}


fn initialize_networking_with_relay_from_sockets_single_worker_process(
    // sockets to the input and output pipelines
    mut sockets_to_input_pipelines: Vec<Vec<TcpStream>>,
    mut sockets_to_output_pipelines: Vec<Vec<TcpStream>>,
    num_input_pipelines_threads_per_process: Vec<usize>,
    num_output_pipelines_threads_per_process: Vec<usize>,
    // current pipeline worker threads (single process, multiple threads)
    worker_threads: usize,
    // log sender
    relay_log_sender: Box<dyn Fn(DirectRelayCommunicationSetup)->Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>>+Send+Sync>)
    -> ::std::io::Result<(Vec<ProcessBuilder>, CommsGuard)>
{
    for socket in sockets_to_input_pipelines.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }
    for socket in sockets_to_output_pipelines.iter_mut().flatten() {
        socket.set_nonblocking(false).expect("failed to set socket to blocking");
    }

    let relay_log_sender = Arc::new(relay_log_sender);

    let mut num_worker_processes_input_pipelines = Vec::with_capacity(sockets_to_input_pipelines.len());
    let mut num_worker_processes_output_pipelines = Vec::with_capacity(sockets_to_output_pipelines.len());

    sockets_to_input_pipelines.iter()
        .for_each(|x| {
            num_worker_processes_input_pipelines.push(x.len());
        });
    sockets_to_output_pipelines.iter()
        .for_each(|x| {
            num_worker_processes_output_pipelines.push(x.len());
        });

    let builder = Process::new_vector_with_direct_connect(
        worker_threads, 
        &num_worker_processes_input_pipelines[..], 
        &num_worker_processes_output_pipelines[..], 
        &num_input_pipelines_threads_per_process[..], 
        &num_output_pipelines_threads_per_process[..]
    );
  
    let worker_builders = builder.worker_thread_builders;

    let mut send_loop_guards = Vec::with_capacity(num_worker_processes_input_pipelines.iter().sum::<usize>());
    let mut recv_loop_gurads = Vec::with_capacity(num_worker_processes_output_pipelines.iter().sum::<usize>());

    let input_pipeline_futures = builder.network_input_pipelines_futures;
    for (pipeline_index, (pipeline_sockets, pipeline_futures)) in sockets_to_input_pipelines.into_iter().zip(input_pipeline_futures).enumerate() {
        for (node_index, (stream, node_futures)) in pipeline_sockets.into_iter().zip(pipeline_futures).enumerate() {
            let log_sender = relay_log_sender.clone();
            let join_guard = std::thread::Builder::new()
                .name(format!("input-pipeline-{}:receiver", pipeline_index))
                .spawn(move || {
                    let logger = log_sender(DirectRelayCommunicationSetup {
                        sender: false,
                        local_worker_process: 0,
                        remote_worker_process: node_index,
                        remote_pipeline_index: pipeline_index
                    });
                    input_pipeline_recv_loop(
                        stream,
                        node_futures,
                        0,
                        0,
                        pipeline_index,
                        node_index,
                        logger
                    );
                })?;
            recv_loop_gurads.push(join_guard);
        }
    }

    let output_pipeline_promises = builder.network_output_pipelines_promises;
    for (pipeline_index, (pipeline_sockets, pipeline_promises)) in sockets_to_output_pipelines.into_iter().zip(output_pipeline_promises).enumerate() {
        for (node_index, (stream, node_promises)) in pipeline_sockets.into_iter().zip(pipeline_promises).enumerate() {
            let log_sender = relay_log_sender.clone();
            let join_guard = std::thread::Builder::new()
                .name(format!("output-pipeline-{}:sender", pipeline_index))
                .spawn(move || {
                    let logger = log_sender(DirectRelayCommunicationSetup {
                        sender: true,
                        local_worker_process: 0,
                        remote_worker_process: node_index,
                        remote_pipeline_index: pipeline_index
                    });
                    output_pipeline_send_loop(
                        stream,
                        node_promises,
                        0,
                        pipeline_index,
                        node_index,
                        logger
                    );
                })?;
            send_loop_guards.push(join_guard);
        }
    }


    // returns #threads/process TcpBuilders
    Ok((worker_builders, CommsGuard {
        send_guards: send_loop_guards,
        recv_guards: recv_loop_gurads
    }))
}