//! Utils for direct relay communication

use std::collections::HashMap;
use std::io;
use std::io::{Read, Result};
use std::net::{TcpListener, TcpStream};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::Duration;

use abomonation::{decode, encode};

const PEER_HANDSHAKE_MAGIC: u64 = 0x6c51631a8397279d;
const RELAY_HANDSHAKE_MAGIC: u64 = 0x5f85c16b44c7544f;

/// Create sockets for a cluster of workers
pub fn create_sockets(
    peer_addresses: Vec<String>, 
    input_pipeline_worker_addresses: Vec<Vec<String>>,
    output_pipeline_worker_addresses: Vec<Vec<String>>,
    my_index: usize, 
    noisy: bool
) -> Result<(Vec<Option<TcpStream>>, Vec<Vec<TcpStream>>, Vec<Vec<TcpStream>>)> {
    let my_addr = peer_addresses[my_index].clone();

    let peer_hosts1 = Arc::new(peer_addresses);
    let peer_hosts2 = peer_hosts1.clone();

    let start_task_to_peers = thread::spawn(move || start_connections_to_peer_workers(peer_hosts1, my_index, noisy));
    let start_task_to_output_pipelines = thread::spawn(move || start_connections_to_output_pipelines(
        output_pipeline_worker_addresses, my_addr, my_index, noisy));

    let await_task = thread::spawn(move || await_connections(
        peer_hosts2, input_pipeline_worker_addresses, my_index, noisy));

    let mut sockets_to_peers = start_task_to_peers.join().unwrap()?;
    let (sockets_to_peers_to_extend, sockets_to_input_pipelines) = await_task.join().unwrap()?;
    // None as a place holder for the socket to the current running process itself
    sockets_to_peers.push(None);
    sockets_to_peers.extend(sockets_to_peers_to_extend.into_iter());
    let sockets_to_output_pipelines = start_task_to_output_pipelines.join().unwrap()?;

    if noisy { println!("worker {}:\tinitialization complete", my_index) }

    Ok((sockets_to_peers, sockets_to_input_pipelines, sockets_to_output_pipelines))
}


/// Create sockets with a single worker process
pub fn create_sockets_single_process_worker(
    input_pipeline_worker_addresses: Vec<Vec<String>>,
    output_pipeline_worker_addresses: Vec<Vec<String>>,
    my_addr: String,
    noisy: bool
) -> Result<(Vec<Vec<TcpStream>>, Vec<Vec<TcpStream>>)> {

    let my_addr_clone = my_addr.clone();
    let start_task = thread::spawn(move || start_connections_to_output_pipelines(
        output_pipeline_worker_addresses, my_addr_clone, 0, noisy));

    let await_task = thread::spawn(move || await_connections_single_process_worker(
        input_pipeline_worker_addresses, my_addr, noisy));

    let sockets_to_input_pipelines = await_task.join().unwrap()?;
    let sockets_to_output_pipelines = start_task.join().unwrap()?;

    if noisy { println!("worker:\tinitialization complete") }

    Ok((sockets_to_input_pipelines, sockets_to_output_pipelines))
}

fn start_connections_to_peer_workers(peer_workers_addrs: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let results = peer_workers_addrs.iter().take(my_index).enumerate().map(|(index, address)| {
        loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    unsafe { encode(&PEER_HANDSHAKE_MAGIC, &mut stream) }.expect("failed to encode/send handshake magic");
                    // send my_index as u64
                    unsafe { encode(&(my_index as u64), &mut stream) }.expect("failed to encode/send worker index");
                    if noisy { println!("worker {}:\tconnection to peer worker process {}", my_index, index); }
                    break Some(stream);
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to peer worker process {}: {}; retrying", my_index, index, error);
                    sleep(Duration::from_secs(1));
                },
            }
        }
    }).collect();

    Ok(results)
}

fn start_connections_to_output_pipelines(
    output_pipelines_workers_addrs: Vec<Vec<String>>,
    my_addr: String,
    my_index: usize,
    noisy: bool,
) -> Result<Vec<Vec<TcpStream>>>
{
    let my_addr = my_addr.to_socket_addrs().unwrap().next().unwrap();
    let results = output_pipelines_workers_addrs.iter().enumerate().map(|(pipeline_idx, addrs)| {
        addrs.into_iter().enumerate().map(|(worker_process_index, addr)| {
            loop {
                match TcpStream::connect(&addr[..]) {
                    Ok(mut stream) => {
                        stream.set_nodelay(true).expect("set_nodelay call failed");
                        unsafe { encode(&RELAY_HANDSHAKE_MAGIC, &mut stream) }.expect("failed to encode/send relay handshake magic");
                        unsafe { encode(&my_addr, &mut stream) }.expect("failed to encode/send local SocketAddr");
                        if noisy { println!("worker {}:\tconnection to worker process {} in output pipeline {}", my_index, worker_process_index, pipeline_idx); }
                        break stream;
                    }
                    Err(error) => {
                        println!("worker {}:\terror connecting to worker process {} in output pipeline {}: {}; retrying", my_index, worker_process_index, pipeline_idx, error);
                        sleep(Duration::from_secs(1));
                    }
                }
            }
        }).collect()
    }).collect();

    Ok(results)
}

fn await_connections(
    peer_workers_addrs: Arc<Vec<String>>,
    input_pipeline_workers_addrs: Vec<Vec<String>>,
    my_index: usize,
    noisy: bool
) -> Result<(Vec<Option<TcpStream>>, Vec<Vec<TcpStream>>)> {
    let mut sockets_to_peer_workers: Vec<_> = (0..(peer_workers_addrs.len() - my_index - 1)).map(|_| None).collect();
    let listener = TcpListener::bind(&peer_workers_addrs[my_index][..])?;

    let num_input_pipeline_worers: usize = input_pipeline_workers_addrs.iter().map(|x| x.len()).sum();

    let mut worker_addr_to_pipeline_mapping = HashMap::new();
    // (num_input_pipelines, num_worker_processes_in_i_th_pipeline)
    let mut sockets_to_input_pipelines = Vec::with_capacity(input_pipeline_workers_addrs.len());
    for idx in 0..input_pipeline_workers_addrs.len() {
        sockets_to_input_pipelines.push(Vec::with_capacity(input_pipeline_workers_addrs[idx].len()));
    }
    for (pipeline_index, workers_addrs) in input_pipeline_workers_addrs.into_iter().enumerate() {
        for (worker_process_index, addr) in workers_addrs.into_iter().enumerate() {
            let mut socket_addrs = addr.to_socket_addrs().expect("failed to translate addr to SocketAddr").collect::<Vec<_>>();
            let socket_addr = socket_addrs.pop().expect("failed to translate addr to SocketAddr");
            worker_addr_to_pipeline_mapping.insert(socket_addr, (pipeline_index, worker_process_index));
        }
    }

    for _ in 0.. peer_workers_addrs.len() - (my_index + 1) + num_input_pipeline_worers {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");

        let mut buffer = [0u8; 8];
        stream.read_exact(&mut buffer)?;
        let (magic, _buffer) = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode magic");
        match magic {
            &RELAY_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 32];
                stream.read_exact(&mut buffer)?;
                let handshake_remote_addr = unsafe { decode::<SocketAddr>(&mut buffer) }.expect("unable to decode input pipelien worker addr").0.clone();
                let (pipeline_idx, worker_process_index) = worker_addr_to_pipeline_mapping.get(&handshake_remote_addr).expect("receive connection from unspecified relay node");
                sockets_to_input_pipelines[*pipeline_idx].push((*worker_process_index, stream));
                if noisy { println!("worker {}:\tconnection from worker process {} in input pipeline {}", my_index, worker_process_index, pipeline_idx); }
            }
            &PEER_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 8];
                stream.read_exact(&mut buffer)?;
                let identifier = unsafe { decode::<u64>(&mut buffer) }.expect("unable to decode peer worker process index").0.clone() as usize;
                sockets_to_peer_workers[identifier - my_index - 1] = Some(stream);
                if noisy { println!("worker {}:\tconnection from peer worker process {}", my_index, identifier); }
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "received incorrect handshake"));
            }
        }
    }

    let sockets_to_input_pipelines = sockets_to_input_pipelines.into_iter()
    .map(|mut sockets| {
        sockets.sort_by(|x, y| x.0.cmp(&y.0));
        sockets.into_iter().map(|x| x.1).collect::<Vec<_>>()
    }).collect::<Vec<_>>();

    Ok((sockets_to_peer_workers, sockets_to_input_pipelines))
}


fn await_connections_single_process_worker(
    input_pipeline_workers_addrs: Vec<Vec<String>>,
    my_addr: String,
    noisy: bool
) -> Result<Vec<Vec<TcpStream>>> {
    let listener = TcpListener::bind(&my_addr[..])?;

    let num_input_pipeline_worers: usize = input_pipeline_workers_addrs.iter().map(|x| x.len()).sum();

    let mut worker_addr_to_pipeline_mapping = HashMap::new();
    // (num_input_pipelines, num_worker_processes_in_i_th_pipeline)
    let mut sockets_to_input_pipelines = Vec::with_capacity(input_pipeline_workers_addrs.len());
    for idx in 0..input_pipeline_workers_addrs.len() {
        sockets_to_input_pipelines.push(Vec::with_capacity(input_pipeline_workers_addrs[idx].len()));
    }
    for (pipeline_index, workers_addrs) in input_pipeline_workers_addrs.into_iter().enumerate() {
        for (worker_process_index, addr) in workers_addrs.into_iter().enumerate() {
            let mut socket_addrs = addr.to_socket_addrs().expect("failed to translate addr to SocketAddr").collect::<Vec<_>>();
            let socket_addr = socket_addrs.pop().expect("failed to translate addr to SocketAddr");
            worker_addr_to_pipeline_mapping.insert(socket_addr, (pipeline_index, worker_process_index));
        }
    }

    for _ in 0 .. num_input_pipeline_worers {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");

        let mut buffer = [0u8; 8];
        stream.read_exact(&mut buffer)?;
        let (magic, _buffer) = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode magic");
        match magic {
            &RELAY_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 32];
                stream.read_exact(&mut buffer)?;
                let handshake_remote_addr = unsafe { decode::<SocketAddr>(&mut buffer) }.expect("unable to decode input pipelien worker addr").0.clone();
                let (pipeline_idx, worker_process_index) = worker_addr_to_pipeline_mapping.get(&handshake_remote_addr).expect("receive connection from unspecified relay node");
                sockets_to_input_pipelines[*pipeline_idx].push((*worker_process_index, stream));
                if noisy { println!("worker:\tconnection from worker process {} in input pipeline {}", worker_process_index, pipeline_idx); }
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "received incorrect handshake"));
            }
        }
    }

    let sockets_to_input_pipelines = sockets_to_input_pipelines.into_iter()
    .map(|mut sockets| {
        sockets.sort_by(|x, y| x.0.cmp(&y.0));
        sockets.into_iter().map(|x| x.1).collect::<Vec<_>>()
    }).collect::<Vec<_>>();

    Ok(sockets_to_input_pipelines)
}
