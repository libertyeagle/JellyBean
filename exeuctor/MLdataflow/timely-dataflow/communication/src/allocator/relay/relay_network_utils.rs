//! utlis to create sockets for relay nodes
use std::collections::HashMap;
use std::io;
use std::io::{Read, Result};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::rc::Rc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use abomonation::{encode, decode};


// magic numbers to identify relay<->relay connection
// and relay<->worker connection
const RELAY_HANDSHAKE_MAGIC: u64 = 0xb8c6aabb07c9703a;
const WORKER_HANDSHAKE_MAGIC: u64 = 0xe801d7b42c68535e;

/// create sockets to relay nodes in input pipeline and output pipelines
/// we establish connections to the relay nodes in the output pipelines
/// and await connections from the timely workers and the relay nodes in the input pipelines
#[allow(dead_code)]
pub fn relay_create_sockets(
    input_pipelines_relay_node_addresses: Vec<Vec<String>>,
    output_pipelines_relay_node_addresses: Vec<Vec<String>>,
    timely_workers_addresses: Vec<String>,
    my_addr: String,
    relay_node_index: usize,
    noisy: bool,
) -> Result<(Vec<Vec<TcpStream>>, Vec<Vec<TcpStream>>, Vec<TcpStream>)>
{
    let my_addr_clone = my_addr.clone();
    let start_task = thread::spawn(move || {
        let output_pipelines_relay_node_addresses = Rc::new(output_pipelines_relay_node_addresses);
        relay_start_connections(output_pipelines_relay_node_addresses, relay_node_index, my_addr_clone, noisy)
    });
    let await_task = thread::spawn(move ||
        relay_await_connections_without_duplicated(timely_workers_addresses, input_pipelines_relay_node_addresses, my_addr, relay_node_index, noisy));

    let results_output = start_task.join().unwrap()?;
    let (results_timely, results_input) = await_task.join().unwrap()?;

    if noisy { println!("relay node {}:\tnetwork sockets initialization complete", relay_node_index) }

    Ok((results_input, results_output, results_timely))
}

/// create sockets to relay nodes in input pipeline and output pipelines
/// we establish connections to the relay nodes in the output pipelines
/// and await connections from the timely workers and the relay nodes in the input pipelines
/// the address in the input pipelines and output pipelines can overlap
#[allow(dead_code)]
fn relay_create_sockets_with_duplicated(
    input_pipelines_relay_node_addresses: Vec<Vec<String>>,
    output_pipelines_relay_node_addresses: Vec<Vec<String>>,
    timely_workers_addresses: Vec<String>,
    my_addr: String,
    relay_node_index: usize,
    noisy: bool,
) -> Result<(Vec<Vec<TcpStream>>, Vec<Vec<TcpStream>>, Vec<TcpStream>)>
{
    // TODO: fix relay node identification
    let connect_task = thread::spawn(move || -> Result<_> {
        let output_pipelines_relay_node_addresses = Rc::new(output_pipelines_relay_node_addresses);
        let output_sockets = Rc::new(relay_start_connections(output_pipelines_relay_node_addresses.clone(), relay_node_index, my_addr.clone(), noisy).unwrap());
        let (timely_sockets, input_sockets) = relay_await_connections_with_duplicated(
            timely_workers_addresses,
            input_pipelines_relay_node_addresses,
            output_pipelines_relay_node_addresses.clone(),
            output_sockets.clone(),
            my_addr,
            relay_node_index,
            noisy,
        )?;
        let output_sockets = Rc::try_unwrap(output_sockets).expect("failed to unwrap sockets to output pipelines");
        Ok((input_sockets, output_sockets, timely_sockets))
    });

    let (results_input, results_output, results_timely) = connect_task.join().unwrap()?;

    if noisy { println!("relay node {}:\tnetwork sockets initialization complete", relay_node_index) }

    Ok((results_input, results_output, results_timely))
}

fn relay_start_connections(
    output_pipelines_relay_node_addresses: Rc<Vec<Vec<String>>>,
    relay_node_index: usize,
    my_addr: String,
    noisy: bool,
) -> Result<Vec<Vec<TcpStream>>>
{
    let results = output_pipelines_relay_node_addresses.iter().enumerate().map(|(pipeline_idx, addrs)| {
        addrs.into_iter().enumerate().map(|(relay_idx, addr)| {
            loop {
                match TcpStream::connect(&addr[..]) {
                    Ok(mut stream) => {
                        stream.set_nodelay(true).expect("set_nodelay call failed");
                        unsafe { encode(&RELAY_HANDSHAKE_MAGIC, &mut stream) }.expect("failed to encode/send relay handshake magic");
                        let local_addr = my_addr.to_socket_addrs().unwrap().nth(0).unwrap();
                        unsafe { encode(&local_addr, &mut stream) }.expect("failed to encode/send local SocketAddr");
                        if noisy { println!("relay node {}:\tconnection to relay node {} in pipeline {}", relay_node_index, relay_idx, pipeline_idx); }
                        break stream;
                    }
                    Err(error) => {
                        println!("relay node {}:\terror connecting to relay node {} in pipeline {}: {}; retrying", relay_node_index, relay_idx, pipeline_idx, error);
                        sleep(Duration::from_secs(1));
                    }
                }
            }
        }).collect()
    }).collect();

    Ok(results)
}

#[allow(dead_code)]
fn relay_await_connections_without_duplicated(
    timely_workers_addresses: Vec<String>,
    input_pipelines_relay_nodes_addresses: Vec<Vec<String>>,
    my_addr: String,
    relay_node_idx: usize,
    noisy: bool,
) -> Result<(Vec<TcpStream>, Vec<Vec<TcpStream>>)>
{
    let listener = TcpListener::bind(my_addr)?;

    let num_relay_node: usize = input_pipelines_relay_nodes_addresses.iter().map(|x| x.len()).sum();
    let num_workers = timely_workers_addresses.len();

    let mut relay_node_addr_to_pipeline_map = HashMap::new();
    let mut sockets_to_input_relay_nodes = Vec::with_capacity(input_pipelines_relay_nodes_addresses.len());
    for idx in 0..input_pipelines_relay_nodes_addresses.len() {
        sockets_to_input_relay_nodes.push(Vec::with_capacity(input_pipelines_relay_nodes_addresses[idx].len()));
    }
    for (pipeline_index, relay_nodes_addrs) in input_pipelines_relay_nodes_addresses.into_iter().enumerate() {
        for (relay_index, addr) in relay_nodes_addrs.into_iter().enumerate() {
            let mut socket_addrs = addr.to_socket_addrs().expect("failed to translate addr to SocketAddr").collect::<Vec<_>>();
            let socket_addr = socket_addrs.pop().expect("failed to translate addr to SocketAddr");
            relay_node_addr_to_pipeline_map.insert(socket_addr, (pipeline_index, relay_index));
        }
    }

    let mut sockets_to_timely_workers = Vec::with_capacity(timely_workers_addresses.len());


    for _ in 0..num_relay_node + num_workers {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");

        let mut buffer = [0u8; 8];
        stream.read_exact(&mut buffer)?;
        let (magic, _buffer) = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode magic");
        match magic {
            &RELAY_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 32];
                stream.read_exact(&mut buffer)?;
                let handshake_remote_addr = unsafe { decode::<SocketAddr>(&mut buffer) }.expect("unable to decode relay node addr").0.clone();
                let (pipeline_idx, relay_idx) = relay_node_addr_to_pipeline_map.get(&handshake_remote_addr).expect("receive connection from unspecified relay node");
                sockets_to_input_relay_nodes[pipeline_idx.to_owned()].push((relay_idx.to_owned(), stream));
                if noisy { println!("relay node {}:\tconnection from relay node {} in input pipeline {}", relay_node_idx, relay_idx, pipeline_idx); }
            }
            &WORKER_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 8];
                stream.read_exact(&mut buffer)?;
                let worker_index = unsafe { decode::<u64>(&mut buffer) }.expect("unable to decode timely worker process index").0.to_owned() as usize;
                sockets_to_timely_workers.push((worker_index.to_owned(), stream));
                if noisy { println!("relay node {}:\tconnection from timely worker {}", relay_node_idx, worker_index); }
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "received incorrect handshake"));
            }
        }
    }

    let sockets_to_input_relay_nodes = sockets_to_input_relay_nodes.into_iter()
        .map(|mut sockets| {
            sockets.sort_by(|x, y| x.0.cmp(&y.0));
            sockets.into_iter().map(|x| x.1).collect::<Vec<_>>()
        }).collect::<Vec<_>>();

    sockets_to_timely_workers.sort_by(|x, y| x.0.cmp(&y.0));
    let sockets_to_timely_workers = sockets_to_timely_workers.into_iter()
        .map(|x| x.1).collect::<Vec<_>>();

    Ok((sockets_to_timely_workers, sockets_to_input_relay_nodes))
}

#[allow(dead_code)]
fn relay_await_connections_with_duplicated(
    timely_workers_addresses: Vec<String>,
    input_pipelines_relay_nodes_addresses: Vec<Vec<String>>,
    output_pipeline_addrs: Rc<Vec<Vec<String>>>,
    output_pipeline_sockets: Rc<Vec<Vec<TcpStream>>>,
    my_addr: String,
    relay_node_idx: usize,
    noisy: bool,
) -> Result<(Vec<TcpStream>, Vec<Vec<TcpStream>>)>
{
    let listener = TcpListener::bind(my_addr)?;

    let mut output_addr_to_idx_map = HashMap::new();
    for (pipeline_idx, addrs) in output_pipeline_addrs.iter().enumerate() {
        for (relay_idx, addr) in addrs.into_iter().enumerate() {
            output_addr_to_idx_map.insert(addr.clone(), (pipeline_idx, relay_idx));
        }
    }

    let num_relay_node: usize = input_pipelines_relay_nodes_addresses.iter().map(|x| x.len()).sum();
    let num_workers = timely_workers_addresses.len();
    let mut sockets_to_input_relay_nodes = Vec::with_capacity(input_pipelines_relay_nodes_addresses.len());
    for idx in 0..input_pipelines_relay_nodes_addresses.len() {
        sockets_to_input_relay_nodes.push(HashMap::with_capacity(input_pipelines_relay_nodes_addresses[idx].len()));
    }
    let mut relay_node_addr_to_pipeline_map = HashMap::new();
    for (pipeline_index, relay_nodes_addrs) in input_pipelines_relay_nodes_addresses.into_iter().enumerate() {
        for (relay_index, addr) in relay_nodes_addrs.into_iter().enumerate() {
            match output_addr_to_idx_map.get(&addr) {
                Some(&(output_pipeline_idx, output_relay_idx)) => {
                    let stream = output_pipeline_sockets[output_pipeline_idx][output_relay_idx].try_clone()?;
                    sockets_to_input_relay_nodes[pipeline_index].insert(relay_index, stream);
                }
                None => {
                    let mut socket_addrs = addr.to_socket_addrs().expect("failed to translate addr to SocketAddr").collect::<Vec<_>>();
                    let socket_addr = socket_addrs.pop().expect("failed to translate addr to SocketAddr");
                    relay_node_addr_to_pipeline_map.insert(socket_addr, (pipeline_index, relay_index));
                }
            }
        }
    }

    let mut sockets_to_timely_workers = Vec::with_capacity(timely_workers_addresses.len());

    for _ in 0..num_relay_node + num_workers {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");

        let mut buffer = [0u8; 8];
        stream.read_exact(&mut buffer)?;
        let (magic, _buffer) = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode magic");
        match magic {
            &RELAY_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 32];
                stream.read_exact(&mut buffer)?;
                let handshake_remote_addr = unsafe { decode::<SocketAddr>(&mut buffer) }.expect("unable to decode relay node addr").0.clone();
                let (pipeline_idx, relay_idx) = relay_node_addr_to_pipeline_map.get(&handshake_remote_addr).expect("receive connection from unspecified relay node");
                sockets_to_input_relay_nodes[pipeline_idx.to_owned()].insert(relay_idx.to_owned(), stream);
                if noisy { println!("relay node {}:\tconnection from relay node {} in input pipeline {}", relay_node_idx, relay_idx, pipeline_idx); }
            }
            &WORKER_HANDSHAKE_MAGIC => {
                let mut buffer = [0u8; 8];
                stream.read_exact(&mut buffer)?;
                let worker_index = unsafe { decode::<u64>(&mut buffer) }.expect("unable to decode timely worker process index").0.to_owned() as usize;
                sockets_to_timely_workers.push((worker_index.to_owned(), stream));
                if noisy { println!("relay node {}:\tconnection from timely worker {}", relay_node_idx, worker_index); }
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "received incorrect handshake"));
            }
        }
    }

    let sockets_to_input_relay_nodes = sockets_to_input_relay_nodes.into_iter()
        .map(|mut sockets| {
            let mut sockets_vec = Vec::with_capacity(sockets.len());
            for idx in 0..sockets.len() {
                sockets_vec.push(sockets.remove(&idx).unwrap());
            }
            sockets_vec
        }).collect::<Vec<_>>();

    sockets_to_timely_workers.sort_by(|x, y| x.0.cmp(&y.0));
    let sockets_to_timely_workers = sockets_to_timely_workers.into_iter()
        .map(|x| x.1).collect::<Vec<_>>();

    Ok((sockets_to_timely_workers, sockets_to_input_relay_nodes))
}
