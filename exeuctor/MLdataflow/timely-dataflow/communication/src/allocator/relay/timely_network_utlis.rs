//! utlis to create sockets to relay nodes for timely workers
use std::io::Result;
use std::net::TcpStream;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use abomonation::encode;

const WORKER_HANDSHAKE_MAGIC: u64 = 0xe801d7b42c68535e;

/// create sockets for timely workers to relay nodes
pub fn create_sockets_to_relay(
    relay_addresses: Vec<String>,
    worker_process_index: usize,
    noisy: bool
) -> Result<Vec<TcpStream>>
{
    let start_task = thread::spawn(move ||
        start_connections_to_relay(relay_addresses, worker_process_index, noisy));

    let results = start_task.join().unwrap()?;

    if noisy { println!("worker {}:\t to relay sockets initialization complete", worker_process_index) }

    Ok(results)
}

/// timely workers establish connections to relay node
pub fn start_connections_to_relay(
    relay_addresses: Vec<String>,
    worker_process_index: usize,
    noisy: bool
) -> Result<Vec<TcpStream>>
{
    let results = relay_addresses.iter().enumerate().map(|(index, address)| {
        loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    unsafe { encode(&WORKER_HANDSHAKE_MAGIC, &mut stream) }.expect("failed to encode/send handshake magic");
                    unsafe { encode(&(worker_process_index as u64), &mut stream) }.expect("failed to encode/send worker index");
                    if noisy { println!("worker {}:\tconnection to worker {}", worker_process_index, index); }
                    break stream;
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to relay node {}: {}; retrying", worker_process_index, index, error);
                    sleep(Duration::from_secs(1));
                },
            }
        }
    }).collect();

    Ok(results)
}

