// READ, Sep 8 2021
//! Network initialization.

use std::sync::Arc;
// use crate::allocator::Process;
use crate::allocator::process::ProcessBuilder;
use crate::networking::create_sockets;
use super::tcp::{send_loop, recv_loop};
use super::allocator::{TcpBuilder, new_vector};

/// Join handles for send and receive threads.
///
/// On drop, the guard joins with each of the threads to ensure that they complete
/// cleanly and send all necessary data.
pub struct CommsGuard {
    pub(crate) send_guards: Vec<::std::thread::JoinHandle<()>>,
    pub(crate) recv_guards: Vec<::std::thread::JoinHandle<()>>,
}

// join network threads when dropped
impl Drop for CommsGuard {
    fn drop(&mut self) {
        for handle in self.send_guards.drain(..) {
            handle.join().expect("Send thread panic");
        }
        // println!("SEND THREADS JOINED");
        for handle in self.recv_guards.drain(..) {
            handle.join().expect("Recv thread panic");
        }
        // println!("RECV THREADS JOINED");
    }
}

use crate::logging::{CommunicationSetup, CommunicationEvent};
use logging_core::Logger;

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    // log_sender takes a CommunicationSetup and returns a logger (which logs CommunicationEvent)
    // CommunicationSetup is the ID of the logger
    log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    // return (#processes - 1) sockets
    let sockets = create_sockets(addresses, my_index, noisy)?;
    initialize_networking_from_sockets(sockets, my_index, threads, log_sender)
}

/// Initialize send and recv threads from sockets.
///
/// This method is available for users who have already connected sockets and simply wish to construct
/// a vector of process-local allocators connected to instantiated send and recv threads.
///
/// It is important that the `sockets` argument contain sockets for each remote process, in order, and
/// with position `my_index` set to `None`.
pub fn initialize_networking_from_sockets(
    mut sockets: Vec<Option<std::net::TcpStream>>,
    my_index: usize,
    threads: usize,
    log_sender: Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    // Sockets are expected to be blocking,
    for socket in sockets.iter_mut() {
        if let Some(socket) = socket {
            // we only set_nondelay when we create sockets.
            socket.set_nonblocking(false).expect("failed to set socket to blocking");
        }
    }

    // we need to use an Arc pointer to wrap the log_sender to share between network threads
    let log_sender = Arc::new(log_sender);
    let processes = sockets.len();

    let process_allocators = crate::allocator::process::Process::new_vector(threads);
    let (builders, promises, futures) = new_vector(process_allocators, my_index, processes);

    // promises and futures to send to the network threads (both a vector of (#process - 1) vectors of #threads/process
    // promises are send to the send_loop threads
    // futures are send to the recv_loop threads
    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    // len: #processes - 1
    let mut send_guards = Vec::with_capacity(sockets.len());
    let mut recv_guards = Vec::with_capacity(sockets.len());

    // for each process, if a stream exists (i.e. not local) ...
    // after s.map(|s| (i,s))
    // the type is Option<(usize, TcpStream)>
    // sockets.into_iter().enumerate().filter_map(|(i, s)| s.map(|s| (i, s))) is a FilterMap
    // filter_map() takes a FnMut(Self::Item) -> Option<B>, it will filter out None items
    // into_iter() consumes `self`
    for (index, stream) in sockets.into_iter().enumerate().filter_map(|(i, s)| s.map(|s| (i, s))) {
        let remote_recv = promises_iter.next().unwrap();

        {
            let log_sender = log_sender.clone();
            let stream = stream.try_clone()?;
            let join_guard =
            ::std::thread::Builder::new()
                .name(format!("timely:send-{}", index))
                .spawn(move || {
                    // CommunicationSetup entails the identity of this connection (communication channel)
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
            let log_sender = log_sender.clone();
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

    // returns #threads/process TcpBuilders
    Ok((builders, CommsGuard { send_guards, recv_guards }))
}
