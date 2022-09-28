//! Network threads functions for relay nodes

use chrono::Utc;
use crossbeam_channel::{Receiver, Sender};
use std::io::{Read, Write};
use std::net::TcpStream;

use crate::allocator::zero_copy::bytes_exchange::{BytesPull, BytesPush, MergeQueue};
use crate::allocator::zero_copy::bytes_slab::BytesSlab;

use super::logging::{
    RelayCommunicationEvent, RelayCommunicationSetup, RelayMessageEvent, RelayStateEvent,
};
use crate::allocator::relay::header::{RelayToRelayMessageHeader, RelayToTimelyMessageHeader};
use crate::allocator::relay::logging::{
    RelayTimelyCommMessageHeader, RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup,
    RelayTimelyMessageEvent, RelayTimelyStateEvent,
};
use crate::networking::MessageHeader;
use logging_core::Logger;

/// Receive data from relay nodes in input pipelines.
/// One thread for each relay node in the input pipeline,
/// the thread executes recv_input_pipeline_loop
pub fn recv_input_pipeline_loop(
    mut reader: TcpStream,
    // receive the MergeQueue sent from
    // the thread handling the input pipeline (of the relay node the socket connects to)
    // to push received data into this MergeQueue
    target: Receiver<MergeQueue>,
    // the index of the input pipeline connected to
    pipeline_index: usize,
    // current running relay node (running this recv_loop)'s index
    relay_node_index: usize,
    // Logger
    mut logger: Option<Logger<RelayCommunicationEvent, RelayCommunicationSetup>>,
) {
    logger.as_mut().map(|l| {
        l.log(RelayStateEvent {
            send: false,
            pipeline_index,
            local_relay_node_index: relay_node_index,
            start: true,
        })
    });

    let mut target = target.recv().expect("Failed to receive MergeQueue");

    let mut buffer = BytesSlab::new(20);
    let mut staged = Vec::new();

    let mut active = true;
    while active {
        buffer.ensure_capacity(1);

        // ensure that we have at least one byte to write
        assert!(!buffer.empty().is_empty());

        let read = match reader.read(&mut buffer.empty()) {
            // it returns how many bytes were read
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            }
        };

        assert!(read > 0);
        buffer.make_valid(read);

        while let Some(header) = RelayToRelayMessageHeader::try_read(buffer.valid()) {
            let peeled_bytes = header.required_bytes();
            let mut bytes = buffer.extract(peeled_bytes);

            // Record message receipt.

            if header.length > 0 {
                // TODO: should we replace message header?
                // replace the header's source with input pipeline index
                // and the target
                let mut replace_header = header.clone();
                replace_header.source = pipeline_index;
                replace_header.target = relay_node_index;
                let curr_ts = Utc::now().timestamp_nanos();
                replace_header.recv_timestamp = Some(curr_ts);
                {
                    let mut raw_bytes = &mut *bytes;
                    let ref mut writer = raw_bytes;
                    replace_header.write_to(writer).unwrap();
                }
                logger.as_mut().map(|logger| {
                    logger.log(RelayMessageEvent {
                        is_send: false,
                        header: replace_header,
                    });
                });

                staged.push(bytes);
            } else {
                // Shutting down upon receiving empty message (a message header with zero length)
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                // buffer.ensure_capacity(1);
                // // read should return Ok(0)
                // if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                //     panic!("Clean shutdown followed by data.");
                // }
            }
        }

        // pass bytes to the input relay worker thread.
        target.extend(staged.drain(..));
    }

    // Log the receive thread's end.
    logger.as_mut().map(|l| {
        l.log(RelayStateEvent {
            send: false,
            pipeline_index,
            local_relay_node_index: relay_node_index,
            start: false,
        })
    });
}

/// Repeatedly sends messages into a relay node in an output pipeline.
/// One thread executing send_loop for each of the connected relay node in all output pipelines
pub fn send_output_pipeline_loop(
    writer: TcpStream,
    source: Sender<MergeQueue>,
    // the index of the output pipeline connected to
    pipeline_index: usize,
    // current running relay node (running this send_loop)'s index
    relay_node_index: usize,
    mut logger: Option<Logger<RelayCommunicationEvent, RelayCommunicationSetup>>,
) {
    logger.as_mut().map(|l| {
        l.log(RelayStateEvent {
            send: true,
            pipeline_index,
            local_relay_node_index: relay_node_index,
            start: true,
        })
    });

    let mut source = {
        let buzzer = crate::buzzer::Buzzer::new();
        let queue = MergeQueue::new(buzzer);
        // send the MergeQueue to the output relay worker thread
        // for it to push data
        // the send_loop network thread will be notified when MergeQueue gets new data
        source
            .send(queue.clone())
            .expect("failed to send MergeQueue");
        queue
    };

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    let mut active = true;
    while active {
        source.drain_into(&mut stash);

        if stash.is_empty() {
            writer.flush().expect("Failed to flush writer.");
            // if source (MergeQueue) is completed, is dropped by the output relay worker thread
            // this network thread that executes send_loop
            // will be the only one holding the MergeQueue
            if source.is_complete() {
                active = false;
            }
            if active {
                std::thread::park();
            }
        } else {
            for mut bytes in stash.drain(..) {
                logger.as_mut().map(|logger| {
                    let mut offset = 0;
                    while let Some(header) =
                        RelayToRelayMessageHeader::try_read(&mut bytes[offset..])
                    {
                        logger.log(RelayMessageEvent {
                            is_send: true,
                            header,
                        });
                        // move to next MessageHeader contains in this Bytes
                        // a Bytes may contain multiple messages
                        offset += header.required_bytes();
                    }
                });

                let mut offset = 0;
                while let Some(mut header) =
                    RelayToRelayMessageHeader::try_read(&mut bytes[offset..])
                {
                    // move to next MessageHeader contains in this Bytes
                    // a Bytes may contain multiple messages
                    let curr_ts = Utc::now().timestamp_nanos();
                    header.send_timestamp = Some(curr_ts);
                    let mut raw_bytes = &mut bytes[offset..];
                    let ref mut writer = raw_bytes;
                    header.write_to(writer).unwrap();
                    offset += header.required_bytes();
                }

                writer
                    .write_all(&bytes[..])
                    .expect("Write failure in send_loop.");
            }
        }
    }

    let header = RelayToRelayMessageHeader {
        channel: 0,
        source: 0,
        target: 0,
        length: 0,
        seqno: 0,
        send_timestamp: None,
        recv_timestamp: None,
    };
    header
        .write_to(&mut writer)
        .expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    // writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    logger.as_mut().map(|logger| {
        logger.log(RelayMessageEvent {
            is_send: true,
            header,
        })
    });

    logger.as_mut().map(|l| {
        l.log(RelayStateEvent {
            send: true,
            pipeline_index,
            local_relay_node_index: relay_node_index,
            start: false,
        })
    });
}

// Repeatedly sends messages into a timely worker process
pub fn send_timely_loop(
    writer: TcpStream,
    // the input relay worker will send
    sources: Vec<Sender<MergeQueue>>,
    // process id of the timely worker
    timely_worker_process_index: usize,
    // current running relay node (running this recv_loop)'s index
    relay_node_index: usize,
    mut logger: Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>,
) {
    logger.as_mut().map(|l| {
        l.log(RelayTimelyStateEvent {
            send: true,
            relay_node_index,
            timely_worker_process_index,
            start: true,
        })
    });

    let mut sources: Vec<MergeQueue> = sources
        .into_iter()
        .map(|x| {
            let buzzer = crate::buzzer::Buzzer::new();
            let queue = MergeQueue::new(buzzer);
            x.send(queue.clone()).expect("failed to send MergeQueue");
            queue
        })
        .collect();

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    while !sources.is_empty() {
        for source in sources.iter_mut() {
            source.drain_into(&mut stash);
        }

        if stash.is_empty() {
            writer.flush().expect("Failed to flush writer.");
            // retain only incomplete sources (from incomplete worker threads)
            sources.retain(|source| !source.is_complete());
            if !sources.is_empty() {
                std::thread::park();
            }
        } else {
            for mut bytes in stash.drain(..) {
                logger.as_mut().map(|logger| {
                    let mut offset = 0;
                    // MessageHeader::try_read reads and clones the MessageHeader from bytes
                    while let Some(header) =
                        RelayToTimelyMessageHeader::try_read(&mut bytes[offset..])
                    {
                        let wrapper = RelayTimelyCommMessageHeader::TimelyRecv(header);
                        logger.log(RelayTimelyMessageEvent {
                            is_send: true,
                            header: wrapper,
                        });
                        // move to next MessageHeader contains in this Bytes
                        offset += header.required_bytes();
                    }
                });

                writer
                    .write_all(&bytes[..])
                    .expect("Write failure in send_loop.");
            }
        }
    }

    let header = RelayToTimelyMessageHeader {
        channel: 0,
        source: 0,
        target: 0,
        length: 0,
        seqno: 0,
        relay_transmission_latency: None,
    };
    header
        .write_to(&mut writer)
        .expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    // writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    let header = RelayTimelyCommMessageHeader::TimelyRecv(header);
    logger.as_mut().map(|logger| {
        logger.log(RelayTimelyMessageEvent {
            is_send: true,
            header,
        })
    });

    logger.as_mut().map(|l| {
        l.log(RelayTimelyStateEvent {
            send: true,
            relay_node_index,
            timely_worker_process_index,
            start: false,
        })
    });
}

/// Repeatedly receives messages from a timely worker process,
/// broadcast messages received to every output pipeline relay worker,
/// directly passthrough the pointer to the same underlying space of bytes
/// to each relay worker
#[allow(dead_code)]
pub fn recv_passthrough_broadcast_timely_loop(
    mut reader: TcpStream,
    // receives MergeQueues sent from output pipeline relay workers
    // length: #num_output_piplines
    // broadcast received data from timely workers
    // to all output pipeline relay workers
    targets: Vec<Receiver<MergeQueue>>,
    // process id of the timely worker
    timely_worker_process_index: usize,
    // current running relay node (running this recv_loop)'s index
    relay_node_index: usize,
    // Logger
    mut logger: Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>,
) {
    logger.as_mut().map(|l| {
        l.log(RelayTimelyStateEvent {
            send: false,
            relay_node_index,
            timely_worker_process_index,
            start: true,
        })
    });

    let mut targets: Vec<MergeQueue> = targets
        .into_iter()
        .map(|x| x.recv().expect("Failed to receive MergeQueue"))
        .collect();

    let mut buffer = BytesSlab::new(20);

    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0..targets.len() {
        stageds.push(Vec::new());
    }

    let mut active = true;

    while active {
        buffer.ensure_capacity(1);
        // ensure that we have at least one byte to write
        assert!(!buffer.empty().is_empty());

        let read = match reader.read(&mut buffer.empty()) {
            // it returns how many bytes were read
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            }
        };

        assert!(read > 0);
        buffer.make_valid(read);

        while let Some(header) = MessageHeader::try_read(buffer.valid()) {
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            // Record message receipt.
            logger.as_mut().map(|logger| {
                let wrapper = RelayTimelyCommMessageHeader::TimelySend(header);
                logger.log(RelayTimelyMessageEvent {
                    is_send: false,
                    header: wrapper,
                });
            });

            if header.length > 0 {
                // pass-through the "pointer" to the same data
                // to every target (output pipeline)
                for index in 0..targets.len() {
                    stageds[index].push(bytes.clone());
                }
            } else {
                // Shutting down upon receiving empty message (a message header with zero length)
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                // buffer.ensure_capacity(1);
                // // read should return Ok(0)
                // if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                //     panic!("Clean shutdown followed by data.");
                // }
            }
        }

        // pass bytes to the input relay worker thread.
        for (index, staged) in stageds.iter_mut().enumerate() {
            targets[index].extend(staged.drain(..));
        }
    }

    // Log the receive thread's end.
    logger.as_mut().map(|l| {
        l.log(RelayTimelyStateEvent {
            send: false,
            relay_node_index,
            timely_worker_process_index,
            start: false,
        })
    });
}

/// Repeatedly receives messages from a timely worker process,
/// broadcast messages received to every output pipeline relay worker,
/// copy each message to every relay worker
#[allow(dead_code)]
pub fn recv_copy_broadcast_timely_loop(
    mut reader: TcpStream,
    // receives MergeQueues sent from output pipeline relay workers
    // length: #num_output_piplines
    // broadcast received data from timely workers
    // to all output pipeline relay workers
    targets: Vec<Receiver<MergeQueue>>,
    // process id of the timely worker
    timely_worker_process_index: usize,
    // current running relay node (running this recv_loop)'s index
    relay_node_index: usize,
    // Logger
    mut logger: Option<Logger<RelayTimelyCommunicationEvent, RelayTimelyCommunicationSetup>>,
) {
    logger.as_mut().map(|l| {
        l.log(RelayTimelyStateEvent {
            send: false,
            relay_node_index,
            timely_worker_process_index,
            start: true,
        })
    });

    let mut targets: Vec<MergeQueue> = targets
        .into_iter()
        .map(|x| x.recv().expect("Failed to receive MergeQueue"))
        .collect();

    // one buffer for an output pipeline (relay worker)
    let mut buffers = Vec::with_capacity(targets.len());
    for _index in 0..targets.len() {
        let buffer = BytesSlab::new(20);
        buffers.push(buffer);
    }

    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0..targets.len() {
        stageds.push(Vec::new());
    }

    let mut active = true;

    while active {
        for buffer in buffers.iter_mut() {
            buffer.ensure_capacity(1);
            // ensure that we have at least one byte to write
            assert!(!buffer.empty().is_empty());
        }

        {
            // we cannot mutably borrowing two elements of a Vec at the same time
            // see https://stackoverflow.com/questions/30073684/how-to-get-mutable-references-to-two-array-elements-at-the-same-time
            // read from TcpStream and copy to all buffers
            let (first_buffer, remaining) = buffers.split_at_mut(1);
            let first_buffer = &mut first_buffer[0];
            let read = match reader.read(first_buffer.empty()) {
                // it returns how many bytes were read
                Ok(n) => n,
                Err(x) => {
                    // We don't expect this, as socket closure results in Ok(0) reads.
                    println!("Error: {:?}", x);
                    0
                }
            };

            assert!(read > 0);
            first_buffer.make_valid(read);
            {
                let read_raw_bytes = first_buffer.valid();
                for buffer in remaining.iter_mut() {
                    let mut raw_bytes = buffer.empty();
                    raw_bytes.write_all(read_raw_bytes).unwrap();
                    buffer.make_valid(read);
                }
            }
        }

        let (first_buffer, remaining_buffers) = buffers.split_at_mut(1);
        let first_buffer = &mut first_buffer[0];
        while let Some(header) = MessageHeader::try_read(first_buffer.valid()) {
            let peeled_bytes = header.required_bytes();
            let bytes = first_buffer.extract(peeled_bytes);

            // Record message receipt.
            logger.as_mut().map(|logger| {
                let wrapper = RelayTimelyCommMessageHeader::TimelySend(header);
                logger.log(RelayTimelyMessageEvent {
                    is_send: false,
                    header: wrapper,
                });
            });

            if header.length > 0 {
                stageds[0].push(bytes);
                for (index, buffer) in remaining_buffers.iter_mut().enumerate() {
                    // shadow bytes in the outer scope
                    let bytes = buffer.extract(peeled_bytes);
                    stageds[index].push(bytes);
                }
            } else {
                // Shutting down upon receiving empty message (a message header with zero length)
                active = false;
                // extract headers in all copies of first_buffer
                for buffer in remaining_buffers.iter_mut() {
                    let _bytes = buffer.extract(peeled_bytes);
                }
                if !first_buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                // first_buffer.ensure_capacity(1);
                // // read should return Ok(0)
                // if reader
                //     .read(&mut first_buffer.empty())
                //     .expect("read failure")
                //     > 0
                // {
                //     panic!("Clean shutdown followed by data.");
                // }
            }
        }

        // pass bytes to the input relay worker thread.
        for (index, staged) in stageds.iter_mut().enumerate() {
            targets[index].extend(staged.drain(..));
        }
    }

    // Log the receive thread's end.
    logger.as_mut().map(|l| {
        l.log(RelayTimelyStateEvent {
            send: false,
            relay_node_index,
            timely_worker_process_index,
            start: false,
        })
    });
}
