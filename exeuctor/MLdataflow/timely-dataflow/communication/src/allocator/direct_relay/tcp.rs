//! Network threads functions of timely workers for communicating with relay nodes
use std::io::{Read, Write};
use std::net::TcpStream;

use crossbeam_channel::{Receiver, Sender};

use logging_core::Logger;

use crate::allocator::direct_relay::logging::{DirectRelayCommunicationEvent, DirectRelayCommunicationSetup, DirectRelayMessageEvent, DirectRelayStateEvent};
use crate::allocator::zero_copy::bytes_exchange::MergeQueue;
use crate::allocator::zero_copy::bytes_slab::BytesSlab;
use crate::networking::MessageHeader;

/// network thread to receive from timely workers in the input pipelines
/// there is one thread executing input_pipeline_recv_loop
/// for each timely worker process in each input pipeline
pub fn input_pipeline_recv_loop(
    mut reader: TcpStream,
    // from worker threads
    targets: Vec<Receiver<MergeQueue>>,
    worker_offset: usize,
    worker_process_index: usize,
    // the input pipeline index
    pipeline_index: usize,
    // remote worker (in the output pipeline) process index
    remote_process_index: usize,
    mut logger: Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>>)
{
    logger.as_mut().map(|l| l.log(DirectRelayStateEvent {
        send: false,
        pipeline_index,
        local: worker_process_index,
        start: true,
        remote: remote_process_index
    }));

    let mut targets: Vec<MergeQueue> = targets.into_iter().map(|x| x.recv().expect("Failed to receive MergeQueue")).collect();

    let mut buffer = BytesSlab::new(20);
    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0 .. targets.len() {
        stageds.push(Vec::new());
    }

    let mut active = true;
    while active {
        buffer.ensure_capacity(1);
        assert!(!buffer.empty().is_empty());
        let read = match reader.read(&mut buffer.empty()) {
            Ok(n) => n,
            Err(x) => {
                println!("Error: {:?}", x);
                0
            },
        };
        assert!(read > 0);
        buffer.make_valid(read);

        while let Some(header) = MessageHeader::try_read(buffer.valid()) {
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            logger.as_mut().map(|logger| {
                logger.log(DirectRelayMessageEvent { is_send: false, header, });
            });

            if header.length > 0 {
                stageds[header.target - worker_offset].push(bytes);
            }
            else {
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                buffer.ensure_capacity(1);
                if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                    panic!("Clean shutdown followed by data.");
                }
            }
        }

        for (index, staged) in stageds.iter_mut().enumerate() {
            use crate::allocator::zero_copy::bytes_exchange::BytesPush;
            targets[index].extend(staged.drain(..));
        }
    }

    logger.as_mut().map(|l| l.log(DirectRelayStateEvent {
        send: false,
        pipeline_index,
        local: worker_process_index,
        start: false,
        remote: remote_process_index
    }));
}

/// network thread to send messages to timely workers in the output pipeline
/// there is one thread executing output_pipeline_send_loop
/// for each timely worker process in each output pipeline
pub fn output_pipeline_send_loop(
    writer: TcpStream,
    // to worker threads in current pipeline
    sources: Vec<Sender<MergeQueue>>,
    worker_process_index: usize,
    // the output pipeline index
    pipeline_index: usize,
    // remote worker (in the output pipeline) process index
    remote_process_index: usize,
    mut logger: Option<Logger<DirectRelayCommunicationEvent, DirectRelayCommunicationSetup>>)
{
    logger.as_mut().map(|l| l.log(DirectRelayStateEvent{
        send: true,
        pipeline_index,
        local: worker_process_index,
        start: true,
        remote: remote_process_index
    }));

    let mut sources: Vec<MergeQueue> = sources.into_iter().map(|x| {
        let buzzer = crate::buzzer::Buzzer::new();
        let queue = MergeQueue::new(buzzer);
        x.send(queue.clone()).expect("failed to send MergeQueue");
        queue
    }).collect();

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    while !sources.is_empty() {
        for source in sources.iter_mut() {
            use crate::allocator::zero_copy::bytes_exchange::BytesPull;
            source.drain_into(&mut stash);
        }

        if stash.is_empty() {
            writer.flush().expect("Failed to flush writer.");
            sources.retain(|source| !source.is_complete());
            if !sources.is_empty() {
                std::thread::park();
            }
        }
        else {
            for mut bytes in stash.drain(..) {
                logger.as_mut().map(|logger| {
                    let mut offset = 0;
                    while let Some(header) = MessageHeader::try_read(&mut bytes[offset..]) {
                        logger.log(DirectRelayMessageEvent { is_send: true, header, });
                        offset += header.required_bytes();
                    }
                });

                writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
            }
        }
    }

    let header = MessageHeader {
        channel:    0,
        source:     0,
        target:     0,
        length:     0,
        seqno:      0,
    };
    header.write_to(&mut writer).expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    logger.as_mut().map(|logger| logger.log(DirectRelayMessageEvent { is_send: true, header }));

    logger.as_mut().map(|l| l.log(DirectRelayStateEvent{
        send: true,
        pipeline_index,
        local: worker_process_index,
        start: false,
        remote: remote_process_index
    }));
}
