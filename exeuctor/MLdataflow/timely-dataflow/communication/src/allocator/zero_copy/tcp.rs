// READ, Sep 5 2021
//!

use std::io::{Read, Write};
use std::net::TcpStream;
use crossbeam_channel::{Sender, Receiver};

use crate::networking::MessageHeader;

use super::bytes_slab::BytesSlab;
use super::bytes_exchange::MergeQueue;

use logging_core::Logger;

use crate::logging::{CommunicationEvent, CommunicationSetup, MessageEvent, StateEvent};

/// Repeatedly reads from a TcpStream and carves out messages.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
/// If the stream ends without being shut down, the receive thread panics in an attempt to
/// take down the computation and cause the failures to cascade.

// one progress will only execute a single recv_loop
// `targets` have #threads/progress elements
// each correspond to a single worker thread
// it dispatches the message to the target thread according to the MessageHeader
pub fn recv_loop(
    mut reader: TcpStream,
    targets: Vec<Receiver<MergeQueue>>,
    worker_offset: usize,
    process: usize,
    remote: usize,
    // Logger 
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{
    // recv_loop receive data from TcpStream and then dispatches (pushes) the data to the MergeQueue corresponds to each thread
    // it has to receive the MergeQueue used to push data into from the worker threads
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: true }));

    // receivers are dropped here, they are just used to transfer MergeQueue
    // one MergeQueue for each worker thread
    let mut targets: Vec<MergeQueue> = targets.into_iter().map(|x| x.recv().expect("Failed to receive MergeQueue")).collect();

    let mut buffer = BytesSlab::new(20);

    // Where we stash Bytes before handing them off.
    // for each worker thread (target), create a vector of bytes to stash.
    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0 .. targets.len() {
        stageds.push(Vec::new());
    }

    // Each loop iteration adds to `self.Bytes` and consumes all complete messages.
    // At the start of each iteration, `self.buffer[..self.length]` represents valid
    // data, and the remaining capacity is available for reading from the reader.
    //
    // Once the buffer fills, we need to copy uncomplete messages to a new shared
    // allocation and place the existing Bytes into `self.in_progress`, so that it
    // can be recovered once all readers have read what they need to.
    let mut active = true;
    while active {

        buffer.ensure_capacity(1);

        // ensure that we have at least one byte to write
        assert!(!buffer.empty().is_empty());

        // Attempt to read some more bytes into self.buffer.
        // read to &mut [u8]
        // in fact we don't need &mut, we can just type &mut buffer.empty() here
        // this would become &mut &mut [u8], and it coerced to &mut [u8]
        // block reading
        let read = match reader.read(&mut buffer.empty()) {
            // it returns how many bytes were read
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            },
        };

        assert!(read > 0);
        buffer.make_valid(read);

        // Consume complete messages from the front of self.buffer.
        // we comsumes all messages currently read
        while let Some(header) = MessageHeader::try_read(buffer.valid()) {

            // TODO: Consolidate message sequences sent to the same worker?
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            // Record message receipt.
            logger.as_mut().map(|logger| {
                logger.log(MessageEvent { is_send: false, header, });
            });

            if header.length > 0 {
                // worker_offset 
                // push both the header and the actual message into stage
                stageds[header.target - worker_offset].push(bytes);
            }
            else {
                // Shutting down; confirm absence of subsequent data.
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                // buffer.ensure_capacity(1);
                // read should return Ok(0)
                // FIXME: bug in timely dataflow
                // after one end of the TcpStream's send_loop exits, and send shutdown
                // the socket enters FIN_WAIT2, and may fail to receive more data from the other end
                // current fix: don't send Shutdown::Write
                // if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                //     panic!("Clean shutdown followed by data.");
                // }
            }
        }

        // Pass bytes along to targets.
        for (index, staged) in stageds.iter_mut().enumerate() {
            // FIXME: try to merge `staged` before handing it to BytesPush::extend
            use crate::allocator::zero_copy::bytes_exchange::BytesPush;
            // MergeQueue will merge consecutive bytes if they are written into Bytes with the same allocated space (managed by buffer)
            // when the capacity is not enough, new space will be allocated and new items may not located in the same space
            targets[index].extend(staged.drain(..));
        }
    }

    // Log the receive thread's end.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: false, }));
}

/// Repeatedly sends messages into a TcpStream.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
pub fn send_loop(
    // TODO: Maybe we don't need BufWriter with consolidation in writes.
    writer: TcpStream,
    sources: Vec<Sender<MergeQueue>>,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{
    // send_loop receives data from MergeQueue that the worker threads will send data into
    // and then pushes the data into TcpStream

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: true, }));

    // send the MergeQueue, then Senders are no longer needed
    let mut sources: Vec<MergeQueue> = sources.into_iter().map(|x| {
        // buzzer points to the thread that does send_loop
        // this MergeQueue is created by the send_loop and pushed to the worker threads
        // worker threads can then push data into this MergeQueue 
        let buzzer = crate::buzzer::Buzzer::new();
        let queue = MergeQueue::new(buzzer);
        x.send(queue.clone()).expect("failed to send MergeQueue");
        queue
    }).collect();

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    while !sources.is_empty() {

        // TODO: Round-robin better, to release resources fairly when overloaded.
        // we don't care about which thread sends the data
        // anyway, this information would only be contained in 
        for source in sources.iter_mut() {
            use crate::allocator::zero_copy::bytes_exchange::BytesPull;
            source.drain_into(&mut stash);
        }

        if stash.is_empty() {
            // No evidence of records to read, but sources not yet empty (at start of loop).
            // We are going to flush our writer (to move buffered data), double check on the
            // sources for emptiness and wait on a signal only if we are sure that there will
            // still be a signal incoming.
            //
            // We could get awoken by more data, a channel closing, or spuriously perhaps.

            // we wraps the TcpStream with BufWriter
            writer.flush().expect("Failed to flush writer.");
            // retain only incomplete sources (from incomplete worker threads)
            sources.retain(|source| !source.is_complete());
            if !sources.is_empty() {
                std::thread::park();
            }
        }
        else {
            // TODO: Could do scatter/gather write here.
            // consumes the elements in stash
            for mut bytes in stash.drain(..) {

                // Record message sends.
                logger.as_mut().map(|logger| {
                    let mut offset = 0;
                    // MessageHeader::try_read reads and clones the MessageHeader from bytes
                    while let Some(header) = MessageHeader::try_read(&mut bytes[offset..]) {
                        logger.log(MessageEvent { is_send: true, header, });
                        // move to next MessageHeader contains in this Bytes
                        offset += header.required_bytes();
                    }
                });

                writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
            }
        }
    }

    // Write final zero-length header.
    // Would be better with meaningful metadata, but as this stream merges many
    // workers it isn't clear that there is anything specific to write here.
    let header = MessageHeader {
        channel:    0,
        source:     0,
        target:     0,
        length:     0,
        seqno:      0,
    };
    header.write_to(&mut writer).expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    // shutdown the writer portion
    // writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    logger.as_mut().map(|logger| logger.log(MessageEvent { is_send: true, header }));

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: false, }));
}
