// READ
//! A large binary allocation for writing and sharing.

use bytes::arc::Bytes;

/// A large binary allocation for writing and sharing.
///
/// A bytes slab wraps a `Bytes` and maintains a valid (written) length, and supports writing after
/// this valid length, and extracting `Bytes` up to this valid length. Extracted bytes are enqueued
/// and checked for uniqueness in order to recycle them (once all shared references are dropped).
pub struct BytesSlab {
    buffer:         Bytes,                      // current working buffer.
    in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
    stash:          Vec<Bytes>,                 // reclaimed and resuable buffers.
    shift:          usize,                      // current buffer allocation size.
    valid:          usize,                      // buffer[..valid] are valid bytes.
}

impl BytesSlab {
    /// Allocates a new `BytesSlab` with an initial size determined by a shift.
    pub fn new(shift: usize) -> Self {
        BytesSlab {
            // 2^shift bytes, current working buffer
            buffer: Bytes::from(vec![0u8; 1 << shift].into_boxed_slice()),
            // older allocated buffers, which may still be shared with other (e.g., returned from extract method)
            // we need to keep track of the older buffers since we might have a chance to recycle them
            in_progress: Vec::new(),
            // recycled space, now uniquely owned by stash
            stash: Vec::new(),
            shift,
            valid: 0,
        }
    }
    /// The empty region of the slab.
    // writes to the empty positions (after the valid length)
    pub fn empty(&mut self) -> &mut [u8] {
        &mut self.buffer[self.valid..]
    }
    /// The valid region of the slab.
    // read up to the valid length
    pub fn valid(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.valid]
    }
    /// Marks the next `bytes` bytes as valid.
    pub fn make_valid(&mut self, bytes: usize) {
        self.valid += bytes;
    }
    /// Extracts the first `bytes` valid bytes.
    pub fn extract(&mut self, bytes: usize) -> Bytes {
        debug_assert!(bytes <= self.valid);
        self.valid -= bytes;
        self.buffer.extract_to(bytes)
    }

    /// Ensures that `self.empty().len()` is at least `capacity`.
    ///
    /// This method may retire the current buffer if it does not have enough space, in which case
    /// it will copy any remaining contents into a new buffer. If this would not create enough free
    /// space, the shift is increased until it is sufficient.
    pub fn ensure_capacity(&mut self, capacity: usize) {

        if self.empty().len() < capacity {

            let mut increased_shift = false;

            // Increase allocation if copy would be insufficient.
            while self.valid + capacity > (1 << self.shift) {
                // if we need to increase shift
                // previously allocated buffers are all useless since we don't have the space to store the data
                self.shift += 1;
                // we no longer need to track previous buffers (in_progress)
                // although these buffers may still be used (shared) by others (users of BytesSlab)
                // we also need to clear stash, as these reclamied space also have no value
                self.stash.clear();         // clear wrongly sized buffers.
                self.in_progress.clear();   // clear wrongly sized buffers.
                increased_shift = true;
            }

            // Attempt to reclaim shared slices.
            if self.stash.is_empty() {
                // this will only happen if we have not increase allocation
                for shared in self.in_progress.iter_mut() {
                    if let Some(mut bytes) = shared.take() {
                        // if an older buffer is uniquely owned by us now (other shares to it are dropped by now)
                        // we can reclaim this space
                        if bytes.try_regenerate::<Box<[u8]>>() {
                            // NOTE: Test should be redundant, but better safe...
                            if bytes.len() == (1 << self.shift) {
                                self.stash.push(bytes);
                            }
                        }
                        else {
                            *shared = Some(bytes);
                        }
                    }
                }
                // clean the None values
                self.in_progress.retain(|x| x.is_some());
            }

            // if there is anything in stash, it must be the case that the shift is not increased
            // we can just use the reclaimed space
            let new_buffer = self.stash.pop().unwrap_or_else(|| Bytes::from(vec![0; 1 << self.shift].into_boxed_slice()));
            // replace returns the previous dest value
            let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

            // copy the valid part of the older buffer to the new one
            self.buffer[.. self.valid].copy_from_slice(&old_buffer[.. self.valid]);
            if !increased_shift {
                // keep track of the older buffer for a potential future chance to recycle the space
                self.in_progress.push(Some(old_buffer));
            }
        }
    }
}