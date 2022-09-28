// READ, Sep 9 2021
//! Parking and unparking timely fibers.

use std::rc::Rc;
use std::sync::Arc;
use std::cell::RefCell;
use std::thread::Thread;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};
use std::cmp::Reverse;
use crossbeam_channel::{Sender, Receiver};
use futures_util::task::ArcWake;

/// Methods required to act as a timely scheduler.
///
/// The core methods are the activation of "paths", sequences of integers, and
/// the enumeration of active paths by prefix. A scheduler may delay the report
/// of a path indefinitely, but it should report at least one extension for the
/// empty path `&[]` or risk parking the worker thread without a certain unpark.
///
/// There is no known harm to "spurious wake-ups" where a not-active path is
/// returned through `extensions()`.
pub trait Scheduler {
    /// Mark a path as immediately scheduleable.
    fn activate(&mut self, path: &[usize]);
    /// Populates `dest` with next identifiers on active extensions of `path`.
    ///
    /// This method is where a scheduler is allowed to exercise some discretion,
    /// in that it does not need to present *all* extensions, but it can instead
    /// present only those that the runtime should schedule.
    fn extensions(&mut self, path: &[usize], dest: &mut Vec<usize>);
}

// Trait objects can be schedulers too.
impl Scheduler for Box<dyn Scheduler> {
    fn activate(&mut self, path: &[usize]) { (**self).activate(path) }
    fn extensions(&mut self, path: &[usize], dest: &mut Vec<usize>) { (**self).extensions(path, dest) }
}

// Activations: activation tracker, activate via call, timer-based, remote inter-thread activate
// map a function to path, for extend
// SyncActivation: a remote (thread-safe) handle to Activation (to activate remotely)
// Activator: activate a specific path
// SyncActivator: thread-safe Activator

/// Allocation-free activation tracker.
pub struct Activations {
    clean: usize,
    /// `(offset, length)`
    bounds: Vec<(usize, usize)>,
    slices: Vec<usize>,
    buffer: Vec<usize>,

    // Inter-thread activations.
    tx: Sender<Vec<usize>>,
    rx: Receiver<Vec<usize>>,

    // Delayed activations.
    timer: Instant,
    queue: BinaryHeap<Reverse<(Duration, Vec<usize>)>>,
}

impl Activations {

    /// Creates a new activation tracker.
    pub fn new(timer: Instant) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        Self {
            clean: 0,
            bounds: Vec::new(),
            slices: Vec::new(),
            buffer: Vec::new(),
            tx,
            rx,
            timer,
            queue: BinaryHeap::new(),
        }
    }

    /// Activates the task addressed by `path`.
    /// path is just a sequence of integers
    pub fn activate(&mut self, path: &[usize]) {
        // push the index and the path length for activation
        self.bounds.push((self.slices.len(), path.len()));
        self.slices.extend(path);
    }

    /// Schedules a future activation for the task addressed by `path`.
    pub fn activate_after(&mut self, path: &[usize], delay: Duration) {
        // TODO: We could have a minimum delay and immediately schedule anything less than that delay.
        if delay == Duration::new(0, 0) {
            self.activate(path);
        }
        else {
            let moment = self.timer.elapsed() + delay;
            // then this queue will activate from the ones with the smallest Duration
            self.queue.push(Reverse((moment, path.to_vec())));
        }
    }

    /// Discards the current active set and presents the next active set.
    pub fn advance(&mut self) {

        // Drain inter-thread activations.
        // self.rx is crossbeam channel receiver
        while let Ok(path) = self.rx.try_recv() {
            self.activate(&path[..])
        }

        // Drain timer-based activations.
        let now = self.timer.elapsed();
        // time's up! time for the first timer-based activation to activate
        while self.queue.peek().map(|Reverse((t,_))| t <= &now) == Some(true) {
            let Reverse((_time, path)) = self.queue.pop().unwrap();
            self.activate(&path[..]);
        }

        // empty idle paths
        //  Discards the current active set
        self.bounds.drain(.. self.clean);

        {   // Scoped, to allow borrow to drop.
            let slices = &self.slices[..];
            // self.bounds serves as an index into self.slices
            // we sort self.bounds according to the slices (paths)
            // so that now indexing through self.bounds
            // we ensure the order of paths
            self.bounds.sort_by_key(|x| &slices[x.0 .. (x.0 + x.1)]);
            // remove duplicated paths
            self.bounds.dedup_by_key(|x| &slices[x.0 .. (x.0 + x.1)]);
        }

        // Compact the slices.
        // buffer serves as a temp variable
        self.buffer.clear();
        for (offset, length) in self.bounds.iter_mut() {
            // usize implements Copy trait
            self.buffer.extend(&self.slices[*offset .. (*offset + *length)]);
            // correct the offset, calc it in the compacted slice
            // *off set modify self.bounds in place
            *offset = self.buffer.len() - *length;
        }
        ::std::mem::swap(&mut self.buffer, &mut self.slices);

        self.clean = self.bounds.len();
    }

    /// Maps a function across activated paths.
    pub fn map_active(&self, logic: impl Fn(&[usize])) {
        for (offset, length) in self.bounds.iter() {
            logic(&self.slices[*offset .. (*offset + *length)]);
        }
    }

    /// Sets as active any symbols that follow `path`.
    pub fn for_extensions(&self, path: &[usize], mut action: impl FnMut(usize)) {

        // position is the index of the path in self.bounds
        // If the value is not found then Result::Err is returned,
        // containing the index where a matching element could be inserted
        // while maintaining sorted order.
        let position =
        self.bounds[..self.clean]
            .binary_search_by_key(&path, |x| &self.slices[x.0 .. (x.0 + x.1)]);
        let position = match position {
            Ok(x) => x,
            Err(x) => x,
        };

        // we can skip the first `position` elements,
        // since the first self..clean paths are sorted.
        // we can use for since path is sorted via self.bounds[..self.clean].
        let mut previous = None;
        self.bounds
            .iter()
            .cloned()
            .skip(position)
            .map(|(offset, length)| &self.slices[offset .. (offset + length)])
            .take_while(|x | x.starts_with(path))
            // x: &[usize], is a path
            .for_each(|x| {
                // push non-empty, non-duplicate extensions.
                if let Some(extension) = x.get(path.len()) {
                    if previous != Some(*extension) {
                        action(*extension);
                        previous = Some(*extension);
                    }
                }
            });
    }

    /// Constructs a thread-safe `SyncActivations` handle to this activator.
    pub fn sync(&self) -> SyncActivations {
        SyncActivations {
            // other thread can use this tx to request to activate
            tx: self.tx.clone(),
            thread: std::thread::current(),
        }
    }

    /// Time until next scheduled event.
    ///
    /// This method should be used before putting a worker thread to sleep, as it
    /// indicates the amount of time before the thread should be unparked for the
    /// next scheduled activation.
    //  it indicates the amount of time before the thread should be unparked
    pub fn empty_for(&self) -> Option<Duration> {
        // we still get paths
        if !self.bounds.is_empty() {
            Some(Duration::new(0,0))
        }
        else {
            self.queue.peek().map(|Reverse((t,_a))| {
                let elapsed = self.timer.elapsed();
                if t < &elapsed { Duration::new(0,0) }
                    // we still need to wait for some time to let the timer-based activations to activate
                else { *t - elapsed }
            })
        }
    }
}

/// A thread-safe handle to an `Activations`.
pub struct SyncActivations {
    tx: Sender<Vec<usize>>,
    thread: Thread,
}

impl SyncActivations {
    /// Unparks the task addressed by `path` and unparks the associated worker
    /// thread.
    pub fn activate(&self, path: Vec<usize>) -> Result<(), SyncActivationError> {
        self.activate_batch(std::iter::once(path))
    }

    /// Unparks the tasks addressed by `paths` and unparks the associated worker
    /// thread.
    ///
    /// This method can be more efficient than calling `activate` repeatedly, as
    /// it only unparks the worker thread after sending all of the activations.
    pub fn activate_batch<I>(&self, paths: I) -> Result<(), SyncActivationError>
    where
        I: IntoIterator<Item = Vec<usize>>
    {
        // paths need to have an iterator that iterate over paths
        for path in paths.into_iter() {
            self.tx.send(path).map_err(|_| SyncActivationError)?;
        }
        self.thread.unpark();
        Ok(())
    }
}

/// A capability to activate a specific path.
pub struct Activator {
    path: Vec<usize>,
    // share a Activation tracker
    queue: Rc<RefCell<Activations>>,
}

impl Activator {
    /// Creates a new activation handle
    pub fn new(path: &[usize], queue: Rc<RefCell<Activations>>) -> Self {
        Self {
            path: path.to_vec(),
            queue,
        }
    }
    /// Activates the associated path.
    pub fn activate(&self) {
        self.queue
            .borrow_mut()
            .activate(&self.path[..]);
    }

    /// Activates the associated path after a specified duration.
    pub fn activate_after(&self, delay: Duration) {
        if delay == Duration::new(0, 0) {
            self.activate();
        }
        else {
            self.queue
                .borrow_mut()
                .activate_after(&self.path[..], delay);
        }
    }
}

/// A thread-safe version of `Activator`.
pub struct SyncActivator {
    path: Vec<usize>,
    queue: SyncActivations,
}

impl SyncActivator {
    /// Creates a new thread-safe activation handle.
    pub fn new(path: &[usize], queue: SyncActivations) -> Self {
        Self {
            path: path.to_vec(),
            queue,
        }
    }

    /// Activates the associated path and unparks the associated worker thread.
    pub fn activate(&self) -> Result<(), SyncActivationError> {
        self.queue.activate(self.path.clone())
    }
}

impl ArcWake for SyncActivator {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.activate().unwrap();
    }
}

/// The error returned when activation fails across thread boundaries because
/// the receiving end has hung up.
#[derive(Debug)]
pub struct SyncActivationError;

impl std::fmt::Display for SyncActivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("sync activation error in timely")
    }
}

impl std::error::Error for SyncActivationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// A wrapper that unparks on drop.
pub struct ActivateOnDrop<T>  {
    wrapped: T,
    address: Rc<Vec<usize>>,
    activator: Rc<RefCell<Activations>>,
}

use std::ops::{Deref, DerefMut};

impl<T> ActivateOnDrop<T> {
    /// Wraps an element so that it is unparked on drop.
    pub fn new(wrapped: T, address: Rc<Vec<usize>>, activator: Rc<RefCell<Activations>>) -> Self {
        Self { wrapped, address, activator }
    }
}

impl<T> Deref for ActivateOnDrop<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.wrapped
    }
}

impl<T> DerefMut for ActivateOnDrop<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.wrapped
    }
}

impl<T> Drop for ActivateOnDrop<T> {
    // activate on drop
    fn drop(&mut self) {
        self.activator.borrow_mut().activate(&self.address[..]);
    }
}
