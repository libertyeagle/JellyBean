//! Direct relay allocate

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

pub use logging::{DirectRelayCommunicationEvent, DirectRelayCommunicationSetup};
pub use network_initialize::{initialize_networking_with_relay, initialize_networking_with_relay_single_worker_process};

use crate::{Allocate, Data, Message, Pull, Push};
use crate::allocator::Event;

pub mod process;
pub mod network_initialize;
pub mod network_utils;
pub mod logging;
pub mod tcp;
pub mod tcp_allocator;
pub mod generic;


/// Direct relay allocate
pub trait DirectRelayAllocate: Allocate {
    /// Number of input pipelines
    fn get_num_input_pipelines(&self) -> usize;
    /// Number of output pipelines
    fn get_num_output_pipelines(&self) -> usize;
    /// Number of workers in input pipeline `pipeline_index`
    fn get_num_input_pipeline_workers(&self, pipeline_index: usize) -> usize;
    /// Number of workers in output pipeline `pipeline_index`
    fn get_num_output_pipeline_workers(&self, pipeline_index: usize) -> usize;

    /// Allocate puller to pull from input pipeline
    fn allocate_input_pipeline_puller<T: Data>(&mut self, pipeline_index: usize, channel_identifier: usize) -> Box<dyn Pull<Message<T>>>;
    /// Allocate pushers to push from input pipeline
    fn allocate_output_pipeline_pushers<T: Data>(&mut self, pipeline_index: usize, channel_identifier: usize) -> Vec<Box<dyn Push<Message<T>>>>;

    /// Receive messages from the input pipelines, take in events
    fn receive_from_input_pipeline(&mut self, pipeline_index: usize);

    /// Receive messages from all input pipelines
    fn receive_from_all_input_pipelines(&mut self) {
        for idx in 0..self.get_num_input_pipelines() {
            self.receive_from_input_pipeline(idx);
        }
    }

    /// Finish allocated input pipeline `pipeline_index`'s channels
    fn finish_input_pipeline_channel_allocation(&mut self, pipeline_index: usize);

    /// Finish allocated all input pipelines' channels
    fn finish_input_pipeline_channel_allocation_all(&mut self) {
        for idx in 0..self.get_num_input_pipelines() {
            self.finish_input_pipeline_channel_allocation(idx);
        }
    }

    /// Signal the completion of a batch of reads from channels.
    fn release_output_pipeline_send(&mut self, pipeline_index: usize);

    /// Signal the completion of a batch of reads from channels (for app output pipelines).
    fn release_output_pipelines_send_all(&mut self) {
        for idx in 0..self.get_num_output_pipelines() {
            self.release_output_pipeline_send(idx);
        }
    }

    /// Communication events from the input pipelines
    fn input_pipeline_events(&self, pipeline_index: usize) -> &Rc<RefCell<VecDeque<(usize, Event)>>>;
    /// Awaits communication events from an input pipelines
    fn await_input_pipeline_events(&self, pipeline_index: usize, _duration: Option<Duration>);
    /// Awaits communication events from any input pipelines
    fn await_any_input_pipelines_events(&self, _duration: Option<Duration>);
    /// Awaits communication events from any input pipelines or from peer workers
    fn await_any_events(&self, _duration: Option<Duration>);

    /// Are all workers within an input pipeline finished?
    fn input_pipeline_completed(&self, pipeline_index: usize) -> bool;

    /// Are all input pipelines finished?
    fn all_input_pipelines_completed(&self) -> bool {
        (0..self.get_num_input_pipelines()).all(|x| self.input_pipeline_completed(x))
    }
}

