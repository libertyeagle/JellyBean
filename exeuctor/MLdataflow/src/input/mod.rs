use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use timely::dataflow::Scope;

use crate::node::{GenericStream, GenericPipelineScope};

pub mod contained;
pub mod closure;
pub mod distributed_contained;
pub mod buffered_distributed_contained;
pub mod buffered_contained;

pub use contained::ContainedInputSource;
pub use closure::ClosureInputSource;
pub use distributed_contained::WorkerDistributedContainedInputSource;
pub use buffered_contained::BufferedContainedInputSource;
pub use buffered_distributed_contained::BufferedWorkerDistributedContainedInputSource;

pub trait GenericScope {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<S: Scope + 'static> GenericScope for S {
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any{ self }
}

pub trait GenericInputFeeder {
    fn build_stream(&mut self, scope: &mut dyn GenericScope, config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream>;
    fn step(&mut self) -> bool;
}

pub trait ExchangeGenericInputFeeder: GenericInputFeeder {
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream>;
    fn build_and_register_output(&mut self, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream>;
    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize);
}