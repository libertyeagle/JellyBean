//! A child dataflow scope, used to build nested dataflow scopes.

use std::rc::Rc;
use std::cell::RefCell;

use timely::communication::{Data, Pull, Push};
use timely::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use timely::communication::Message;
use timely::scheduling::Scheduler;
use timely::scheduling::activate::Activations;
use timely::progress::{Timestamp, Operate, SubgraphBuilder};
use timely::progress::{Source, Target};
use timely::progress::timestamp::Refines;
use timely::order::Product;
use timely::logging::TimelyLogger as Logger;
use timely::logging::TimelyProgressLogger as ProgressLogger;
use timely::worker::{AsWorker, Config};

use timely::dataflow::{Scope, ScopeParent};

/// Type alias for iterative child scope.
pub type Iterative<G, T> = Child<G, Product<<G as ScopeParent>::Timestamp, T>>;

/// A `Child` wraps a `Subgraph` and a parent `G: Scope`. It manages the addition
/// of `Operate`s to a subgraph, and the connection of edges between them.
pub struct Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>
{
    /// The subgraph under assembly.
    pub subgraph: Rc<RefCell<SubgraphBuilder<G::Timestamp, T>>>,
    /// A copy of the child's parent scope.
    pub parent: G,
    /// The log writer for this scope.
    pub logging: Option<Logger>,
    /// The progress log writer for this scope.
    pub progress_logging: Option<ProgressLogger>,
}

impl<G, T> Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>
{
    /// This worker's unique identifier.
    ///
    /// Ranges from `0` to `self.peers() - 1`.
    pub fn index(&self) -> usize { self.parent.index() }
    /// The total number of workers in the computation.
    pub fn peers(&self) -> usize { self.parent.peers() }
}

impl<G, T> AsWorker for Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>
{
    fn config(&self) -> &Config { self.parent.config() }
    fn index(&self) -> usize { self.parent.index() }
    fn peers(&self) -> usize { self.parent.peers() }
    fn allocate<D: Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<dyn Push<Message<D>>>>, Box<dyn Pull<Message<D>>>) {
        self.parent.allocate(identifier, address)
    }
    fn pipeline<D: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<Message<D>>, ThreadPuller<Message<D>>) {
        self.parent.pipeline(identifier, address)
    }
    fn new_identifier(&mut self) -> usize {
        self.parent.new_identifier()
    }
    fn log_register(&self) -> ::std::cell::RefMut<timely::logging_core::Registry<timely::logging::WorkerIdentifier>> {
        self.parent.log_register()
    }
}

impl<G, T> Scheduler for Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>
{
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.parent.activations()
    }
}

impl<G, T> ScopeParent for Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>
{
    type Timestamp = T;
}

impl<G, T> Scope for Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>,
{
    fn name(&self) -> String { self.subgraph.borrow().name.clone() }
    fn addr(&self) -> Vec<usize> { self.subgraph.borrow().path.clone() }
    fn add_edge(&self, source: Source, target: Target) {
        self.subgraph.borrow_mut().connect(source, target);
    }

    fn add_operator_with_indices(&mut self, operator: Box<dyn Operate<Self::Timestamp>>, local: usize, global: usize) {
        self.subgraph.borrow_mut().add_child(operator, local, global);
    }

    fn allocate_operator_index(&mut self) -> usize {
        self.subgraph.borrow_mut().allocate_child_id()
    }

    #[inline]
    fn scoped<T2, R, F>(&mut self, name: &str, func: F) -> R
    where
        T2: Timestamp+Refines<T>,
        F: FnOnce(&mut timely::dataflow::scopes::Child<Self, T2>) -> R,
    {
        let index = self.subgraph.borrow_mut().allocate_child_id();
        let path = self.subgraph.borrow().path.clone();

        let subscope = RefCell::new(SubgraphBuilder::new_from(index, path, self.logging().clone(), self.progress_logging.clone(), name));
        let result = {
            let mut builder = timely::dataflow::scopes::Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: self.logging.clone(),
                progress_logging: self.progress_logging.clone(),
            };
            func(&mut builder)
        };
        let subscope = subscope.into_inner().build(self);

        self.add_operator_with_index(Box::new(subscope), index);

        result
    }
}

impl<G, T> Clone for Child<G, T>
where
    G: ScopeParent + 'static,
    T: Timestamp+Refines<G::Timestamp>
{
    fn clone(&self) -> Self {
        Child {
            subgraph: self.subgraph.clone(),
            parent: self.parent.clone(),
            logging: self.logging.clone(),
            progress_logging: self.progress_logging.clone(),
        }
    }
}