use std::collections::HashMap;

use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use crate::builder::GraphBuilder;
use crate::graph::GraphNode;
use crate::input::GenericScope;
use crate::static_timely::timely_static_execute::execute as timely_execute;
use crate::static_timely::timely_static_execute::Config as TimelyConfig;


pub fn local_execute<T, F>(dag_builder: F, config: TimelyConfig)
where 
    T: Timestamp + Refines<()> + TotalOrder,
    F: Fn(&mut GraphBuilder<T>) + Send + Sync + 'static
{
    timely_execute(config, move |worker| {
        let worker_index = worker.index();
        let peers = worker.peers();
        let mut builder = GraphBuilder::new(worker_index, peers);
        (dag_builder)(&mut builder);
        let mut graph = builder.get_graph();
        let num_nodes = graph.operators.len();
        
        worker.dataflow::<T, _, _>(|scope| {
            let mut streams = HashMap::new();
            for i in 0..num_nodes {
                let node = graph.operators.get_mut(&i).expect("opeartor does not exist");
                match node {
                    GraphNode::LocalInputNode(node) => {
                        let stream = node.build_stream(scope as &mut dyn GenericScope, None);
                        streams.insert(i, stream);
                    }
                    GraphNode::ExchangeInputNode(_) => {
                        panic!("(pipeline) exchange operators cannot exist in non-piopelin");

                    },
                    GraphNode::LocalComputeNode(node) => {
                        let prev_indices = node.required_prev_nodes();
                        let mut prev_nodes = Vec::with_capacity(prev_indices.len());
                        for prev_idx in prev_indices.iter() {
                            prev_nodes.push(streams.get(prev_idx).unwrap());
                        }
                        let stream = node.build(&prev_nodes[..], None);
                        streams.insert(i, stream);
                    },
                    GraphNode::ExchangeComputeNode(_) => {
                        panic!("(pipeline) exchange operators cannot exist in non-pipeline mode");
                    },
                }
            }
        });


        let mut completed = false; 
        while !completed {
            completed = true;
            for index in graph.input_operator_indices.iter() {
                let input_node = graph.operators.get_mut(index).unwrap();
                if let GraphNode::LocalInputNode(input_node) = input_node {
                    if input_node.step() {
                        completed = false;
                    }
                }
                else {
                    panic!("node {} should be a pipeline-local input node", index);
                }
            }
            worker.step();
        }
    }).unwrap();
}


pub fn local_execute_thread<T, F>(dag_builder: F)
where 
    T: Timestamp + Refines<()> + TotalOrder,
    F: Fn(&mut GraphBuilder<T>) + Send + Sync + 'static
{
    local_execute(dag_builder, TimelyConfig::thread());
}

pub fn local_execute_process<T, F>(dag_builder: F, threads: usize)
where 
    T: Timestamp + Refines<()> + TotalOrder,
    F: Fn(&mut GraphBuilder<T>) + Send + Sync + 'static
{
    local_execute(dag_builder, TimelyConfig::process(threads));
}
