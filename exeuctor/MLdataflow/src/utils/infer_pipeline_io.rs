use std::collections::HashSet;
use std::iter::FromIterator;

use crate::config::ExecutionConfigGUID; 

pub fn infer_pipeline_inputs(config: &mut ExecutionConfigGUID, pipeline_idx: usize) {
    let mut required_inputs = Vec::new();
    let graph_connections = config.graph_connections.as_ref().expect("graph connections should be provided");

    let pipeline_config = config.pipeline_configs.get(&pipeline_idx).expect(&*format!("could not find config for pipeline {}", pipeline_idx));
    let assigned_ops = &pipeline_config.assigned_ops;
    for node in assigned_ops {
        let inputs = graph_connections.incoming_edges.get(&node).expect("could not retrieve node {}'s inputs");
        for input in inputs.iter().filter(|x| !assigned_ops.contains(*x)) {
            required_inputs.push(*input);
        }
    }
    required_inputs.sort();
    let pipeline_config = config.pipeline_configs.get_mut(&pipeline_idx).unwrap();
    pipeline_config.required_input_ops = Some(required_inputs);
}

pub fn infer_pipeline_io_from_config(config: &mut ExecutionConfigGUID, pipeline_idx: usize) {
    let mut required_inputs = Vec::new();
    let mut registered_outputs = HashSet::new();
    let graph_connections = config.graph_connections.as_ref().expect("graph connections should be provided");

    let pipeline_config = config.pipeline_configs.get(&pipeline_idx).expect(&*format!("could not find config for pipeline {}", pipeline_idx));
    let assigned_ops = &pipeline_config.assigned_ops;
    for node in assigned_ops {
        let inputs = graph_connections.incoming_edges.get(&node).expect("could not retrieve node {}'s inputs");
        for input in inputs.iter().filter(|x| !assigned_ops.contains(*x)) {
            required_inputs.push(*input);
        }
    }

    let output_pipelines = pipeline_config.output_pipelines.clone();
    for idx in output_pipelines.iter() {
        if config.pipeline_configs.get(&idx).expect(&*format!("could not find config for pipeline {}", idx)).required_input_ops.is_none() {
            infer_pipeline_inputs(config, *idx);
        }
    }
    
    let output_pipelines = output_pipelines.into_iter().map(
        |idx| config.pipeline_configs.get(&idx).unwrap()
    ).collect::<Vec<_>>();

    {
        let pipeline_config = config.pipeline_configs.get(&pipeline_idx).expect(&*format!("could not find config for pipeline {}", pipeline_idx));
        let assigned_ops = &pipeline_config.assigned_ops;
        for pipeline in output_pipelines {
            let output_pipeline_required_inputs = pipeline.required_input_ops.as_ref().unwrap();
            for input_node in output_pipeline_required_inputs  {
                if assigned_ops.contains(input_node) {
                    registered_outputs.insert(*input_node);
                }            
            }
        }
    }

    required_inputs.sort();
    let mut registered_outputs = Vec::from_iter(registered_outputs);
    registered_outputs.sort();
    
    let pipeline_config = config.pipeline_configs.get_mut(&pipeline_idx).unwrap();
    pipeline_config.required_input_ops = Some(required_inputs);
    pipeline_config.output_ops = Some(registered_outputs);
}

pub fn infer_pipeline_io_from_config_all(config: &mut ExecutionConfigGUID) {
    let pipeline_indices = config.pipeline_configs.keys().map(|x| *x).collect::<Vec<_>>();
    for pipeline_idx in pipeline_indices {
        let visited = config.pipeline_configs.get(&pipeline_idx).unwrap().required_input_ops.is_some() && config.pipeline_configs.get(&pipeline_idx).unwrap().output_ops.is_some();
        if !visited {
            infer_pipeline_io_from_config(config, pipeline_idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use crate::graph::GraphConnections;
    use crate::ExecutionConfigGUID;
    use crate::PipelineConfigGUID;

    use super::{infer_pipeline_io_from_config, infer_pipeline_io_from_config_all};
    
    #[test]
    fn infer_all() {
        let pipeline_0_config = PipelineConfigGUID {
            pipeline_index: 0,
            assigned_ops: vec![1, 3],
            required_input_ops: None,
            output_ops: None,
            worker_addrs: vec![String::from("127.0.0.1:5000"), String::from("127.0.0.1:5001")],
            relay_addrs: vec![String::from("127.0.0.1:6000")],
            relay_load_balance_weights: None,
            input_pipelines: vec![],
            output_pipelines: vec![2],
            builder_configs: HashMap::new(),
            operator_configs: HashMap::new(),
            request_rates: HashMap::new(),
            source_operators: HashSet::new(),
        };
    
        let pipeline_1_config = PipelineConfigGUID {
            pipeline_index: 1,
            assigned_ops: vec![0, 2],
            required_input_ops: None,
            output_ops: None,
            worker_addrs: vec![String::from("127.0.0.1:5100")],
            relay_addrs: vec![String::from("127.0.0.1:6100")],
            relay_load_balance_weights: None,
            input_pipelines: vec![],
            output_pipelines: vec![2],
            builder_configs: HashMap::new(),
            operator_configs: HashMap::new(),
            request_rates: HashMap::new(),
            source_operators: HashSet::new(),
        };
    
        let pipeline_2_config = PipelineConfigGUID {
            pipeline_index: 2,
            assigned_ops: vec![4, 5, 6],
            required_input_ops: None,
            output_ops: None,
            worker_addrs: vec![String::from("127.0.0.1:5200")],
            relay_addrs: vec![String::from("127.0.0.1:6200"), String::from("127.0.0.1:6201")],
            relay_load_balance_weights: None,
            input_pipelines: vec![0, 1],
            output_pipelines: vec![],
            builder_configs: HashMap::new(),
            operator_configs: HashMap::new(),
            request_rates: HashMap::new(),
            source_operators: HashSet::new(),
        };

        let config = HashMap::from_iter([
            (0, pipeline_0_config),
            (1, pipeline_1_config),
            (2, pipeline_2_config)
        ]);

        let connections = GraphConnections {
            outgoing_edges: HashMap::from_iter([
                (0, vec![2]),
                (1, vec![3]),
                (2, vec![4]),
                (3, vec![4]),
                (4, vec![5]),
                (5, vec![6]),
                (6, vec![])
            ]),
            incoming_edges: HashMap::from_iter([
                (0, vec![]),
                (1, vec![]),
                (2, vec![0]),
                (3, vec![1]),
                (4, vec![2, 3]),
                (5, vec![4]),
                (6, vec![5])
            ])
        }; 

        let mut config = ExecutionConfigGUID { 
            pipeline_configs: config,
            op_name_guid_mapping: None, 
            graph_connections: Some(connections),
            message_buffer_size: None
        };

        infer_pipeline_io_from_config_all(&mut config);
        assert_eq!(config.pipeline_configs.get(&0).unwrap().output_ops.as_ref().unwrap()[0], 3);
        assert_eq!(config.pipeline_configs.get(&1).unwrap().output_ops.as_ref().unwrap()[0], 2);
        assert_eq!(config.pipeline_configs.get(&2).unwrap().required_input_ops.as_ref().unwrap()[0], 2);
        assert_eq!(config.pipeline_configs.get(&2).unwrap().required_input_ops.as_ref().unwrap()[1], 3);
    }

    #[test]
    fn simple_inference() {
        let pipeline_0_config = PipelineConfigGUID {
            pipeline_index: 0,
            assigned_ops: vec![1, 3],
            required_input_ops: None,
            output_ops: None,
            worker_addrs: vec![String::from("127.0.0.1:5000"), String::from("127.0.0.1:5001")],
            relay_addrs: vec![String::from("127.0.0.1:6000")],
            relay_load_balance_weights: None,
            input_pipelines: vec![],
            output_pipelines: vec![2],
            builder_configs: HashMap::new(),
            operator_configs: HashMap::new(),
            request_rates: HashMap::new(),
            source_operators: HashSet::new(),
        };
    
        let pipeline_1_config = PipelineConfigGUID {
            pipeline_index: 1,
            assigned_ops: vec![0, 2],
            required_input_ops: None,
            output_ops: None,
            worker_addrs: vec![String::from("127.0.0.1:5100")],
            relay_addrs: vec![String::from("127.0.0.1:6100")],
            relay_load_balance_weights: None,
            input_pipelines: vec![],
            output_pipelines: vec![2],
            builder_configs: HashMap::new(),
            operator_configs: HashMap::new(),
            request_rates: HashMap::new(),
            source_operators: HashSet::new(),
        };
    
        let pipeline_2_config = PipelineConfigGUID {
            pipeline_index: 2,
            assigned_ops: vec![4, 5, 6],
            required_input_ops: None,
            output_ops: None,
            worker_addrs: vec![String::from("127.0.0.1:5200")],
            relay_addrs: vec![String::from("127.0.0.1:6200"), String::from("127.0.0.1:6201")],
            relay_load_balance_weights: None,
            input_pipelines: vec![0, 1],
            output_pipelines: vec![],
            builder_configs: HashMap::new(),
            operator_configs: HashMap::new(),
            request_rates: HashMap::new(),
            source_operators: HashSet::new(),
        };


        let config = HashMap::from_iter([
            (0, pipeline_0_config),
            (1, pipeline_1_config),
            (2, pipeline_2_config)
        ]);

        let connections = GraphConnections {
            outgoing_edges: HashMap::from_iter([
                (0, vec![2]),
                (1, vec![3]),
                (2, vec![4]),
                (3, vec![4]),
                (4, vec![5]),
                (5, vec![6]),
                (6, vec![])
            ]),
            incoming_edges: HashMap::from_iter([
                (0, vec![]),
                (1, vec![]),
                (2, vec![0]),
                (3, vec![1]),
                (4, vec![2, 3]),
                (5, vec![4]),
                (6, vec![5])
            ])
        }; 

        let mut config = ExecutionConfigGUID { 
            pipeline_configs: config,
            op_name_guid_mapping: None, 
            graph_connections: Some(connections),
            message_buffer_size: None,
        };
        
        infer_pipeline_io_from_config(&mut config, 2);
        infer_pipeline_io_from_config(&mut config, 1);

        assert_eq!(config.pipeline_configs.get(&1).unwrap().output_ops.as_ref().unwrap()[0], 2);
        assert_eq!(config.pipeline_configs.get(&2).unwrap().required_input_ops.as_ref().unwrap()[0], 2);
        assert_eq!(config.pipeline_configs.get(&2).unwrap().required_input_ops.as_ref().unwrap()[1], 3);
    }
}