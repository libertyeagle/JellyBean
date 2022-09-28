use std::collections::{HashMap, HashSet, BTreeMap};
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::CommunicationWithRelayConfig;
use timely::WorkerConfig;

use timely::relay::{RelayConfig, RelayToOutputExchangePattern};
use timely::communication::RelayNodeConfig as RelayNodeCommConfig;
use timely::relay::execute_from_config as pipeline_relay_execute_from_config;

use crate::builder::PipelineGraphBuilder;
use crate::config::{ExecutionConfig, ExecutionConfigGUID};
use crate::graph::GraphNode;
use crate::input::GenericScope;
use crate::metrics::{MetricsLogger, OperatorMetricsStats};
use crate::node::GenericPipelineScope;
use crate::static_timely::timely_static_pipeline_execute::execute as timely_pipeline_execute; 
use crate::static_timely::timely_static_pipeline_execute::Config as TimelyPipelineConfig;


pub fn pipeline_worker_execute<T, F>(dag_builder: F, config: &ExecutionConfig, pipeline_index: usize, worker_index: usize)
where
    T: Timestamp + Refines<()> + TotalOrder,
    F: Fn(&mut PipelineGraphBuilder<T>) + Send + Sync + 'static
{
    let op_name_guid_mapping = config.op_name_guid_mapping.clone();
    let metrics_logging_dir = config.metrics_logging_dir.clone();
    let config = config.to_guid();
    
    let loggers = pipeline_worker_execute_guid(dag_builder, &config, pipeline_index, worker_index);
    let throughput_loggers = loggers.throughput_loggers;
    let execution_latency_loggers = loggers.execution_latency_loggers;
    let edge_latency_loggers = loggers.edge_latency_loggers;
    let path_latency_loggers = loggers.path_latency_loggers;
    let jct_loggers = loggers.jct_loggers;

    let mut system_metrics = BTreeMap::new();
    let mut all_exec_latencies = BTreeMap::new();
    let mut all_path_latencies = BTreeMap::new();
    for (op_name, gid) in op_name_guid_mapping.iter() {
        let mut op_metrics = OperatorMetricsStats {
            throughput: None,
            overall_throughput: None,
            latency: None,
            operator_jct: None,
            path_jct: None
        };
        let mut latency_metrics = BTreeMap::new();
        if let Some(logger) = execution_latency_loggers.get(&gid) {
            let metrics = logger.compute_latency();
            if let Some(metrics) = metrics {
                latency_metrics.insert(String::from("operator_execution"), metrics);
            }
            all_exec_latencies.insert(op_name.clone(), logger.get_all_latencies());
        }
        if let Some(logger) = edge_latency_loggers.get(&gid) {
            let metrics = logger.compute_latency();
            if let Some(metrics) = metrics {
                latency_metrics.insert(String::from("dataflow_edge"), metrics);
            } 
        }
        if let Some(logger) = path_latency_loggers.get(&gid) {
            let metrics = logger.compute_latency();
            if let Some(metrics) = metrics {
                latency_metrics.insert(String::from("dataflow_path"), metrics);
            }
            all_path_latencies.insert(op_name.clone(), logger.get_all_latencies());
        }
        if !latency_metrics.is_empty() {
            op_metrics.latency = Some(latency_metrics);
        }

        if let Some(logger) = throughput_loggers.get(&gid) {
            let metrics = logger.compute_throughput();
            if let Some(metrics) = metrics {
                op_metrics.throughput = Some(metrics);
            }
            if let Some(overall_th) = logger.compute_overall_throughput() {
                op_metrics.overall_throughput = Some(overall_th);
            }
        }
        if let Some(logger) = jct_loggers.get(&gid) {
            let op_jct = logger.compute_operator_job_completion_time();
            if let Some(op_jct) = op_jct {
                op_metrics.operator_jct = Some(op_jct)
            }
            let path_jct = logger.compute_path_job_completion_time();
            if let Some(path_jct) = path_jct {
                op_metrics.path_jct = Some(path_jct)
            }
        }
        if op_metrics.throughput.is_some() || 
            op_metrics.overall_throughput.is_some() || 
            op_metrics.latency.is_some() || 
            op_metrics.operator_jct.is_some() ||
            op_metrics.path_jct.is_some()
        {
            system_metrics.insert(op_name.to_owned(), op_metrics);
        }
    }

    if let Some(logging_dir) = metrics_logging_dir {
        std::fs::create_dir_all(logging_dir.join("latency_logs")).unwrap();
        let metrics_logging_path = logging_dir.join(format!("performance_metrics_p{}_w{}.json", pipeline_index, worker_index));
        let exec_latency_logging_path = logging_dir.join(format!("latency_logs/exec_latencies_p{}_w{}.json", pipeline_index, worker_index));
        let path_latency_logging_path = logging_dir.join(format!("latency_logs/path_latencies_p{}_w{}.json", pipeline_index, worker_index));
        
        let f = File::create(metrics_logging_path).expect("Unable to create file");
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &system_metrics).unwrap();

        let f = File::create(exec_latency_logging_path).expect("Unable to create file");
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &all_exec_latencies).unwrap();

        let f = File::create(path_latency_logging_path).expect("Unable to create file");
        let writer = BufWriter::new(f);
        serde_json::to_writer_pretty(writer, &all_path_latencies).unwrap();
    }
    
    let stats = serde_json::to_string_pretty(&system_metrics).unwrap();
    println!("=====METRICS======");
    println!("{}", stats);
}


pub fn pipeline_worker_execute_guid<T, F>(dag_builder: F, config: &ExecutionConfigGUID, pipeline_index: usize, worker_index: usize) -> MetricsLogger
where
    T: Timestamp + Refines<()> + TotalOrder,
    F: Fn(&mut PipelineGraphBuilder<T>) + Send + Sync + 'static
{
    let current_pipeline_config = config.pipeline_configs.get(&pipeline_index).unwrap();
    let message_buffer_size = config.message_buffer_size;
    let op_name_guid_mapping = config.op_name_guid_mapping.clone();

    let mut required_inputs = current_pipeline_config.required_input_ops.as_ref().unwrap().clone();
    required_inputs.sort();
    let mut register_outputs = current_pipeline_config.output_ops.as_ref().unwrap().clone();
    register_outputs.sort();
    let mut current_pipeline_nodes = current_pipeline_config.assigned_ops.clone();
    current_pipeline_nodes.sort();

    let builder_configs = current_pipeline_config.builder_configs.clone();
    let operator_configs = current_pipeline_config.operator_configs.clone();
    let source_operators = current_pipeline_config.source_operators.clone();
    let request_rates = current_pipeline_config.request_rates.clone();
    let worker_addrs = current_pipeline_config.worker_addrs.clone();
    let relay_addrs = current_pipeline_config.relay_addrs.clone();

    let comm_config = if worker_addrs.len() == 1 {
        CommunicationWithRelayConfig::Process {
            threads: 1,
            relay_addresses: relay_addrs,
            report: true,
            relay_log_fn: Box::new(|_| None)
        }
    }
    else {
        CommunicationWithRelayConfig::Cluster {
            threads: 1,
            process: worker_index,
            worker_addresses: worker_addrs,
            relay_addresses: relay_addrs,
            report: true,
            worker_log_fn: Box::new(|_| None),
            relay_log_fn: Box::new(|_| None)
        }
    };    

    let mut config = TimelyPipelineConfig {
        communication: comm_config,
        worker: WorkerConfig::default()
    };

    if let Some(msg_buffer_size) = message_buffer_size {
        config.worker.set(String::from("message_buffer_size"), msg_buffer_size);
    }

    let guards = timely_pipeline_execute(config, move |worker| {
        let current_pipeline_nodes_names = match &op_name_guid_mapping {
            Some(mapping) => {
                let mut reverse_mapping = HashMap::with_capacity(mapping.len());
                for (name, index) in mapping.iter() {
                    reverse_mapping.insert(*index, name);
                }
                let nodes_names = current_pipeline_nodes.iter().map(|x| (*reverse_mapping.get(x).unwrap()).clone()).collect::<Vec<_>>();
                Some(nodes_names)
            },
            None => None
        };
        let worker_index = worker.index();
        let peers = worker.peers();
        let mut builder = PipelineGraphBuilder::new(
            pipeline_index,
            worker_index,
            peers,
            current_pipeline_nodes_names,
            builder_configs.clone()
        );
        (dag_builder)(&mut builder);
        let mut graph = builder.get_graph();

        let op_name_local_id_mapping = &graph.op_name_local_id_mapping;
        let mut op_lid_guid_mapping = HashMap::with_capacity(op_name_local_id_mapping.len());
        let mut op_guid_lid_mapping = HashMap::with_capacity(op_name_local_id_mapping.len());
        if op_name_guid_mapping.is_none() {
            for (_, lid) in op_name_local_id_mapping.iter() {
                op_lid_guid_mapping.insert(*lid, *lid);
                op_guid_lid_mapping.insert(*lid, *lid);
            }     
        }
        else {
            for (op_name, lid) in op_name_local_id_mapping.iter() {
                let gid = op_name_guid_mapping.as_ref().unwrap().get(op_name).expect(&*format!("could not find operator {}", op_name));
                op_lid_guid_mapping.insert(*lid, *gid);
                op_guid_lid_mapping.insert(*gid, *lid);
            }
        }

        let mut current_pipeline_nodes_with_lid = current_pipeline_nodes.iter().map(
            |gid| (*gid, *op_guid_lid_mapping.get(gid).expect("could not find operator in the graph"))
        ).collect::<Vec<_>>();

        current_pipeline_nodes_with_lid.sort_by_key(|(_gid, lid)| *lid);

        worker.pipeline_dataflow::<T, _, _>(|scope| {
            let mut streams = HashMap::new();
            // node_index is GUID
            for (input_index, node_index) in required_inputs.iter().enumerate() {
                let local_node_index = op_guid_lid_mapping.get(&node_index).expect("could not find input operator in the graph");
                let node = graph.operators.get_mut(local_node_index).unwrap();
                match node {
                    GraphNode::ExchangeComputeNode(node) => {
                        let stream = node.acquire_from_input_pipeline(scope as &mut dyn GenericPipelineScope, input_index);
                        streams.insert(*node_index, stream);
                    },
                    GraphNode::ExchangeInputNode(node) => {
                        let stream = node.acquire_from_input_pipeline(scope as &mut dyn GenericPipelineScope, input_index);
                        streams.insert(*node_index, stream);
                    }
                    _ => {
                        panic!{"pipeline input operators must be exchangeable operators"};
                    },
                }
            }

            for (node_index, local_node_index) in current_pipeline_nodes_with_lid.iter() {                
                let node = graph.operators.get_mut(local_node_index).expect("opeartor does not exist");
                let mut op_config = operator_configs.get(&node_index).map(|x| x.clone());
                if source_operators.contains(&node_index) {
                    if let Some(config) = &mut op_config {
                        config.insert(String::from("reset_timestamp"), Arc::new(true));
                    }
                    else {
                        let mut config: HashMap<String, Arc<dyn std::any::Any + Send + Sync>> = HashMap::new();
                        config.insert(String::from("reset_timestamp"), Arc::new(true));
                        op_config = Some(config);
                    }
                }
                match node {
                    GraphNode::LocalInputNode(node) => {
                        if let Some(request_rate) = request_rates.get(&node_index).map(|x| *x) {
                            if let Some(config) = &mut op_config {
                                config.insert(String::from("request_rate"), Arc::new(request_rate));
                            }
                            else {
                                let mut config: HashMap<String, Arc<dyn std::any::Any + Send + Sync>> = HashMap::new();
                                config.insert(String::from("request_rate"), Arc::new(request_rate));
                                op_config = Some(config);
                            }
                        }
                        let stream = node.build_stream(scope as &mut dyn GenericScope, op_config);
                        streams.insert(*node_index, stream);
                    },
                    GraphNode::ExchangeInputNode(node) => {
                        if let Some(request_rate) = request_rates.get(&node_index).map(|x| *x) {
                            if let Some(config) = &mut op_config {
                                    config.insert(String::from("request_rate"), Arc::new(request_rate));
                            }
                            else {
                                let mut config: HashMap<String, Arc<dyn std::any::Any + Send + Sync>> = HashMap::new();
                                config.insert(String::from("request_rate"), Arc::new(request_rate));
                                op_config = Some(config);
                            }
                        }
                        let stream = node.build_stream(scope as &mut dyn GenericScope, op_config);
                        if let Some(output_index) = register_outputs.iter().position(|x| x == node_index) {
                            node.register_pipeline_output(&stream, scope as &mut dyn GenericPipelineScope, output_index);
                        }
                        streams.insert(*node_index, stream);
                    },
                    GraphNode::LocalComputeNode(node) => {
                        let prev_indices = node.required_prev_nodes();
                        let prev_indices = prev_indices.into_iter().map(
                            |x| *op_lid_guid_mapping.get(&x).unwrap()
                        ).collect::<Vec<_>>();
                        let mut prev_nodes = Vec::with_capacity(prev_indices.len());
                        for prev_idx in prev_indices.iter() {
                            prev_nodes.push(streams.get(prev_idx).unwrap());
                        }
                        let stream = node.build(&prev_nodes[..], op_config);
                        streams.insert(*node_index, stream);
                    },
                    GraphNode::ExchangeComputeNode(node) => {
                        let prev_indices = node.required_prev_nodes();
                        let prev_indices = prev_indices.into_iter().map(
                            |x| *op_lid_guid_mapping.get(&x).unwrap()
                        ).collect::<Vec<_>>();
                        let mut prev_nodes = Vec::with_capacity(prev_indices.len());
                        for prev_idx in prev_indices.iter() {
                            prev_nodes.push(streams.get(prev_idx).unwrap());
                        }
                        let stream = node.build(&prev_nodes[..], op_config);
                        if let Some(output_index) = register_outputs.iter().position(|x| x == node_index) {
                            node.register_pipeline_output(&stream, scope as &mut dyn GenericPipelineScope, output_index);
                        }
                        streams.insert(*node_index, stream);
                    },
                }
            }
        });


        let mut throughput_loggers = HashMap::new();
        let mut execution_latency_loggers = HashMap::new();
        let mut edge_latency_loggers = HashMap::new();
        let mut path_latency_loggers = HashMap::new();
        let mut jct_loggers = HashMap::new();

        for (node_gid, node_lid) in current_pipeline_nodes_with_lid.iter() {
            let node = graph.operators.get_mut(node_lid).expect("opeartor does not exist");
            match node {
                GraphNode::LocalComputeNode(node) => {
                    if let Some(logger) = node.get_throughput_logger() {
                        throughput_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_flow_compute_latency_logger() {
                        execution_latency_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_flow_edge_latency_logger() {
                        edge_latency_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_flow_path_latency_logger() {
                        path_latency_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_jct_logger() {
                        jct_loggers.insert(*node_gid, logger);
                    }
                },
                GraphNode::ExchangeComputeNode(node) => {
                    if let Some(logger) = node.get_throughput_logger() {
                        throughput_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_flow_compute_latency_logger() {
                        execution_latency_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_flow_edge_latency_logger() {
                        edge_latency_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_flow_path_latency_logger() {
                        path_latency_loggers.insert(*node_gid, logger);
                    }
                    if let Some(logger) = node.get_jct_logger() {
                        jct_loggers.insert(*node_gid, logger);
                    }
                }
                _ => {}
            }
        }

        let metrics_loggers = MetricsLogger {
            throughput_loggers,
            execution_latency_loggers,
            edge_latency_loggers,
            path_latency_loggers,
            jct_loggers,
        };

        let current_pipeline_nodes_lid = current_pipeline_nodes_with_lid.into_iter().map(
            |(_gid, lid)| lid
        ).collect::<HashSet<_>>();

        let mut completed = false; 
        while !completed {
            completed = true;
            for index in graph.input_operator_indices.iter() {
                if current_pipeline_nodes_lid.contains(&index) {
                    let input_node = graph.operators.get_mut(index).unwrap();
                    if let GraphNode::LocalInputNode(input_node) = input_node {
                        if input_node.step() {
                            completed = false;
                        }
                    }
                    else if let GraphNode::ExchangeInputNode(input_node) = input_node {
                        if input_node.step() {
                            completed = false;
                        }
                    }
                    else {
                        panic!("node {} should be an input node", index);
                    }
                }
            }
            worker.step();
        }

        metrics_loggers
    }).unwrap();

    let loggers = guards.join();
    let mut loggers = loggers.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>();
    assert_eq!(loggers.len(), 1);
    let loggers = loggers.pop().unwrap();
    loggers
}


pub fn pipeline_relay_execute(config: &ExecutionConfig, pipeline_index: usize, relay_node_index: usize) {
    let config = config.to_guid();
    pipeline_relay_execute_guid(&config, pipeline_index, relay_node_index);
}

pub fn pipeline_relay_execute_guid(config: &ExecutionConfigGUID, pipeline_index: usize, relay_node_index: usize) {
    let current_pipeline_config = config.pipeline_configs.get(&pipeline_index).unwrap();
    let relay_addrs = current_pipeline_config.relay_addrs.clone();
    let worker_addrs = current_pipeline_config.worker_addrs.clone();
    
    let mut required_input_ops = current_pipeline_config.required_input_ops.as_ref().unwrap().clone();
    required_input_ops.sort();
    let mut register_output_ops = current_pipeline_config.output_ops.as_ref().unwrap().clone();
    register_output_ops.sort();
    let num_input_pipelines = current_pipeline_config.input_pipelines.len();
    let mut input_pipelines_output_ops = Vec::with_capacity(num_input_pipelines);
    let mut input_pipelines_relay_addrs = Vec::with_capacity(num_input_pipelines);
    for pipeline_idx in current_pipeline_config.input_pipelines.iter() {
        let input_pipeline_config = config.pipeline_configs.get(&pipeline_idx).unwrap();
        let mut output_ops = input_pipeline_config.output_ops.as_ref().unwrap().clone();
        output_ops.sort();
        input_pipelines_output_ops.push(output_ops);
        let relay_addrs = input_pipeline_config.relay_addrs.clone();
        input_pipelines_relay_addrs.push(relay_addrs);
    }
    let mut input_index_mappings = vec![HashMap::new(); num_input_pipelines];
    for (input_index, node_idx) in required_input_ops.iter().enumerate() {
        let mut mapped = false;
        for (pipeline_index, output_nodes) in input_pipelines_output_ops.iter().enumerate() {
            let output_index = output_nodes.iter().position(|x| *x == *node_idx);
            if let Some(output_index) = output_index {
                input_index_mappings[pipeline_index].insert(output_index, input_index);
                mapped = true;
                break
            }
        }
        assert_eq!(mapped, true);
    }

    let mut output_pipelines_required_ops = Vec::with_capacity(current_pipeline_config.output_pipelines.len());
    let mut output_pipelines_relay_addrs = Vec::with_capacity(current_pipeline_config.output_pipelines.len());
    let mut output_pipelines_relay_load_balance_weights = Vec::with_capacity(current_pipeline_config.output_pipelines.len());
    for pipeline_index in current_pipeline_config.output_pipelines.iter() {
        let output_pipeline_config = config.pipeline_configs.get(pipeline_index).unwrap();
        let relay_addrs = output_pipeline_config.relay_addrs.clone();
        let relay_lb_weights = output_pipeline_config.relay_load_balance_weights.clone();
        if let Some(ratios) = &relay_lb_weights {
            assert_eq!(ratios.len(),  relay_addrs.len(), "load balance ratios must be provided for each relay node in pipeline@{}", pipeline_index);
        }
        output_pipelines_relay_addrs.push(relay_addrs);
        output_pipelines_relay_load_balance_weights.push(relay_lb_weights);
        let mut required_inputs = output_pipeline_config.required_input_ops.as_ref().unwrap().clone();
        required_inputs.sort();
        let mut output_indices = Vec::new();
        for node_idx in required_inputs {
            let output_idx = register_output_ops.iter().position(|x| *x == node_idx);
            if let Some(output_idx) = output_idx {
                output_indices.push(output_idx);
            }
        }
        output_pipelines_required_ops.push(output_indices);
    }

    let num_relays = relay_addrs.len();
    let comm_config = RelayNodeCommConfig {
        input_relay_nodes_addresses: input_pipelines_relay_addrs,
        output_relay_nodes_addresses: output_pipelines_relay_addrs,
        timely_workers_addresses: worker_addrs,
        threads_per_timely_worker_process: 1,
        my_addr: relay_addrs[relay_node_index].clone(),
        my_index: relay_node_index,
        num_relay_nodes_peers: num_relays,
        report: true,
        relay_log_sender: Box::new(|_| None),
        timely_log_sender: Box::new(|_| None),
    };
    
    let mut output_pipelines_load_balanced_ratios_map = HashMap::new();
    for (output_pipeline_idx, lb_ratios) in output_pipelines_relay_load_balance_weights.into_iter().enumerate() {
        if let Some(ratios) = lb_ratios {
            output_pipelines_load_balanced_ratios_map.insert(output_pipeline_idx, ratios);
        }
    }

    let relay_config = RelayConfig {
        comm_config,
        input_index_mapping: input_index_mappings,
        required_outputs: output_pipelines_required_ops,
        input_to_worker_exchange_patterns: None,
        relay_to_output_exchange_pattern: Some(RelayToOutputExchangePattern::Balance),
        output_pipelines_relay_load_balance_ratios: output_pipelines_load_balanced_ratios_map
    };

    pipeline_relay_execute_from_config(relay_config);
}