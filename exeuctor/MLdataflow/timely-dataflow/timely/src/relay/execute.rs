//! Execute relay node from config or user defined function (UDF)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;
use weighted_rs::{SmoothWeight, Weight};

use timely_communication::{Message, Pull, Push};

use crate::communication::{relay_initialize, RelayNodeConfig as RelayNodeCommConfig};
use crate::communication::allocator::relay::{InputRelayAllocate, OutputRelayAllocate};
use crate::communication::allocator::relay::relay_allocator::{InputRelayWorkerAllocator, OutputRelayWorkerAllocator};
use crate::progress::Timestamp;
use crate::relay::{InputToWorkerExchangePattern, RelayConfig, RelayToOutputExchangePattern};
use crate::relay::registry::{InputRelayRegistry, OutputRelayRegistry};

pub fn execute_from_config(config: RelayConfig) {
    let comm_config = config.comm_config;
    let input_index_mapping = config.input_index_mapping;
    let input_exchange_pattern = config.input_to_worker_exchange_patterns;

    let mut input_indices = HashMap::new();
    let mut output_indices = HashSet::new();
    for input_index_map in input_index_mapping.iter() {
        for (_, input_idx) in input_index_map.iter() {
            if !input_indices.contains_key(input_idx) {
                input_indices.insert(*input_idx, 0i32);
            }
            *input_indices.get_mut(input_idx).unwrap() += 1;
        }
    }
    for output_idx in config.required_outputs.iter().flatten() {
        output_indices.insert(*output_idx);
    };
    let total_num_inputs = input_indices.len();
    let total_num_outputs = output_indices.len();


    // the logic of the input relay worker
    // execute in one thread for each input pipeline
    let input_relay_worker = move |mut allocator: InputRelayWorkerAllocator| {
        // for each current pipeline input, how many input pipelines it get from?
        let input_acquire_count = input_indices.clone();
        // get current input pipeline index
        let pipeline_index = allocator.pipeline_index();
        // obtain the mapping from the output index of the input pipeline connected to
        // to the input index of the current pipeline
        let pipeline_input_index_mapping = &input_index_mapping[pipeline_index];
        // for each input, we can define how should we distribute the received data to the timely workers
        let pipeline_input_exchange_pattern = &input_exchange_pattern;
        // number of scope inputs we can get from this input pipeline
        let num_inputs_current_input_pipeline = pipeline_input_index_mapping.len();
        // pullers to pull data (Bundle<T, D>) from the relay nodes in the input pipeline
        // one for each scope input (acquired from the input pipeline connected to)
        let mut data_pullers = Vec::with_capacity(num_inputs_current_input_pipeline);
        // pullers to pull frontier changes from the relay nodes in the input pipeline
        // one for each scope input (acquired from the input pipeline connected to)
        let mut frontier_pullers = Vec::with_capacity(num_inputs_current_input_pipeline);
        // pushers to push data (Bundle<T, D>) to timely workers
        let mut data_pushers = Vec::with_capacity(num_inputs_current_input_pipeline);
        // pushers to push frontier changes for EACH of the scope input to timely workers
        let mut frontier_pushers = Vec::with_capacity(num_inputs_current_input_pipeline);
        // pushers to push initial frontier changes reception mark for EACH of the scope input to timely workers
        let mut frontier_init_pushers = Vec::with_capacity(num_inputs_current_input_pipeline);
        // mark whether the frontier init mark has been sent
        let mut first_frontier_received = Vec::with_capacity(num_inputs_current_input_pipeline);
        // each input relay worker connects to an input pipeline
        // and is in charge of some of the scope inputs of the current pipeline
        let mut input_indices = Vec::with_capacity(num_inputs_current_input_pipeline);
        // for the Balance exchange pattern, for each of the input, we maintain a counter
        // to distribute it evenly among the timely workers (threads)
        let mut send_counter = HashMap::with_capacity(num_inputs_current_input_pipeline);

        // allocate channels
        for (output_index, input_index) in pipeline_input_index_mapping.iter() {
            let frontier_puller = allocator.allocate_input_pipeline_bytes_puller(2 * output_index);
            let data_puller = allocator.allocate_input_pipeline_bytes_puller(2 * output_index + 1);
            let to_timely_frontier_pushers = allocator.allocate_timely_workers_bytes_pushers(2 * input_index);
            let to_timely_data_pushers = allocator.allocate_timely_workers_bytes_pushers(2 * input_index + 1);
            let mut to_timely_frontier_init_pushers = allocator.allocate_timely_workers_pushers(2 * (total_num_inputs + total_num_outputs) + input_index);

            data_pullers.push(data_puller);
            frontier_pullers.push(frontier_puller);
            data_pushers.push(to_timely_data_pushers);
            frontier_pushers.push(to_timely_frontier_pushers);
            for pusher in to_timely_frontier_init_pushers.iter_mut() {
                pusher.send_with_latency_passthrough(Message::from_typed(*input_acquire_count.get(input_index).unwrap()), 0);
                pusher.done();
            }
            frontier_init_pushers.push(to_timely_frontier_init_pushers);
            first_frontier_received.push(false);
            input_indices.push(input_index.to_owned());
            send_counter.insert(input_index.to_owned(), 0usize);
        }

        allocator.finish_channel_allocation();
        let num_timely_workers = allocator.get_num_timely_workers();
        let num_peer_relay_nodes = allocator.get_num_relay_nodes_peers();
        let relay_index = allocator.relay_index();
        let direct_pass_flag = num_peer_relay_nodes == num_timely_workers;

        let mut rng = thread_rng();
        let uniform_dist = Uniform::new(0, num_timely_workers);

        let mut active = true;
        while active {
            // Exit loop if all input relay nodes has shutdown
            active = !allocator.input_relay_completed();
            allocator.receive_pipeline_input();
            allocator.events().borrow_mut().drain(..);

            // broadcast frontier changes for each of the scope input obtained from this input pipeline
            for (((puller, pushers), init_pushers), mark) in
            frontier_pullers.iter_mut().zip(frontier_pushers.iter_mut()).zip(frontier_init_pushers.iter_mut()).zip(first_frontier_received.iter_mut()) {
                while let Some((element, latency)) = puller.recv_with_transmission_latency() {
                    let element = &mut Some(element);
                    // we should broadcast frontier updates to every timely worker in the current pipeline
                    for pusher in pushers.iter_mut() {
                        pusher.push_with_latency_passthrough(element, Some(latency));
                    }

                    if !*mark {
                        for init_pusher in init_pushers.iter_mut() {
                            init_pusher.send_with_latency_passthrough(Message::from_typed(-1i32), 0);
                        }
                        for init_pusher in init_pushers.iter_mut() { init_pusher.done() }
                        *mark = true;
                    }
                }
                // flush the pushers
                for pusher in pushers.iter_mut() { pusher.done() }
            }

            // receive and forward data for each scope input
            for (index, (puller, pushers)) in
            input_indices.iter().zip(data_pullers.iter_mut().zip(data_pushers.iter_mut())) {
                while let Some((element, latency)) = puller.recv_with_transmission_latency() {
                    let element = &mut Some(element);
                    // distribute the data (Bundle<T, D>) according to the user defined pattern
                    // for this scope input
                    let exchange_pattern = match pipeline_input_exchange_pattern {
                        Some(patterns) => *patterns.get(index).unwrap(),
                        None => InputToWorkerExchangePattern::Balance
                    };
                    match exchange_pattern {
                        InputToWorkerExchangePattern::Random => {
                            let worker_to_send = uniform_dist.sample(&mut rng);
                            let pusher = &mut pushers[worker_to_send];
                            pusher.push_with_latency_passthrough(element, Some(latency));
                        },
                        InputToWorkerExchangePattern::Balance => {
                            // TODO: balance between timely worker process
                            // i.e., distribute to different timely worker processes between two consecutive calls
                            if direct_pass_flag {
                                let pusher = &mut pushers[relay_index];
                                pusher.push_with_latency_passthrough(element, Some(latency));
                            }
                            else {
                                let count = send_counter.get_mut(index).unwrap();
                                let pusher = &mut pushers[*count];
                                *count = (*count + 1) % num_timely_workers;
                                pusher.push_with_latency_passthrough(element, Some(latency));
                            }
                        },
                        InputToWorkerExchangePattern::Broadcast => {
                            for pusher in pushers.iter_mut() {
                                pusher.push_with_latency_passthrough(element, Some(latency));
                            }
                        }
                    }
                }
                // TODO: only flush the pushers that actually have data sent
                // flush the pushers
                for pusher in pushers.iter_mut() { pusher.done() }
            }

            if active {
                // Wait for the messages from the relay nodes in the input pipeline
                allocator.await_events(Some(std::time::Duration::from_millis(1000)));
            }
        }
    };

    let required_outputs = config.required_outputs;
    let output_exchange_pattern = config.relay_to_output_exchange_pattern;
    let output_pipelines_relay_load_ratios = config.output_pipelines_relay_load_balance_ratios;

    // the logic of the output relay worker
    // execute in a separate thread for each output pipeline
    let output_relay_worker = move |mut allocator: OutputRelayWorkerAllocator| {
        // get current output pipeline index
        let pipeline_index = allocator.pipeline_index();
        // indices of scope outputs that are required by this output pipeline
        let required_outputs_indices = &required_outputs[pipeline_index];
        // how should we distribute output data and frontier changes
        // to the relay nodes in the output pipeline?
        let output_exchange_pattern = &output_exchange_pattern;
        // the number of scope outputs required by this output pipeline
        let num_outputs_current_output_pipeline = required_outputs_indices.len();
        // pullers to pull data (Bundle<T, D>) from timely workers
        // one for each required scope output
        let mut data_pullers = Vec::with_capacity(num_outputs_current_output_pipeline);
        // pullers to pull frontier changes from timely workers
        // one for each required scope output
        let mut frontier_pullers = Vec::with_capacity(num_outputs_current_output_pipeline);
        // pushers to push data (Bundle<T, D>) to relay nodes in the output pipeline
        let mut data_pushers = Vec::with_capacity(num_outputs_current_output_pipeline);
        // pushers to push frontier changes for EACH of the required scope output
        // to the relay nodes in the output pipeline
        let mut frontier_pushers = Vec::with_capacity(num_outputs_current_output_pipeline);
        // for the Balance exchange pattern, for each of the required scope output, we maintain a counter
        // to distribute the frontier updates evenly among the output pipeline relay nodes
        let mut frontier_send_counter = Vec::with_capacity(num_outputs_current_output_pipeline);
        // load balancing
        let mut smooth_wrr_load_balancer: Vec<SmoothWeight<usize>> = Vec::new();

        let output_exchange_pattern = match output_exchange_pattern {
            Some(pattern) => *pattern,
            None => RelayToOutputExchangePattern::Balance
        };

        let num_relay_nodes = allocator.get_num_output_relay_nodes();


        for scope_output_index in required_outputs_indices.iter() {
            let frontier_puller = allocator.allocate_timely_workers_bytes_puller(2 * (total_num_inputs + scope_output_index));
            let data_puller = allocator.allocate_timely_workers_bytes_puller(2 * (total_num_inputs + scope_output_index) + 1);
            let to_output_relay_frontier_pushers = allocator.allocate_output_pipeline_bytes_pushers(2 * scope_output_index);
            let to_output_relay_data_pushers = allocator.allocate_output_pipeline_bytes_pushers(2 * scope_output_index + 1);

            data_pullers.push(data_puller);
            frontier_pullers.push(frontier_puller);
            data_pushers.push(to_output_relay_data_pushers);
            frontier_pushers.push(to_output_relay_frontier_pushers);
            frontier_send_counter.push(0usize);
            let mut output_lb = SmoothWeight::new();
            if let RelayToOutputExchangePattern::Balance = output_exchange_pattern {
                if let Some(ratios) = output_pipelines_relay_load_ratios.get(&pipeline_index) {
                    assert!(ratios.len() == num_relay_nodes, "weight for each of the output relay server must be provided");
                    let total_sum = ratios.iter().sum::<f64>();
                    let weights = ratios.iter().map(|x| std::cmp::max((*x * 100.0_f64 / total_sum).round() as isize, 1isize)).collect::<Vec<_>>();
                    for (idx, weight) in weights.into_iter().enumerate() {
                        output_lb.add(idx, weight);
                    }
                }
                else {
                    for idx in 0 .. num_relay_nodes {
                        output_lb.add(idx, 1);
                    }
                }
            }
            smooth_wrr_load_balancer.push(output_lb);
        }
        allocator.finish_channel_allocation();

        let mut rng = thread_rng();
        let uniform_dist = Uniform::new(0, num_relay_nodes);

        let mut active = true;
        while active {
            // Exit loop when all timely workers has completed
            active = !allocator.timely_workers_completed();
            allocator.receive_from_timely_workers();
            allocator.events().borrow_mut().drain(..);

            // send frontier changes
            for (idx, (puller, pushers)) in
            frontier_pullers.iter_mut().zip(frontier_pushers.iter_mut()).enumerate() {
                while let Some(element) = puller.recv() {
                    match output_exchange_pattern {
                        RelayToOutputExchangePattern::Random => {
                            let relay_node_to_send = uniform_dist.sample(&mut rng);
                            let pusher = &mut pushers[relay_node_to_send];
                            pusher.send(element);
                        },
                        RelayToOutputExchangePattern::Balance => {
                            let pusher = &mut pushers[frontier_send_counter[idx]];
                            // increment send count
                            frontier_send_counter[idx] = (frontier_send_counter[idx] + 1) % num_relay_nodes;
                            pusher.send(element);
                        },
                    }
                }
                // TODO: only flush the pushers that actually have data sent
                // flush the pushers
                for pusher in pushers.iter_mut() { pusher.done() }
            }

            // send data
            for (idx, (puller, pushers)) in
            data_pullers.iter_mut().zip(data_pushers.iter_mut()).enumerate() {
                while let Some(element) = puller.recv() {
                    match output_exchange_pattern {
                        RelayToOutputExchangePattern::Random => {
                            let relay_node_to_send = uniform_dist.sample(&mut rng);
                            let pusher = &mut pushers[relay_node_to_send];
                            pusher.send(element);
                        },
                        RelayToOutputExchangePattern::Balance => {
                            let target_idx = smooth_wrr_load_balancer[idx].next().unwrap();
                            let pusher = &mut pushers[target_idx];
                            pusher.send(element);
                        },
                    }
                }
                // TODO: only flush the pushers that actually have data sent
                // flush the pushers
                for pusher in pushers.iter_mut() { pusher.done() }
            }

            if active {
                // Wait for the messages from the relay nodes in the input pipeline
                allocator.await_events(Some(std::time::Duration::from_millis(1000)));
            }
        }
    };

    relay_initialize(comm_config, input_relay_worker, output_relay_worker).unwrap();
}

/// Execute from UDF functions that take in InputRelayRegistry and OutputRelayRegistry
/// to manually register inputs and outputs for each input and output pipeline
/// VERY IMPORTANT!
/// NOTE: execute_from_udf() assumes that timely workers send/recv frontier changes through a single channel
/// i.e., channel 0.
#[allow(dead_code)]
pub fn execute_from_udf<T, F1, F2>(comm_config: RelayNodeCommConfig, func_input_pipeline: F1, func_output_pipeline: F2)
where
    T: Timestamp + Send,
    F1: Fn(&mut InputRelayRegistry<T, InputRelayWorkerAllocator>) -> () + Send + Sync + 'static,
    F2: Fn(&mut OutputRelayRegistry<T, OutputRelayWorkerAllocator>) -> () + Send + Sync + 'static,
{
    let func_input_pipeline = Arc::new(func_input_pipeline);
    let input_relay_worker = move |mut allocator: InputRelayWorkerAllocator| {
        let mut registry = InputRelayRegistry::new(&mut allocator);
        let func_input_pipeline = func_input_pipeline.clone();
        (*func_input_pipeline)(&mut registry);

        let mut data_connectors = registry.data_connectors;
        let mut frontier_connector = registry.frontier_connector.unwrap();

        let mut active = false;
        while active {
            active = !allocator.input_relay_completed();
            allocator.receive_pipeline_input();
            allocator.events().borrow_mut().drain(..);

            frontier_connector.relay_frontier();
            for connector in data_connectors.iter_mut() {
                connector.relay_data();
            }

            if active {
                allocator.await_events(None);
            }
        }
    };

    let func_output_pipeline = Arc::new(func_output_pipeline);

    let output_relay_worker = move |mut allocator: OutputRelayWorkerAllocator| {
        let mut registry = OutputRelayRegistry::new(&mut allocator);
        let func_output_pipeline = func_output_pipeline.clone();
        (*func_output_pipeline)(&mut registry);

        let mut data_connectors = registry.data_connectors;
        let mut frontier_connector = registry.frontier_connector.unwrap();

        let mut active = false;
        while active {
            active = !allocator.timely_workers_completed();
            allocator.receive_from_timely_workers();
            allocator.events().borrow_mut().drain(..);

            frontier_connector.relay_frontier();
            for connector in data_connectors.iter_mut() {
                connector.relay_data();
            }

            if active {
                allocator.await_events(Some(std::time::Duration::from_millis(1000)));
            }
        }
    };

    relay_initialize(comm_config, input_relay_worker, output_relay_worker).unwrap();
}

