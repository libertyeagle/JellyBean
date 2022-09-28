import os
import json
import pickle
import pandas as pd
import numpy as np


def read_inputs(data_dir):
    """Read inputs from dir
    Inputs files:
        workers_numbers.csv (optional)
        workers_specs.csv
        workers_workers_link.csv (optional)
        workers_partition.json (optional)
        logical_graph.csv
        throughput_normalization_ratios.csv
        execution_profile.csv
        message_sizes.csv
        sources_workers_link.csv (optional)
        input_injection_rates.csv
        accuracy_requirement.json
        nodes_accuracy_profiles.pkl
    """
    if os.path.isfile(os.path.join(data_dir, "workers_numbers.csv")):
        return read_inputs_with_worker_numbers(data_dir)
    else:
        return read_inputs_with_concrete_workers(data_dir)
        
def read_inputs_with_worker_numbers(data_dir):
    # workers_numbers.csv
    # worker_type, number
    workers_numbers = dict()
    workers_numbers_path = os.path.join(data_dir, "workers_numbers.csv")
    columns_names = ["worker_type", "number"]
    dtypes = {
        "worker_type": str,
        "number": int
    }
    workers_numbers_specs = pd.read_csv(
        workers_numbers_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in workers_numbers_specs.itertuples():
        workers_numbers[row.worker_type] = row.number
    
    # workers_specs.csv
    # worker_name, worker_cost
    workers_cost = dict()
    columns_names = ["worker_name", "worker_cost"]
    dtypes = {
        "worker_name": str,
        "worker_cost": float
    }
    workers_specs_path = os.path.join(data_dir, "workers_specs.csv")
    workers_specs = pd.read_csv(
        workers_specs_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in workers_specs.itertuples():
        for i in range(workers_numbers[row.worker_name]):
            worker_name = "{}_{:d}".format(row.worker_name, i)
            cost = row.worker_cost
            workers_cost[worker_name] = cost
    
    # workers_workers_link.csv
    # worker_a, worker_b, cost
    workers_workers_link_comm_cost = dict()
    workers_workers_link_path = os.path.join(data_dir, "workers_workers_link.csv")
    columns_names = ["worker_a", "worker_b", "cost"]
    dtypes = {
        "worker_a": str,
        "worker_b": str,
        "cost": float
    }
    if os.path.isfile(workers_workers_link_path):
        workers_workers_link = pd.read_csv(
            workers_workers_link_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in workers_workers_link.itertuples():
            worker_a = row.worker_a
            worker_b = row.worker_b
            for i in range(workers_numbers[worker_a]):
                for j in range(workers_numbers[worker_b]):
                    worker_a_concrete = "{}_{:d}".format(worker_a, i)
                    worker_b_concrete = "{}_{:d}".format(worker_b, j)            
                    workers_workers_link_comm_cost[(worker_a_concrete, worker_b_concrete)] = row.cost   

    # logical_graph.csv
    # u, v
    logical_graph_edges = []
    logical_graph_path = os.path.join(data_dir, "logical_graph.csv")
    columns_names = ["u", "v"]
    dtypes = {
        "u": str,
        "v": str
    }
    logical_graph = pd.read_csv(
        logical_graph_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in logical_graph.itertuples():
        logical_graph_edges.append((row.u, row.v))

    # throughput_normalization_ratios.csv
    # u, v, normalization_ratio
    throughput_normalization_ratios = dict()
    throughput_normalization_path = os.path.join(data_dir, "throughput_normalization_ratios.csv")
    columns_names = ["u", "v", "ratio"]
    dtypes = {
        "u": str,
        "v": str,
        "ratio": float
    }
    if os.path.isfile(throughput_normalization_path):
        throughput_normalization_specs = pd.read_csv(
            throughput_normalization_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in throughput_normalization_specs.itertuples():
            throughput_normalization_ratios[(row.u, row.v)] = row.ratio

    throughput_normalization_path = os.path.join(data_dir, "throughput_normalization_ratios_per_variant.csv")
    columns_names = ["u", "v", "u_model", "v_model", "ratio"]
    dtypes = {
        "u": str,
        "v": str,
        "u_model": str,
        "v_model": str,
        "ratio": float
    }
    if os.path.isfile(throughput_normalization_path):
        throughput_normalization_ratios = dict()
        throughput_normalization_specs = pd.read_csv(
            throughput_normalization_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in throughput_normalization_specs.itertuples():
            if (row.u, row.v) not in throughput_normalization_ratios:
                throughput_normalization_ratios[(row.u, row.v)] = dict()            
            throughput_normalization_ratios[(row.u, row.v)][(row.u_model, row.v_model)] = row.ratio

    # execution_profile.csv
    # node, worker, model_variant, latency
    # latency is in milliseconds
    nodes_workers_execution_profiles = dict()
    execution_profile_path = os.path.join(data_dir, "execution_profile.csv")
    columns_names = ["node", "worker", "model", "latency"]
    dtypes = {
        "node": str,
        "worker": str,
        "model": str,
        "latency": float
    }
    execution_profile_specs = pd.read_csv(
        execution_profile_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in execution_profile_specs.itertuples():
        for i in range(workers_numbers[row.worker]):
            worker_name = "{}_{:d}".format(row.worker, i)
            if (row.node, worker_name) not in nodes_workers_execution_profiles:
                nodes_workers_execution_profiles[(row.node, worker_name)] = dict()
            nodes_workers_execution_profiles[(row.node, worker_name)][row.model] = row.latency

    # message_sizes.csv
    # u, v, u_model_variant, v_model_variant, message_size
    # if an operator has no variant, use `default`
    message_sizes_profiles = dict()
    message_sizes_path = os.path.join(data_dir, "message_sizes.csv")
    columns_names = ["u", "v", "u_model", "v_model", "size"]
    dtypes = {
        "u": str,
        "v": str,
        "u_model": str,
        "v_model": str,
        "size": float
    }
    message_sizes_secs = pd.read_csv(
        message_sizes_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in message_sizes_secs.itertuples():
        if (row.u, row.v) not in message_sizes_profiles:
            message_sizes_profiles[(row.u, row.v)] = dict()
        message_sizes_profiles[(row.u, row.v)][(row.u_model, row.v_model)] = row.size

    # sources_workers_link.csv
    # source_node, worker, cost
    sources_workers_link_comm_cost = dict()
    sources_workers_link_path = os.path.join(data_dir, "sources_workers_link.csv")
    columns_names = ["source", "worker", "cost"]
    dtypes = {
        "source": str,
        "worker": str,
        "cost": float
    }
    if os.path.isfile(sources_workers_link_path):
        sources_workers_link = pd.read_csv(
            sources_workers_link_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in sources_workers_link.itertuples():
            for i in range(workers_numbers[row.worker]):
                concrete_worker = "{}_{:d}".format(row.worker, i)
                sources_workers_link_comm_cost[(row.source, concrete_worker)] = row.cost
    
    # input_injection_rates.csv
    # node, rate
    input_injection_rates = dict()
    input_injection_rates_path = os.path.join(data_dir, "input_injection_rates.csv")
    columns_names = ["node", "rate"]
    dtypes = {
        "node": str,
        "rate": float
    }
    input_injection_rates_specs = pd.read_csv(
        input_injection_rates_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in input_injection_rates_specs.itertuples():
        input_injection_rates[row.node] = row.rate

    # accuracy_requirement.json
    # { "sink": sink_node, "threshold": end_to_end_accuracy_threshold }
    accuracy_specs_path = os.path.join(data_dir, "accuracy_requirement.json")
    with open(accuracy_specs_path, 'rt') as f:
        accuracy_specs = json.load(f)
        sink_node = accuracy_specs["sink"]
        accuracy_threshold = accuracy_specs["threshold"]

    workers_partition = None
    workers_partition_path = os.path.join(data_dir, "workers_partition.json")
    if os.path.isfile(workers_partition_path):
        with open(workers_partition_path, 'rt') as f:
            workers_partition_specs = json.load(f)
        if not isinstance(workers_partition_specs, list):
            raise ValueError("workers partition should be a list of sets of workers!")
        workers_partition = []
        for partition in workers_partition_specs:
            concrete_partition = []
            if not isinstance(partition, list):
                raise ValueError("workers partition should be a list of sets of workers!")
            for worker in partition:
                for i in range(workers_numbers[worker]):
                    concrete_worker = "{}_{:d}".format(worker, i)
                    concrete_partition.append(concrete_worker)
            workers_partition.append(concrete_partition)

    accuracy_profile_path = os.path.join(data_dir, "nodes_accuracy_profiles.pkl")
    with open(accuracy_profile_path, 'rb') as f:
        nodes_accuracy_profiles = pickle.load(f)

    inputs = {
        "input_injection_rates": input_injection_rates,
        "sink_node": sink_node,
        "accuracy_threshold": accuracy_threshold,
        "nodes_accuracy_profiles": nodes_accuracy_profiles,
        "nodes_workers_execution_profiles": nodes_workers_execution_profiles,
        "message_sizes_profiles": message_sizes_profiles,
        "workers_partition": workers_partition,
        "workers_workers_link_comm_cost": workers_workers_link_comm_cost,
        "workers_cost": workers_cost,
        "logical_graph_edges": logical_graph_edges,
        "throughput_normalization_ratios": throughput_normalization_ratios,
        "sources_workers_link_comm_cost": sources_workers_link_comm_cost,
    }
    return inputs


def read_inputs_with_concrete_workers(data_dir):
    # workers_specs.csv
    # worker_name, worker_cost
    workers_cost = dict()
    columns_names = ["worker_name", "worker_cost"]
    dtypes = {
        "worker_name": str,
        "worker_cost": float,
    }
    workers_specs_path = os.path.join(data_dir, "workers_specs.csv")
    workers_specs = pd.read_csv(
        workers_specs_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in workers_specs.itertuples():
        worker_name = row.worker_name
        cost = row.worker_cost
        workers_cost[worker_name] = cost
    
    # workers_workers_link.csv
    # worker_a, worker_b, cost
    workers_workers_link_comm_cost = dict()
    workers_workers_link_path = os.path.join(data_dir, "workers_workers_link.csv")
    columns_names = ["worker_a", "worker_b", "cost"]
    dtypes = {
        "worker_a": str,
        "worker_b": str,
        "cost": float
    }
    if os.path.isfile(workers_workers_link_path):
        workers_workers_link = pd.read_csv(
            workers_workers_link_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in workers_workers_link.itertuples():
            workers_workers_link_comm_cost[(row.worker_a, row.worker_b)] = row.cost

    # logical_graph.csv
    # u, v
    logical_graph_edges = []
    logical_graph_path = os.path.join(data_dir, "logical_graph.csv")
    columns_names = ["u", "v"]
    dtypes = {
        "u": str,
        "v": str
    }
    logical_graph = pd.read_csv(
        logical_graph_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in logical_graph.itertuples():
        logical_graph_edges.append((row.u, row.v))

    # throughput_normalization_ratios.csv
    # u, v, normalization_ratio
    throughput_normalization_ratios = dict()
    throughput_normalization_path = os.path.join(data_dir, "throughput_normalization_ratios.csv")
    columns_names = ["u", "v", "ratio"]
    dtypes = {
        "u": str,
        "v": str,
        "ratio": float
    }
    if os.path.isfile(throughput_normalization_path):
        throughput_normalization_specs = pd.read_csv(
            throughput_normalization_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in throughput_normalization_specs.itertuples():
            throughput_normalization_ratios[(row.u, row.v)] = row.ratio

    throughput_normalization_path = os.path.join(data_dir, "throughput_normalization_ratios_per_variant.csv")
    columns_names = ["u", "v", "u_model", "v_model", "ratio"]
    dtypes = {
        "u": str,
        "v": str,
        "u_model": str,
        "v_model": str,
        "ratio": float
    }
    if os.path.isfile(throughput_normalization_path):
        throughput_normalization_ratios = dict()
        throughput_normalization_specs = pd.read_csv(
            throughput_normalization_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in throughput_normalization_specs.itertuples():
            if (row.u, row.v) not in throughput_normalization_ratios:
                throughput_normalization_ratios[(row.u, row.v)] = dict()            
            throughput_normalization_ratios[(row.u, row.v)][(row.u_model, row.v_model)] = row.ratio

    # execution_profile.csv
    # node, worker, model_variant, latency
    # latency is in milliseconds
    nodes_workers_execution_profiles = dict()
    execution_profile_path = os.path.join(data_dir, "execution_profile.csv")
    columns_names = ["node", "worker", "model", "latency"]
    dtypes = {
        "node": str,
        "worker": str,
        "model": str,
        "latency": float
    }
    execution_profile_specs = pd.read_csv(
        execution_profile_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in execution_profile_specs.itertuples():
        if (row.node, row.worker) not in nodes_workers_execution_profiles:
            nodes_workers_execution_profiles[(row.node, row.worker)] = dict()
        nodes_workers_execution_profiles[(row.node, row.worker)][row.model] = row.latency

    # message_sizes.csv
    # u, v, u_model_variant, v_model_variant, message_size
    # if an operator has no variant, use `default`
    message_sizes_profiles = dict()
    message_sizes_path = os.path.join(data_dir, "message_sizes.csv")
    columns_names = ["u", "v", "u_model", "v_model", "size"]
    dtypes = {
        "u": str,
        "v": str,
        "u_model": str,
        "v_model": str,
        "size": float
    }
    message_sizes_secs = pd.read_csv(
        message_sizes_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in message_sizes_secs.itertuples():
        if (row.u, row.v) not in message_sizes_profiles:
            message_sizes_profiles[(row.u, row.v)] = dict()
        message_sizes_profiles[(row.u, row.v)][(row.u_model, row.v_model)] = row.size

    # sources_workers_link.csv
    # source_node, worker, cost
    sources_workers_link_comm_cost = dict()
    sources_workers_link_path = os.path.join(data_dir, "sources_workers_link.csv")
    columns_names = ["source", "worker", "cost"]
    dtypes = {
        "source": str,
        "worker": str,
        "cost": float
    }
    if os.path.isfile(sources_workers_link_path):
        sources_workers_link = pd.read_csv(
            sources_workers_link_path,
            sep=',',
            header=None,
            names=columns_names,
            dtype=dtypes
        )
        for row in sources_workers_link.itertuples():
            sources_workers_link_comm_cost[(row.source, row.worker)] = row.cost
    
    # input_injection_rates.csv
    # node, rate
    input_injection_rates = dict()
    input_injection_rates_path = os.path.join(data_dir, "input_injection_rates.csv")
    columns_names = ["node", "rate"]
    dtypes = {
        "node": str,
        "rate": float
    }
    input_injection_rates_specs = pd.read_csv(
        input_injection_rates_path,
        sep=',',
        header=None,
        names=columns_names,
        dtype=dtypes
    )
    for row in input_injection_rates_specs.itertuples():
        input_injection_rates[row.node] = row.rate

    # accuracy_requirement.json
    # { "sink": sink_node, "threshold": end_to_end_accuracy_threshold }
    accuracy_specs_path = os.path.join(data_dir, "accuracy_requirement.json")
    with open(accuracy_specs_path, 'rt') as f:
        accuracy_specs = json.load(f)
        sink_node = accuracy_specs["sink"]
        accuracy_threshold = accuracy_specs["threshold"]

    workers_partition = None
    workers_partition_path = os.path.join(data_dir, "workers_partition.json")
    if os.path.isfile(workers_partition_path):
        with open(workers_partition_path, 'rt') as f:
            workers_partition_specs = json.load(f)
        if not isinstance(workers_partition_specs, list):
            raise ValueError("workers partition should be a list of sets of workers!")
        workers_partition = []
        for partition in workers_partition_specs:
            if not isinstance(partition, list):
                raise ValueError("workers partition should be a list of sets of workers!")
            workers_partition.append(partition)

    accuracy_profile_path = os.path.join(data_dir, "nodes_accuracy_profiles.pkl")
    with open(accuracy_profile_path, 'rb') as f:
        nodes_accuracy_profiles = pickle.load(f)

    inputs = {
        "input_injection_rates": input_injection_rates,
        "sink_node": sink_node,
        "accuracy_threshold": accuracy_threshold,
        "nodes_accuracy_profiles": nodes_accuracy_profiles,
        "nodes_workers_execution_profiles": nodes_workers_execution_profiles,
        "message_sizes_profiles": message_sizes_profiles,
        "workers_partition": workers_partition,
        "workers_workers_link_comm_cost": workers_workers_link_comm_cost,
        "workers_cost": workers_cost,
        "logical_graph_edges": logical_graph_edges,
        "throughput_normalization_ratios": throughput_normalization_ratios,
        "sources_workers_link_comm_cost": sources_workers_link_comm_cost,
    }
    return inputs


def convert_inputs_to_index_based(
    workers_workers_link_comm_cost,
    workers_cost,
    logical_graph_edges,
    input_injection_rates,  
    throughput_normalization_ratios,
    sources_workers_link_comm_cost,
    workers_partition=None,
    comm_cost_scaling_factor=1.0,
):
    """Convert inputs from str-based (name-based) to zero-indexed.
    `nodes_workers_execution_latency` and `message_sizes` are not considered in this function.
    Parameters
    ----------
    workers_workers_link_comm_cost: dict[tuple[str, str], float]
        Pairwise communication cost. If some pairs are not specified, it is considered to be 0.
    workers_cost: dict[str, float]
        The cost of each worker. It maps the worker name to the cost.
    logical_graph_edges: list[tuple[str, str]]
        The edges in the logical (dataflow) graph. We assume the logical graph is weakly connected.
        Each tuples indicates a directed edge u->v
    input_injection_rates: dict[str, float]
        The input injection rates (request rates) of the sources nodes in the logical graph. It maps the source node's name to the input rate.
    throughput_normalization_ratios: dict[tuple[str, str], float]
        Input rate normalization ratio along the edges. For each edge u->v, it indicates how the multiplication ratio to obtain
        the input rate of v from the (output) throughput of u. If a logical graph edge u->v is not included, we assume the normalization factor is 1.
    sources_workers_link_comm_cost: dict[tuple[str, str], float]
        Source -> worker communication cost.
        If a source->worker link does not appear in the dict, we consider the cost to be 0.
    workers_partition: list[list[str]]
        Optional workers parition. Disjoint sets of workers. The partition is ordered.
    Returns
    -------
    dict
        str inputs (worker names, node names) mapped to zero-indexed inputs.
    """
    workers_idx_mapping = {worker: idx for idx, worker in enumerate(workers_cost.keys())}
    idx_workers_mapping = {idx: worker for worker, idx in workers_idx_mapping.items()}
    num_workers = len(workers_idx_mapping)
    
    mapped_workers_to_workers_comm_cost = np.zeros((num_workers, num_workers), dtype=np.float64)
    for (worker_a, worker_b), cost in workers_workers_link_comm_cost.items():
        worker_a_idx = workers_idx_mapping[worker_a]
        worker_b_idx = workers_idx_mapping[worker_b]
        mapped_workers_to_workers_comm_cost[worker_a_idx, worker_b_idx] = cost * comm_cost_scaling_factor

    mapped_workers_cost = []
    for idx in range(num_workers):
        worker = idx_workers_mapping[idx]
        mapped_workers_cost.append(workers_cost[worker])
    
    logical_graph_nodes = set()
    for u, v in logical_graph_edges:
        logical_graph_nodes.add(u)
        logical_graph_nodes.add(v)
    logical_graph_nodes = list(logical_graph_nodes)
    nodes_idx_mapping = {node: idx for idx, node in enumerate(logical_graph_nodes)}
    idx_nodes_mapping = {idx: node for node, idx in nodes_idx_mapping.items()}
    
    mapped_logical_graph_edges = set()
    for u, v in logical_graph_edges:
        u_idx = nodes_idx_mapping[u]
        v_idx = nodes_idx_mapping[v]
        if (u_idx, v_idx) in mapped_logical_graph_edges or (v_idx, u_idx) in mapped_logical_graph_edges:
            raise ValueError("edge {}->{} appers multiple times or the edge is bidirectional".format(u, v))
        mapped_logical_graph_edges.add((u_idx, v_idx))

    if (len(throughput_normalization_ratios) == 0) or (not isinstance(next(iter(throughput_normalization_ratios.values())), dict)):
        mapped_throughput_normalization_ratios = dict()
        for (u, v), ratio in throughput_normalization_ratios.items():
            u_idx = nodes_idx_mapping[u]
            v_idx = nodes_idx_mapping[v]
            if (u_idx, v_idx) not in mapped_logical_graph_edges:
                raise ValueError("edge {}->{} is not in the logical graph".format(u, v))
            mapped_throughput_normalization_ratios[(u_idx, v_idx)] = ratio
        for u, v in logical_graph_edges:
            u_idx = nodes_idx_mapping[u]
            v_idx = nodes_idx_mapping[v]
            if (u_idx, v_idx) not in mapped_throughput_normalization_ratios:
                mapped_throughput_normalization_ratios[(u_idx, v_idx)] = 1.
    else:
        mapped_throughput_normalization_ratios = throughput_normalization_ratios

    mapped_logical_graph_edges = list(mapped_logical_graph_edges)        

    sources_nodes = list(input_injection_rates.keys())
    mapped_input_injection_rates = dict()
    for source_node, input_rate in input_injection_rates.items():
        source_node_idx = nodes_idx_mapping[source_node]
        mapped_input_injection_rates[source_node_idx] = input_rate
    
    mapped_sources_to_workers_comm_cost = dict()
    for source_node in sources_nodes:
        source_node_idx = nodes_idx_mapping[source_node]
        to_workers_comm_cost = [0.] * num_workers
        mapped_sources_to_workers_comm_cost[source_node_idx] = to_workers_comm_cost
    for (source_node, worker), cost in sources_workers_link_comm_cost.items():
        source_node_idx = nodes_idx_mapping[source_node]
        worker_idx = workers_idx_mapping[worker]
        mapped_sources_to_workers_comm_cost[source_node_idx][worker_idx] = cost * comm_cost_scaling_factor
    
    mapped_workers_partition = None
    if workers_partition:
        mapped_workers_partition = []
        for partition in workers_partition:
            mapped_partition = [workers_idx_mapping[x] for x in partition]
            mapped_workers_partition.append(mapped_partition)
    
    mapped_inputs = {
        "workers_idx_mapping": workers_idx_mapping,
        "idx_workers_mapping": idx_workers_mapping,
        "nodes_idx_mapping": nodes_idx_mapping,
        "idx_nodes_mapping": idx_nodes_mapping,
        "workers_cost": mapped_workers_cost,
        "workers_to_workers_comm_cost": mapped_workers_to_workers_comm_cost,
        "workers_partition": mapped_workers_partition,
        "logical_graph_edges": mapped_logical_graph_edges,
        "input_injection_rates": mapped_input_injection_rates,
        "throughput_normalization_ratios": mapped_throughput_normalization_ratios,
        "sources_to_workers_comm_cost": mapped_sources_to_workers_comm_cost,
    }
    return mapped_inputs
    

def assign_model_variants(
    models_assignment,
    node_idx_mapping,
    worker_idx_mapping,
    nodes_workers_execution_profiles,
    message_sizes_profiles=None,
    throughput_normalization_ratios=None,
    latency_overestimate_ratio=1.0,
):
    """Assign model variants, select corresponding execution profile for each node, 
    and use appropriate message sizes
    Parameters
    ----------
    model_assignment: dict[str, str]
        Model assignment for each ML node in the logical graph. The dict maps from node to model variant assignment
        If a node is not present in this dict, then we use the variant `default` (e.g., non-ML nodes).
    node_idx_mapping: dict[str, int]
        Node name to index mapping/
    worker_idx_mapping: dict[str, int]
        Worker name to index mapping.
    nodes_workers_execution_profiles: dict[tuple[str, str], dict[str, float]]
        The execution latency for each model variant of each node on each worker.
        The dict maps from (node, worker) to another dict, which maps model variant to execution latency.
    throughput_normalization_ratios: dict[tuple[str, str], dict[str, float]] or dict[tuple[str, str], float]
        The throughput normalization factor between nodes, with either a uniformed one or on a per-variant basis.
    message_sizes: dict[tuple[str, str], dict[tuple[str, str], float]]
        The message sizes along each edge in the logical graph. The dict maps each edge (u, v)
        to another dict, which maps the model choices for u, v (i.e., model_u, model_v) to the message size along this edge. 
    """
    mapped_nodes_workers_execution_latency = dict()
    for (node, worker), profile in nodes_workers_execution_profiles.items():
        node_idx = node_idx_mapping[node]
        worker_idx = worker_idx_mapping[worker]
        variant = models_assignment.get(node, "default")
        mapped_nodes_workers_execution_latency[(node_idx, worker_idx)] = profile[variant] * latency_overestimate_ratio
    
    mapped_message_sizes = None
    if message_sizes_profiles:
        mapped_message_sizes = dict()
        for (u, v), profile in message_sizes_profiles.items():
            u_idx = node_idx_mapping[u]
            v_idx = node_idx_mapping[v]
            u_variant = models_assignment.get(u, "default")
            v_variant = models_assignment.get(v, "default")
            if (u_variant, v_variant) not in profile:
                if ("default", v_variant) not in profile:
                    if (u_variant, "default") not in profile:
                        u_variant = "default"
                        v_variant = "default"
                    else:
                        v_variant = "default"
                else:
                    u_variant = "default"
            mapped_message_sizes[(u_idx, v_idx)] = profile[(u_variant, v_variant)]
    

    if isinstance(throughput_normalization_ratios, dict) and len(throughput_normalization_ratios) > 0 and isinstance(next(iter(throughput_normalization_ratios.values())), dict):
        mapped_throughput_normalization_ratios = dict()
        for (u, v), profile in throughput_normalization_ratios.items():
            u_idx = node_idx_mapping[u]
            v_idx = node_idx_mapping[v]
            u_variant = models_assignment.get(u, "default")
            v_variant = models_assignment.get(v, "default")
            if (u_variant, v_variant) not in profile:
                if ("default", v_variant) not in profile:
                    if (u_variant, "default") not in profile:
                        u_variant = "default"
                        v_variant = "default"
                    else:
                        v_variant = "default"
                else:
                    u_variant = "default"
            mapped_throughput_normalization_ratios[(u_idx, v_idx)] = profile[(u_variant, v_variant)]
    else:
        mapped_throughput_normalization_ratios = throughput_normalization_ratios
    
    return mapped_nodes_workers_execution_latency, mapped_message_sizes, mapped_throughput_normalization_ratios