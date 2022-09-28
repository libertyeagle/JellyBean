import networkx as nx
import networkx.algorithms.community as nx_comm
from sklearn.cluster import SpectralClustering
import numpy as np


def partition_workers_into_tiers(
    workers_workers_comm_cost,
    workers_cost,
    **kwargs
):
    strategy = kwargs["strategy"]
    if strategy == "louvain":
        return louvain(workers_workers_comm_cost, workers_cost, **kwargs)
    elif strategy == "girvan_newman":
        return girvan_newman(workers_workers_comm_cost, workers_cost, **kwargs)
    elif strategy == "min-cut":
        return recursive_min_cut(workers_workers_comm_cost, workers_cost, **kwargs)
    elif strategy == "spectral-clustering":
        return spectral_clustering(workers_workers_comm_cost, workers_cost, **kwargs)
    elif strategy == "greedy-modularity":
        return greedy_modularity(workers_workers_comm_cost, workers_cost, **kwargs)
    else:
        raise ValueError("invalid worker partition strategy")

def louvain(
    workers_workers_comm_cost,
    workers_cost,
    louvain_resolution=0.5,
    louvain_threshold=1e-07
):
    num_workers = len(workers_cost)
    graph = nx.Graph()
    graph.add_nodes_from(range(num_workers))
    for worker_a in range(num_workers):
        for worker_b in range(num_workers):
            graph.add_edge(worker_a, worker_b, weight=1 / (1 + workers_workers_comm_cost[worker_a, worker_b]))
    
    partitions = nx_comm.louvain_communities(
        graph,
        weight='weight', 
        resolution=louvain_resolution,
        threshold=louvain_threshold,
        seed=42
    )
    partitions = list(partitions)
    partitions_costs = []
    for idx, partition in enumerate(partitions):
        partitions_costs.append((idx, np.mean([workers_cost[x] for x in partition])))
    partitions_costs.sort(key=lambda x: x[1])

    ordered_partitions = []
    for idx, _ in partitions_costs:
        ordered_partitions.append(list(partitions[idx]))
    
    return ordered_partitions


def girvan_newman(
    workers_workers_comm_cost,
    workers_cost
):
    num_workers = len(workers_cost)
    graph = nx.Graph()
    graph.add_nodes_from(range(num_workers))
    for worker_a in range(num_workers):
        for worker_b in range(num_workers):
            graph.add_edge(worker_a, worker_b, weight=1 / (1 + workers_workers_comm_cost[worker_a, worker_b]))
    
    partitions = nx_comm.girvan_newman(
        graph,
        weight='weight'
    )
    partitions = next(partitions)
    partitions_costs = []
    for idx, partition in enumerate(partitions):
        partitions_costs.append((idx, np.mean([workers_cost[x] for x in partition])))
    partitions_costs.sort(key=lambda x: x[1])

    ordered_partitions = []
    for idx, _ in partitions_costs:
        ordered_partitions.append(list(partitions[idx]))
    
    return ordered_partitions


def greedy_modularity(
    workers_workers_comm_cost,
    workers_cost,
    greedy_modularity_resolution=0.5,
    num_tiers=2,
):
    num_workers = len(workers_cost)
    graph = nx.Graph()
    graph.add_nodes_from(range(num_workers))
    for worker_a in range(num_workers):
        for worker_b in range(num_workers):
            graph.add_edge(worker_a, worker_b, weight=1 / (1 + workers_workers_comm_cost[worker_a, worker_b]))
    
    partitions = nx_comm.greedy_modularity_communities(
        graph,
        weight='weight',
        resolution=greedy_modularity_resolution,
        n_communities=num_tiers
    )
    partitions = next(partitions)
    partitions_costs = []
    for idx, partition in enumerate(partitions):
        partitions_costs.append((idx, np.mean([workers_cost[x] for x in partition])))
    partitions_costs.sort(key=lambda x: x[1])

    ordered_partitions = []
    for idx, _ in partitions_costs:
        ordered_partitions.append(list(partitions[idx]))
    
    return ordered_partitions


def recursive_min_cut(
    workers_workers_comm_cost,
    workers_cost,
    num_tiers=2,
):
    num_workers = len(workers_cost)
    graph = nx.Graph()
    graph.add_nodes_from(range(num_workers))
    for worker_a in range(num_workers):
        for worker_b in range(num_workers):
            graph.add_edge(worker_a, worker_b, weight=1 / (1 + workers_workers_comm_cost[worker_a, worker_b]))
    
    nodes_to_partition = list(range(num_workers))
    ordered_partitions = []
    for _ in range(num_tiers - 1):
        source =  max(nodes_to_partition, key=lambda x: workers_cost[x])()
        sink = max(nodes_to_partition, key=lambda x: workers_cost[x])
        partition_a, partition_b = nx.minimum_cut(graph, _s=source, _sink=sink)[1]
        average_cost_a = np.mean([workers_cost[x] for x in partition_a])
        average_cost_b = np.mean([workers_cost[x] for x in partition_b])
        if average_cost_a < average_cost_b:
            ordered_partitions.append(list(partition_a))
            nodes_to_partition = list(partition_b)
        else:
            ordered_partitions.append(list(partition_b))
            nodes_to_partition = list(partition_a)

    return ordered_partitions


def spectral_clustering(
    workers_workers_comm_cost,
    workers_cost,
    num_tiers=2
):
    num_workers = len(workers_cost)
    for worker_a in range(num_workers):
        for worker_b in range(num_workers):
            workers_workers_comm_cost[worker_a, worker_b] = 1 / (1 + workers_workers_comm_cost[worker_a, worker_b])
    if not np.allclose(workers_workers_bandwidth_graph, workers_workers_bandwidth_graph.T):
        workers_workers_bandwidth_graph = 0.5 * (workers_workers_bandwidth_graph + workers_workers_bandwidth_graph.T)

    cluster_lables = SpectralClustering(n_clusters=num_tiers, affinity='precomputed').fit_predict(workers_workers_bandwidth_graph)
    clusters = dict()
    for worker_idx, cluster_idx in enumerate(cluster_lables):
        if cluster_idx not in clusters:
            clusters[cluster_idx] = []
        cluster_idx[cluster_idx].append(worker_idx)
    
    partitions = list(clusters.values())
    partitions_costs = []
    for idx, partition in enumerate(partitions):
        partitions_costs.append((idx, np.mean([workers_cost[x] for x in partition])))
    partitions_costs.sort(key=lambda x: x[1])

    ordered_partitions = []
    for idx, _ in partitions_costs:
        ordered_partitions.append(list(partitions[idx]))
    
    return ordered_partitions