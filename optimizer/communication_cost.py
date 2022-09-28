def compute_communication_cost(
    workers_to_workers_comm_cost,
    workers_partition,
    logical_graph_edges,
    workers_assignment,
    message_sizes,
    sources_to_workers_comm_cost,
    allow_backward_traffic=True
):
    """Compute the (cross-tier) communication cost of a physical execution.
    Workers and logical graph node are all integer-indexed.
    Assume the data sources are located on the first tier.
    Parameters
    ----------
    workers_to_workers_comm_cost: np.ndarray
        ndarray of (num_workers, num_workers). It represents the pairwise communication cost (of unit traffic/bandwidth).
    workers_partition: list[list(int)]
        Partition of the workers into tiers. It is a list of clusters, each cluster represents a tier,
        which contains the indices of the workers assigned to it.
    logical_graph_edges: list[tuple[int, int]]
        The edges in the logical dataflow graph, each represent an edge u->v in the dataflow graph.
        The source nodes (which represent the data sources) are also included in the logical graph.
    workers_assignment: dict[int, list[tuple[int, float]]]
        The worker assignment for each logical node. For each node v, there is a list of tuples, each tuple represents
        the index of assigned worker, and the throughput (with respect to the input injection rate of v)
        assigned to this worker.
    message_sizes: dict[tuple[int, int], float]
        The message sizes (i.e., number of bytes) along each edge in the logical graph. For each edge u->v,
        we have a message size represents for each request of v, the size of the input v acquires from u.
    sources_to_workers_comm_cost: dict[int, list[float]]
        For each of the source node, the communication cost to each of the worker.
    allow_backward_traffic: bool
        Whether backward traffic is allowd.
    Returns
    -------
    float
        communication cost
    """
    sources_nodes = set(sources_to_workers_comm_cost.keys())

    workers_partition_mapping = dict()
    for idx, partition in enumerate(workers_partition):
        for worker_idx in partition:
            if worker_idx in workers_partition_mapping:
                raise ValueError("worker {} is in multiple partitions".format(worker_idx))
            workers_partition_mapping[worker_idx] = idx

    total_communication_cost = 0.
    for u, v in logical_graph_edges:
        if u in sources_nodes:
            v_assigned_workers = workers_assignment[v]
            # v_tier = workers_partition_mapping[v_assigned_workers[0][0]]
            # # check whether v's workers are located on the same tier.            
            # if not all(workers_partition_mapping[x[0]] == v_tier for x in v_assigned_workers):
            #     raise ValueError("logical graph node {}'s assigned workers {} are in not on the same tier".format(v, [x[0] for x in v_assigned_workers])) 
            total_v_throughput = sum(x[1] for x in v_assigned_workers)
            total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
            v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in v_assigned_workers]

            # source -> worker link commmunication cost
            for worker_idx, worker_rx in v_per_worker_rx:
                worker_tier = workers_partition_mapping[worker_idx]
                if worker_tier > 0:                
                    total_communication_cost += sources_to_workers_comm_cost[u][worker_idx] * worker_rx * 3600. / 1024. / 1024.
        else:
            # u and v's assigned workers, and each worker's throughput
            u_assigned_workers = workers_assignment[u]
            v_assigned_workers = workers_assignment[v]

            # u_tier = workers_partition_mapping[u_assigned_workers[0][0]]
            # v_tier = workers_partition_mapping[v_assigned_workers[0][0]]
            # if u_tier == v_tier:
            #     # u and v are on the same tier, we do not consider the communication cost within the same tier.
            #     continue
            # # check whether u's workers are located on the same tier.
            # if not all(workers_partition_mapping[x[0]] == u_tier for x in u_assigned_workers):
            #     raise ValueError("logical graph node {}'s assigned workers {} are in not on the same tier".format(u, [x[0] for x in u_assigned_workers]))
            # # check whether v's workers are located on the same tier.            
            # if not all(workers_partition_mapping[x[0]] == v_tier for x in v_assigned_workers):
            #     raise ValueError("logical graph node {}'s assigned workers {} are in not on the same tier".format(v, [x[0] for x in v_assigned_workers]))

            total_u_throughput = sum(x[1] for x in u_assigned_workers)
            total_v_throughput = sum(x[1] for x in v_assigned_workers)
            total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
            u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
            v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in v_assigned_workers]

            # worker -> worker communication cost
            for u_worker_idx, u_worker_tx in u_per_worker_tx:
                for v_worker_idx, v_worker_rx in v_per_worker_rx:
                    u_worker_tier = workers_partition_mapping[u_worker_idx]
                    v_worker_tier = workers_partition_mapping[v_worker_idx]
                    if (not allow_backward_traffic) and (v_worker_tier < u_worker_tier):
                        total_communication_cost = float('inf')
                    elif v_worker_tier > u_worker_tier:
                        link_bandwidth = u_worker_tx * (v_worker_rx / total_edge_bandwidth)
                        total_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * link_bandwidth * 3600. / 1024. / 1024.

    return total_communication_cost


def compute_communication_traffic_volume(
    logical_graph_edges,
    workers_partition,
    workers_assignment,
    message_sizes,
    sources_to_workers_comm_cost,
    tier_forward_scaling_factor=1.0,
    tier_backward_scaling_factor=10.0,
):
    """Compute the total cross-tier communication traffic volume of a physical execution.
    Workers and logical graph node are all integer-indexed.
    Assume the data sources are located on the first tier.
    Parameters
    ----------
    workers_to_workers_comm_cost: np.ndarray
        ndarray of (num_workers, num_workers). It represents the pairwise communication cost (of unit traffic/bandwidth).
    workers_partition: list[list(int)]
        Partition of the workers into tiers. It is a list of clusters, each cluster represents a tier,
        which contains the indices of the workers assigned to it.        
    logical_graph_edges: list[tuple[int, int]]
        The edges in the logical dataflow graph, each represent an edge u->v in the dataflow graph.
        The source nodes (which represent the data sources) are also included in the logical graph.
    workers_assignment: dict[int, list[tuple[int, float]]]
        The worker assignment for each logical node. For each node v, there is a list of tuples, each tuple represents
        the index of assigned worker, and the throughput (with respect to the input injection rate of v)
        assigned to this worker.
    message_sizes: dict[tuple[int, int], float]
        The message sizes (i.e., number of bytes) along each edge in the logical graph. For each edge u->v,
        we have a message size represents for each request of v, the size of the input v acquires from u.
    sources_to_workers_comm_cost dict[int, list[float]]:
        Only used to get the indices of the data source nodes. Maintain API consistency
    Returns
    -------
    float
        communication traffic_volume, GB / hour
    """
    sources_nodes = set(sources_to_workers_comm_cost.keys())

    workers_partition_mapping = dict()
    for idx, partition in enumerate(workers_partition):
        for worker_idx in partition:
            if worker_idx in workers_partition_mapping:
                raise ValueError("worker {} is in multiple partitions".format(worker_idx))
            workers_partition_mapping[worker_idx] = idx

    total_communication_cost = 0.
    for u, v in logical_graph_edges:
        if u in sources_nodes:
            v_assigned_workers = workers_assignment[v]
            total_v_throughput = sum(x[1] for x in v_assigned_workers)
            total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
            v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in v_assigned_workers]

            # source -> worker link commmunication cost
            for worker_idx, worker_rx in v_per_worker_rx:
                worker_tier = workers_partition_mapping[worker_idx]
                if worker_tier > 0:
                    total_communication_cost += tier_forward_scaling_factor * worker_rx * 3600. / 1024. / 1024.
        else:
            # u and v's assigned workers, and each worker's throughput
            u_assigned_workers = workers_assignment[u]
            v_assigned_workers = workers_assignment[v]
 
            total_u_throughput = sum(x[1] for x in u_assigned_workers)
            total_v_throughput = sum(x[1] for x in v_assigned_workers)
            total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
            u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
            v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in v_assigned_workers]

            # worker -> worker communication cost
            for u_worker_idx, u_worker_tx in u_per_worker_tx:
                for v_worker_idx, v_worker_rx in v_per_worker_rx:
                    u_worker_tier = workers_partition_mapping[u_worker_idx]
                    v_worker_tier = workers_partition_mapping[v_worker_idx]
                    link_bandwidth = u_worker_tx * (v_worker_rx / total_edge_bandwidth)
                    if v_worker_tier < u_worker_tier:
                        total_communication_cost += tier_backward_scaling_factor * link_bandwidth * 3600. / 1024. / 1024.
                    elif v_worker_tier > u_worker_tier:
                        total_communication_cost += tier_forward_scaling_factor * link_bandwidth * 3600. / 1024. / 1024.

    return total_communication_cost


