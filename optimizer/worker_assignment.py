from copy import deepcopy
import math
import bisect
from random import shuffle
from itertools import chain, combinations
from graphlib import TopologicalSorter
import numpy as np
import networkx as nx


def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))


def compute_worker_assignment(
    workers_to_workers_comm_cost,
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
    message_sizes,
    sources_to_workers_comm_cost,
    strategy='same-tier',
    tier_allocation='greedy',
    max_num_assign_orders=None,
    best_k_assignments=None, 
):
    """
    With respect to the parameters, message_sizes and the bandwidth capacity should use the same unit of information (e.g., KB, MB...)
    Parameters
    ----------
    workers_to_workers_comm_cost: np.ndarray
        ndarray of (num_workers, num_workers). It represents the pairwise communication cost (of unit traffic/bandwidth).
    logical_graph_edges: list[tuple[int, int]]
        The edges in the logical dataflow graph, each represent an edge u->v in the dataflow graph.
        The source nodes (which represent the data sources) are also included in the logical graph.
    nodes_workers_execution_latency: dict[tuple[int, int], float]
        The execution latency for each logical node on each worker. It is a dictionary that maps (node_idx, worker_idx) to the execution latency.
        The execution latency's unit should be milliseconds.
    input_injection_rates: dict[int, float]
        The input injection rates for the source nodes. It is a dictionary that maps node_idx to the input rate.
    throughput_normalization_ratios: dict[tuple[int, int], float]
        Input rate normalization ratio along each edge. For each edge u->v, it indicates how the multiplication ratio to obtain
        the input rate of v from the (output) throughput of u.
    message_sizes: dict[tuple[int, int], float]
        The message sizes (i.e., number of bytes) along each edge in the logical graph. For each edge u->v,
        we have a message size represents for each request of v, the size of the input v acquires from u.
    sources_to_workers_comm_cost dict[int, list[float]]:
        For each of the source node, the communication cost to each of the worker.    
    strategy: str
        Which strategy to use?
        "same-tier": Prefer assignment to the same tier as input nodes. Only move forward to the next tier, if current tier does not
        have enough computation capability.
        "cross-tier": Allow cross-tier placement, even the higher tiers have lower computation cost, and the bandwidth is enough.
        "cross-tier-one-hop": "cross-tier" with one-hop allowed.
        "all-tiers": Consider all possible tiers in assigning each node, extend multiple worker assignments each time.
    tier_allocation: str
        Per-tier worker allocation strategy. `dp` or `greedy` or `meet-in-the-middle`
    max_num_assign_orders: int
        Consider different topological order in assigning the nodes, this parameter restricts the maximum number of topological orders considered.
        Only used if strategy is `all-tiers`
    best_k_assignments: int
        Keep the top-k lowest cost worker assignment each time expanding to a new node, if strategy `all-tiers` is used.        
    Returns
    -------
        If returns a dict or a list of dict, it represents the workers assignment and the total compute cost.
        For each node v, there is a list of tuples, each tuple represents the index of assigned worker, and the throughput
        If returns None, it means no feasible solution have been found.
    """
    if strategy == "same-tier":
        return compute_worker_assignment_same_tier(
            workers_partition,
            workers_cost,
            logical_graph_edges,
            nodes_workers_execution_latency,
            input_injection_rates,
            throughput_normalization_ratios,
            tier_allocation=tier_allocation
        )
    elif strategy == "cross-tier" or strategy == 'cross-tier-one-hop':
        return compute_worker_assignment_cross_tier(
            workers_to_workers_comm_cost,
            workers_partition,
            workers_cost,
            logical_graph_edges,
            nodes_workers_execution_latency,
            input_injection_rates,
            throughput_normalization_ratios,
            message_sizes,
            sources_to_workers_comm_cost,
            strategy=strategy,
            tier_allocation=tier_allocation    
        )
    elif strategy == "multiple-tiers":
        return compute_worker_assignment_multiple_tiers(
            workers_to_workers_comm_cost,
            workers_partition,
            workers_cost,
            logical_graph_edges,
            nodes_workers_execution_latency,
            input_injection_rates,
            throughput_normalization_ratios,
            message_sizes,
            sources_to_workers_comm_cost,
            max_num_assign_orders,
            best_k_assignments,
            tier_allocation=tier_allocation
        )
    else:
        return compute_worker_assignment_all_tiers(
            workers_to_workers_comm_cost,
            workers_partition,
            workers_cost,
            logical_graph_edges,
            nodes_workers_execution_latency,
            input_injection_rates,
            throughput_normalization_ratios,
            message_sizes,
            sources_to_workers_comm_cost,
            max_num_assign_orders,
            best_k_assignments,
            tier_allocation=tier_allocation   
        )


def compute_worker_assignment_same_tier(
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
    tier_allocation='greedy'
):
    predecessors_list = dict()
    for u, v in logical_graph_edges:
        if v not in predecessors_list:
            predecessors_list[v] = set()
        if u not in predecessors_list:
            predecessors_list[u] = set()
        predecessors_list[v].add(u)
    topological_order = TopologicalSorter(predecessors_list)
    topological_order = list(topological_order.static_order())

    nodes_workers_execution_throughput = dict()
    for (v, worker_idx), latency in nodes_workers_execution_latency.items():
        if not np.isnan(latency):
            nodes_workers_execution_throughput[(v, worker_idx)] = 1000. / latency
    num_workers = len(workers_cost)
    num_tiers = len(workers_partition)

    sources_nodes = list(input_injection_rates.keys())
    
    # the tier of the worker partition each logical graph node is assigned on
    tiers_assignment = dict()
    workers_assignment = dict()
    total_compute_cost = 0.
    nodes_total_throughput = dict()
    workers_is_assigned = np.zeros(num_workers, dtype=bool)
    for v in topological_order:
        if v in sources_nodes:
            nodes_total_throughput[v] = input_injection_rates[v]
            tiers_assignment[v] = -1
            continue
        inputs_nodes = predecessors_list[v]
        v_expected_throughput = min([nodes_total_throughput[u] * throughput_normalization_ratios[(u, v)] for u in inputs_nodes])
        nodes_total_throughput[v] = v_expected_throughput
        highest_inputs_tier = max([tiers_assignment[u] for u in inputs_nodes])
        if highest_inputs_tier < 0:
            highest_inputs_tier = 0
    
        v_cost = None
        v_assignment = None
        for i in range(highest_inputs_tier, num_tiers):
            if tier_allocation == "greedy":
                # all available workers on tier i
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                satisfiable_workers = [x for x in avaiable_workers if nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput]
                if len(satisfiable_workers) > 0:
                    # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                    workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                    workers_unit_cost.sort(key=lambda x: x[1])
                else:
                    # otherwise we sort the workers according to per-input execution cost
                    workers_unit_cost = [(x, workers_cost[x] / nodes_workers_execution_throughput[(v, x)], workers_cost[x]) for x in avaiable_workers]
                    workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                
                temp_assigned_workers = []
                temp_throughput = 0.
                while temp_throughput < v_expected_throughput:
                    if len(workers_unit_cost) == 0:
                        break                
                    worker_idx = workers_unit_cost.pop(0)[0]
                    worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                    if temp_throughput + worker_max_throughput > v_expected_throughput:
                        worker_assigned_throughput = v_expected_throughput - temp_throughput
                    else:
                        worker_assigned_throughput = worker_max_throughput
                    temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                    temp_throughput += worker_assigned_throughput
                    if temp_throughput < v_expected_throughput:
                        satisfiable_workers = [x[0] for x in workers_unit_cost if nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput]
                        if len(satisfiable_workers) > 0:
                            workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            workers_unit_cost = [(x[0], workers_cost[x[0]] / nodes_workers_execution_throughput[(v, x[0])], workers_cost[x[0]]) for x in workers_unit_cost]
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
            elif tier_allocation == "greedy-backtrace":
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                satisfiable_workers = [x for x in avaiable_workers if nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput]
                if len(satisfiable_workers) > 0:
                    # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                    workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                    workers_unit_cost.sort(key=lambda x: x[1])
                else:
                    # otherwise we sort the workers according to per-input execution cost
                    workers_unit_cost = [(x, workers_cost[x] / nodes_workers_execution_throughput[(v, x)], workers_cost[x]) for x in avaiable_workers]
                    workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                
                temp_assigned_workers = []
                temp_throughput = 0.
                while temp_throughput < v_expected_throughput:
                    if len(workers_unit_cost) == 0:
                        break                
                    worker_idx = workers_unit_cost.pop(0)[0]
                    worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                    temp_assigned_workers.append((worker_idx, worker_max_throughput))
                    temp_throughput += worker_max_throughput
                    if temp_throughput < v_expected_throughput:
                        satisfiable_workers = [x[0] for x in workers_unit_cost if nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput]
                        if len(satisfiable_workers) > 0:
                            workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            workers_unit_cost = [(x[0], workers_cost[x[0]] / nodes_workers_execution_throughput[(v, x[0])], workers_cost[x[0]]) for x in workers_unit_cost]
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                temp_assigned_workers.sort(key=lambda x: x[1])
                start = -1
                removed_throughput = 0
                while removed_throughput < temp_throughput - v_expected_throughput:
                    if removed_throughput + temp_assigned_workers[start + 1][1] > temp_throughput - v_expected_throughput:
                        break 
                    else:
                        start += 1
                        removed_throughput += temp_assigned_workers[start][1]
                temp_assigned_workers_filtered = temp_assigned_workers[start + 1:]
                temp_assigned_workers = []
                temp_throughput = 0.
                for worker_idx, worker_max_throughput in temp_assigned_workers_filtered:
                    if temp_throughput + worker_max_throughput > v_expected_throughput:
                        worker_assigned_throughput = v_expected_throughput - temp_throughput
                    else:
                        worker_assigned_throughput = worker_max_throughput
                    temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                    temp_throughput += worker_assigned_throughput
            elif tier_allocation == "dp":
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                avaiable_workers = [(x, workers_cost[1], math.floor(nodes_workers_execution_throughput[(v, x)] * 100.)) for x in avaiable_workers]
                
                num_available_workers = len(avaiable_workers)
                evict_capacity = sum([x[2] for x in avaiable_workers]) - math.ceil(v_expected_throughput * 100.)
                if evict_capacity >= 0:
                    knapsack_dp = np.zeros((num_available_workers + 1, evict_capacity + 1))
                    knapsack_evict = np.zeros((num_available_workers + 1, evict_capacity + 1), dtype=bool)
                    for m in range(1, num_available_workers + 1):
                        for w in range(0, evict_capacity + 1):
                            if avaiable_workers[m - 1][2] > w:
                                knapsack_dp[m, w] = knapsack_dp[m - 1, w]
                            else:
                                knapsack_dp[m, w] = max(knapsack_dp[m - 1, w], knapsack_dp[m - 1, w - avaiable_workers[m - 1][2]] + avaiable_workers[m - 1][1])
                                knapsack_evict[m, w] = True if knapsack_dp[m - 1, w - avaiable_workers[m - 1][2]] + avaiable_workers[m - 1][1] > knapsack_dp[m - 1, w] else False

                    temp_assigned_workers = []
                    temp_throughput = 0.
                    free_capacity = evict_capacity
                    for m in range(num_available_workers, 0, -1):
                        if not knapsack_evict[m, free_capacity]:
                            worker_idx = avaiable_workers[m - 1][0]
                            worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                            if temp_throughput + worker_max_throughput > v_expected_throughput:
                                worker_assigned_throughput = v_expected_throughput - temp_throughput
                            else:
                                worker_assigned_throughput = worker_max_throughput
                            temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                            temp_throughput += worker_assigned_throughput
                            if temp_throughput >= v_expected_throughput:
                                break
                        else:
                            free_capacity -= avaiable_workers[m - 1][2]
                else:
                    temp_assigned_workers = []
                    temp_throughput = 0.
            elif tier_allocation == "meet-in-the-middle":
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                shuffle(avaiable_workers)
                num_available_workers = len(avaiable_workers)
                workers_set_a = avaiable_workers[:num_available_workers // 2]
                workers_set_b = avaiable_workers[num_available_workers // 2:]
                subsets_a = powerset(workers_set_a)
                subsets_b = powerset(workers_set_b)
                subsets_a_marked = []
                for subset in subsets_a:
                    subset_cost = sum([workers_cost[x] for x in subset])
                    subset_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in subset])
                    subsets_a_marked.append((subset, subset_cost, subset_throughput))
                subsets_b_marked = []
                for subset in subsets_b:
                    subset_cost = sum([workers_cost[x] for x in subset])
                    subset_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in subset])
                    subsets_b_marked.append((subset, subset_cost, subset_throughput))                    
                subsets_b_marked = [subset for subset in subsets_b_marked if not any(x[1] <= subset[1] and x[2] > subset[2] for x in subsets_b_marked)]
                subsets_b_marked.sort(key=lambda subset: subset[2])
                subsets_b_tp = [subset[2] for subset in subsets_b_marked]
                min_cost = np.inf
                min_subset_a = None
                min_subset_b = None
                for (subset_a, cost_a, throughput_a) in subsets_a_marked:
                    min_throughput_b = v_expected_throughput - throughput_a
                    if min_throughput_b <= 0:
                        if cost_a < min_cost:
                            min_cost = cost_a
                            min_subset_a = set(subset_a)
                            min_subset_b = set()
                    else:
                        pos = bisect.bisect_left(subsets_b_tp, min_throughput_b)
                        if pos < len(subsets_b_marked):
                            subset_b, cost_b, throughput_b = min(subsets_b_marked[pos:], key=lambda x: x[1])
                            assert throughput_a + throughput_b >= v_expected_throughput
                            if cost_a + cost_b < min_cost:
                                min_cost = cost_a + cost_b
                                min_subset_a = set(subset_a)
                                min_subset_b = set(subset_b)
                temp_assigned_workers = []
                temp_throughput = 0.
                if not np.isinf(min_cost):
                    for worker_idx in chain(min_subset_a, min_subset_b):
                        worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                        if temp_throughput + worker_max_throughput > v_expected_throughput:
                            worker_assigned_throughput = v_expected_throughput - temp_throughput
                        else:
                            worker_assigned_throughput = worker_max_throughput   
                        temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                        temp_throughput += worker_assigned_throughput             
            else:
                raise ValueError("invalid tier worker allocation strategy {}".format(tier_allocation))

            # we have a feasible assignment on tier i
            if temp_throughput >= v_expected_throughput:
                v_cost = sum([workers_cost[x[0]] for x in temp_assigned_workers])
                total_compute_cost += v_cost
                for worker_idx, _ in temp_assigned_workers:
                    workers_is_assigned[worker_idx] = True 
                v_assignment = temp_assigned_workers.copy()
                workers_assignment[v] = v_assignment
                tiers_assignment[v] = i
                break

        if v_cost is None:
            # infeasible
            return None
    
    assignment = {
        "total_compute_cost": total_compute_cost,
        "workers_assignment": workers_assignment
    }
    return assignment


def compute_worker_assignment_cross_tier(
    workers_to_workers_comm_cost,
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
    message_sizes,
    sources_to_workers_comm_cost,
    strategy='cross-tier',
    tier_allocation="greedy"
):
    predecessors_list = dict()
    for u, v in logical_graph_edges:
        if v not in predecessors_list:
            predecessors_list[v] = set()
        if u not in predecessors_list:
            predecessors_list[u] = set()
        predecessors_list[v].add(u)
    topological_order = TopologicalSorter(predecessors_list)
    topological_order = list(topological_order.static_order())

    nodes_workers_execution_throughput = dict()
    for (v, worker_idx), latency in nodes_workers_execution_latency.items():
        if not np.isnan(latency):
            nodes_workers_execution_throughput[(v, worker_idx)] = 1000. / latency

    sources_nodes = set(sources_to_workers_comm_cost.keys())
    num_workers = workers_to_workers_comm_cost.shape[0]
    num_tiers = len(workers_partition)

    # the tier of the worker partition each logical graph node is assigned on
    tiers_assignment = dict()
    workers_assignment = dict()
    total_compute_cost = 0.
    nodes_total_throughput = dict()
    workers_is_assigned = np.zeros(num_workers, dtype=bool)
    for v in topological_order:
        if v in sources_nodes:
            nodes_total_throughput[v] = input_injection_rates[v]
            tiers_assignment[v] = -1
            continue
        inputs_nodes = predecessors_list[v]
        v_expected_throughput = min([nodes_total_throughput[u] * throughput_normalization_ratios[(u, v)] for u in inputs_nodes])
        nodes_total_throughput[v] = v_expected_throughput
        highest_inputs_tier = max([tiers_assignment[u] for u in inputs_nodes])
        if highest_inputs_tier < 0:
            highest_inputs_tier = 0
        
        min_cost = np.inf
        min_compute_cost = None
        min_cost_tier = None
        min_cost_assignment = None

        # try to put node v in every tier that is higher than its inputs.
        for i in range(highest_inputs_tier, num_tiers):
            no_solution_found_prev_tiers = min_cost_tier is None
           
            if tier_allocation == "greedy":
                # all available workers on tier i
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                satisfiable_workers = [x for x in avaiable_workers if nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput]
                if len(satisfiable_workers) > 0:
                    # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                    workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                    workers_unit_cost.sort(key=lambda x: x[1])
                else:
                    # otherwise we sort the workers according to per-input execution cost
                    workers_unit_cost = [(x, workers_cost[x] / nodes_workers_execution_throughput[(v, x)], workers_cost[x]) for x in avaiable_workers]
                    workers_unit_cost.sort(key=lambda x: (x[1], x[2]))

                # temporary worker assignment, if we are going to put v on tier i
                temp_assigned_workers = []
                # current throughput
                temp_throughput = 0.
                while temp_throughput < v_expected_throughput:
                    if len(workers_unit_cost) == 0:
                        break                
                    worker_idx = workers_unit_cost.pop(0)[0]
                    worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                    if temp_throughput + worker_max_throughput > v_expected_throughput:
                        worker_assigned_throughput = v_expected_throughput - temp_throughput
                    else:
                        worker_assigned_throughput = worker_max_throughput

                    temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                    temp_throughput += worker_assigned_throughput
                    if temp_throughput < v_expected_throughput:
                        satisfiable_workers = [x[0] for x in workers_unit_cost if nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput]
                        if len(satisfiable_workers) > 0:
                            workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            workers_unit_cost = [(x[0], workers_cost[x[0]] / nodes_workers_execution_throughput[(v, x[0])], workers_cost[x[0]]) for x in workers_unit_cost]
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
            elif tier_allocation == "greedy-backtrace":
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                satisfiable_workers = [x for x in avaiable_workers if nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput]
                if len(satisfiable_workers) > 0:
                    # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                    workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                    workers_unit_cost.sort(key=lambda x: x[1])
                else:
                    # otherwise we sort the workers according to per-input execution cost
                    workers_unit_cost = [(x, workers_cost[x] / nodes_workers_execution_throughput[(v, x)], workers_cost[x]) for x in avaiable_workers]
                    workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                
                temp_assigned_workers = []
                temp_throughput = 0.
                while temp_throughput < v_expected_throughput:
                    if len(workers_unit_cost) == 0:
                        break                
                    worker_idx = workers_unit_cost.pop(0)[0]
                    worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                    temp_assigned_workers.append((worker_idx, worker_max_throughput))
                    temp_throughput += worker_max_throughput
                    if temp_throughput < v_expected_throughput:
                        satisfiable_workers = [x[0] for x in workers_unit_cost if nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput]
                        if len(satisfiable_workers) > 0:
                            workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            workers_unit_cost = [(x[0], workers_cost[x[0]] / nodes_workers_execution_throughput[(v, x[0])], workers_cost[x[0]]) for x in workers_unit_cost]
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                temp_assigned_workers.sort(key=lambda x: x[1])
                start = -1
                removed_throughput = 0
                while removed_throughput < temp_throughput - v_expected_throughput:
                    if removed_throughput + temp_assigned_workers[start + 1][1] > temp_throughput - v_expected_throughput:
                        break 
                    else:
                        start += 1
                        removed_throughput += temp_assigned_workers[start][1]
                temp_assigned_workers_filtered = temp_assigned_workers[start + 1:]
                temp_assigned_workers = []
                temp_throughput = 0.
                for worker_idx, worker_max_throughput in temp_assigned_workers_filtered:
                    if temp_throughput + worker_max_throughput > v_expected_throughput:
                        worker_assigned_throughput = v_expected_throughput - temp_throughput
                    else:
                        worker_assigned_throughput = worker_max_throughput
                    temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                    temp_throughput += worker_assigned_throughput                            
            elif tier_allocation == "dp":
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                avaiable_workers = [(x, workers_cost[1], math.floor(nodes_workers_execution_throughput[(v, x)] * 100.)) for x in avaiable_workers]
                
                num_available_workers = len(avaiable_workers)
                evict_capacity = sum([x[2] for x in avaiable_workers]) - math.ceil(v_expected_throughput * 100.)
                if evict_capacity >= 0:
                    knapsack_dp = np.zeros((num_available_workers + 1, evict_capacity + 1))
                    knapsack_evict = np.zeros((num_available_workers + 1, evict_capacity + 1), dtype=bool)
                    for m in range(1, num_available_workers + 1):
                        for w in range(0, evict_capacity + 1):
                            if avaiable_workers[m - 1][2] > w:
                                knapsack_dp[m, w] = knapsack_dp[m - 1, w]
                            else:
                                knapsack_dp[m, w] = max(knapsack_dp[m - 1, w], knapsack_dp[m - 1, w - avaiable_workers[m - 1][2]] + avaiable_workers[m - 1][1])
                                knapsack_evict[m, w] = True if knapsack_dp[m - 1, w - avaiable_workers[m - 1][2]] + avaiable_workers[m - 1][1] > knapsack_dp[m - 1, w] else False

                    temp_assigned_workers = []
                    temp_throughput = 0.
                    free_capacity = evict_capacity
                    for m in range(num_available_workers, 0, -1):
                        if not knapsack_evict[m, free_capacity]:
                            worker_idx = avaiable_workers[m - 1][0]
                            worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                            if temp_throughput + worker_max_throughput > v_expected_throughput:
                                worker_assigned_throughput = v_expected_throughput - temp_throughput
                            else:
                                worker_assigned_throughput = worker_max_throughput
                            temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                            temp_throughput += worker_assigned_throughput
                            if temp_throughput >= v_expected_throughput:
                                break
                        else:
                            free_capacity -= avaiable_workers[m - 1][2]
                else:
                    temp_assigned_workers = []
                    temp_throughput = 0.                    
            elif tier_allocation == "meet-in-the-middle":
                avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                shuffle(avaiable_workers)
                num_available_workers = len(avaiable_workers)
                workers_set_a = avaiable_workers[:num_available_workers // 2]
                workers_set_b = avaiable_workers[num_available_workers // 2:]
                subsets_a = powerset(workers_set_a)
                subsets_b = powerset(workers_set_b)
                subsets_a_marked = []
                for subset in subsets_a:
                    subset_cost = sum([workers_cost[x] for x in subset])
                    subset_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in subset])
                    subsets_a_marked.append((subset, subset_cost, subset_throughput))
                subsets_b_marked = []
                for subset in subsets_b:
                    subset_cost = sum([workers_cost[x] for x in subset])
                    subset_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in subset])
                    subsets_b_marked.append((subset, subset_cost, subset_throughput))                    
                subsets_b_marked = [subset for subset in subsets_b_marked if not any(x[1] <= subset[1] and x[2] > subset[2] for x in subsets_b_marked)]
                subsets_b_marked.sort(key=lambda subset: subset[2])
                subsets_b_tp = [subset[2] for subset in subsets_b_marked]
                min_cost = np.inf
                min_subset_a = None
                min_subset_b = None
                for (subset_a, cost_a, throughput_a) in subsets_a_marked:
                    min_throughput_b = v_expected_throughput - throughput_a
                    if min_throughput_b <= 0:
                        if cost_a < min_cost:
                            min_cost = cost_a
                            min_subset_a = set(subset_a)
                            min_subset_b = set()
                    else:
                        pos = bisect.bisect_left(subsets_b_tp, min_throughput_b)
                        if pos < len(subsets_b_marked):
                            subset_b, cost_b, throughput_b = min(subsets_b_marked[pos:], key=lambda x: x[1])
                            assert throughput_a + throughput_b >= v_expected_throughput
                            if cost_a + cost_b < min_cost:
                                min_cost = cost_a + cost_b
                                min_subset_a = set(subset_a)
                                min_subset_b = set(subset_b)
                temp_assigned_workers = []
                temp_throughput = 0.
                if not np.isinf(min_cost):
                    for worker_idx in chain(min_subset_a, min_subset_b):
                        worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                        if temp_throughput + worker_max_throughput > v_expected_throughput:
                            worker_assigned_throughput = v_expected_throughput - temp_throughput
                        else:
                            worker_assigned_throughput = worker_max_throughput   
                        temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                        temp_throughput += worker_assigned_throughput
            else:
                raise ValueError("invalid tier worker allocation strategy {}".format(tier_allocation))

            # we have a feasible assignment on tier i
            if temp_throughput >= v_expected_throughput:
                compute_cost = sum([workers_cost[x[0]] for x in temp_assigned_workers])
                communication_cost = 0.
                for u in inputs_nodes:
                    if u in sources_nodes:
                        total_v_throughput = v_expected_throughput
                        total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
                        v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in temp_assigned_workers]
                        for worker_idx, worker_rx in v_per_worker_rx:
                            communication_cost += sources_to_workers_comm_cost[u][worker_idx] * worker_rx * 3600. / 1024. / 1024.
                    else:
                        u_assigned_workers = workers_assignment[u]
                        total_u_throughput = nodes_total_throughput[u]
                        total_v_throughput = v_expected_throughput
                        total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
                        u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                        v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in temp_assigned_workers]                    
                        for u_worker_idx, u_worker_tx in u_per_worker_tx:
                            for v_worker_idx, v_worker_rx in v_per_worker_rx:
                                link_bandwidth = u_worker_tx * (v_worker_rx / total_edge_bandwidth)
                                communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * link_bandwidth * 3600. / 1024. / 1024.
                if compute_cost + communication_cost < min_cost:
                    min_cost = compute_cost + communication_cost
                    min_compute_cost = compute_cost
                    min_cost_tier == i
                    min_cost_assignment = temp_assigned_workers.copy()
            
            if (not no_solution_found_prev_tiers) and (strategy == "cross-tier-one-hop"):
                break
        
        if min_cost_tier is not None:
            for worker_idx, _ in min_cost_assignment:
                workers_is_assigned[worker_idx] = True            
            workers_assignment[v] = min_cost_assignment.copy()
            tiers_assignment[v] = min_cost_tier
            total_compute_cost += min_compute_cost
        else:
            # infeasible
            return None
    
    assignment = {
        "total_compute_cost": total_compute_cost,
        "workers_assignment": workers_assignment
    }
    return assignment


def compute_worker_assignment_all_tiers(
    workers_to_workers_comm_cost,
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
    message_sizes,
    sources_to_workers_comm_cost,
    max_num_assign_orders=None,
    best_k_assignments=None,
    tier_allocation="greedy"
):
    predecessors_list = dict()
    for u, v in logical_graph_edges:
        if v not in predecessors_list:
            predecessors_list[v] = set()
        if u not in predecessors_list:
            predecessors_list[u] = set()
        predecessors_list[v].add(u)
        
    graph = nx.DiGraph(incoming_graph_data=logical_graph_edges)
    all_topological_orders = list(nx.all_topological_sorts(graph))
    if max_num_assign_orders:
        all_topological_orders = all_topological_orders[:max_num_assign_orders]

    nodes_workers_execution_throughput = dict()
    for (v, worker_idx), latency in nodes_workers_execution_latency.items():
        if not np.isnan(latency):
            nodes_workers_execution_throughput[(v, worker_idx)] = 1000. / latency

    sources_nodes = set(sources_to_workers_comm_cost.keys())
    num_workers = workers_to_workers_comm_cost.shape[0]
    num_tiers = len(workers_partition)

    best_workers_assignments = []
    for topological_order in all_topological_orders:
        init_state = {
            "total_overall_cost": 0.,
            "total_compute_cost": 0.,
            "tiers_assignment": dict(),
            "workers_assignment": dict(),
            "workers_is_assigned": np.zeros(num_workers, dtype=bool)
        }
        search_states = [init_state]
        nodes_total_throughput = dict()
        for v in topological_order:
            inputs_nodes = predecessors_list[v]
            if v in sources_nodes:
                v_expected_throughput = input_injection_rates[v]
            else:
                v_expected_throughput = min([nodes_total_throughput[u] * throughput_normalization_ratios[(u, v)] for u in inputs_nodes])
            nodes_total_throughput[v] = v_expected_throughput            
            
            new_states = []
            for state in search_states:
                total_overall_cost = state["total_overall_cost"]
                total_compute_cost = state["total_compute_cost"]
                tiers_assignment = state["tiers_assignment"]
                workers_assignment = state["workers_assignment"]
                workers_is_assigned = state["workers_is_assigned"]

                if v in sources_nodes:
                    new_overall_cost = total_overall_cost
                    new_compute_cost = total_compute_cost
                    new_tiers_assignment = deepcopy(tiers_assignment)
                    new_tiers_assignment[v] = -1
                    new_workers_assignment = deepcopy(workers_assignment)
                    new_workers_is_assigned = deepcopy(workers_is_assigned)
                    new_state = {
                        "total_overall_cost": new_overall_cost,
                        "total_compute_cost": new_compute_cost,
                        "tiers_assignment": new_tiers_assignment,
                        "workers_assignment": new_workers_assignment,
                        "workers_is_assigned": new_workers_is_assigned
                    }
                    new_states.append(new_state)                
                    continue
        
                highest_inputs_tier = max([tiers_assignment[u] for u in inputs_nodes])
                if highest_inputs_tier < 0:
                    highest_inputs_tier = 0

                for i in range(highest_inputs_tier, num_tiers):
                    if tier_allocation == "greedy":
                        avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                        satisfiable_workers = [x for x in avaiable_workers if nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput]
                        if len(satisfiable_workers) > 0:
                            # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                            workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            # otherwise we sort the workers according to per-input execution cost
                            workers_unit_cost = [(x, workers_cost[x] / nodes_workers_execution_throughput[(v, x)], workers_cost[x]) for x in avaiable_workers]
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))

                        temp_assigned_workers = []
                        temp_throughput = 0.                        
                        while temp_throughput < v_expected_throughput:
                            if len(workers_unit_cost) == 0:
                                break
                            worker_idx = workers_unit_cost.pop(0)[0]
                            worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                            if temp_throughput + worker_max_throughput > v_expected_throughput:
                                worker_assigned_throughput = v_expected_throughput - temp_throughput
                            else:
                                worker_assigned_throughput = worker_max_throughput

                            temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                            temp_throughput += worker_assigned_throughput
                            if temp_throughput < v_expected_throughput:
                                satisfiable_workers = [x[0] for x in workers_unit_cost if nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput]
                                if len(satisfiable_workers) > 0:
                                    workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                                    workers_unit_cost.sort(key=lambda x: x[1])
                                else:
                                    workers_unit_cost = [(x[0], workers_cost[x[0]] / nodes_workers_execution_throughput[(v, x[0])], workers_cost[x[0]]) for x in workers_unit_cost]
                                    workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                    elif tier_allocation == "greedy-backtrace":
                        avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                        satisfiable_workers = [x for x in avaiable_workers if nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput]
                        if len(satisfiable_workers) > 0:
                            # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                            workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            # otherwise we sort the workers according to per-input execution cost
                            workers_unit_cost = [(x, workers_cost[x] / nodes_workers_execution_throughput[(v, x)], workers_cost[x]) for x in avaiable_workers]
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                        temp_assigned_workers = []
                        temp_throughput = 0.
                        while temp_throughput < v_expected_throughput:
                            if len(workers_unit_cost) == 0:
                                break                
                            worker_idx = workers_unit_cost.pop(0)[0]
                            worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                            temp_assigned_workers.append((worker_idx, worker_max_throughput))
                            temp_throughput += worker_max_throughput
                            if temp_throughput < v_expected_throughput:
                                satisfiable_workers = [x[0] for x in workers_unit_cost if nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput]
                                if len(satisfiable_workers) > 0:
                                    workers_unit_cost = [(x, workers_cost[x]) for x in satisfiable_workers]
                                    workers_unit_cost.sort(key=lambda x: x[1])
                                else:
                                    workers_unit_cost = [(x[0], workers_cost[x[0]] / nodes_workers_execution_throughput[(v, x[0])], workers_cost[x[0]]) for x in workers_unit_cost]
                                    workers_unit_cost.sort(key=lambda x: (x[1], x[2]))
                        temp_assigned_workers.sort(key=lambda x: x[1])
                        start = -1
                        removed_throughput = 0
                        while removed_throughput < temp_throughput - v_expected_throughput:
                            if removed_throughput + temp_assigned_workers[start + 1][1] > temp_throughput - v_expected_throughput:
                                break 
                            else:
                                start += 1
                                removed_throughput += temp_assigned_workers[start][1]
                        temp_assigned_workers_filtered = temp_assigned_workers[start + 1:]
                        temp_assigned_workers = []
                        temp_throughput = 0.
                        for worker_idx, worker_max_throughput in temp_assigned_workers_filtered:
                            if temp_throughput + worker_max_throughput > v_expected_throughput:
                                worker_assigned_throughput = v_expected_throughput - temp_throughput
                            else:
                                worker_assigned_throughput = worker_max_throughput
                            temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                            temp_throughput += worker_assigned_throughput     
                    elif tier_allocation == "dp":
                        avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                        avaiable_workers = [(x, workers_cost[1], math.floor(nodes_workers_execution_throughput[(v, x)] * 100.)) for x in avaiable_workers]
                        
                        num_available_workers = len(avaiable_workers)
                        evict_capacity = sum([x[2] for x in avaiable_workers]) - math.ceil(v_expected_throughput * 100.)
                        if evict_capacity >= 0:
                            knapsack_dp = np.zeros((num_available_workers + 1, evict_capacity + 1))
                            knapsack_evict = np.zeros((num_available_workers + 1, evict_capacity + 1), dtype=bool)
                            for m in range(1, num_available_workers + 1):
                                for w in range(0, evict_capacity + 1):
                                    if avaiable_workers[m - 1][2] > w:
                                        knapsack_dp[m, w] = knapsack_dp[m - 1, w]
                                    else:
                                        knapsack_dp[m, w] = max(knapsack_dp[m - 1, w], knapsack_dp[m - 1, w - avaiable_workers[m - 1][2]] + avaiable_workers[m - 1][1])
                                        knapsack_evict[m, w] = True if knapsack_dp[m - 1, w - avaiable_workers[m - 1][2]] + avaiable_workers[m - 1][1] > knapsack_dp[m - 1, w] else False

                            temp_assigned_workers = []
                            temp_throughput = 0.
                            free_capacity = evict_capacity
                            for m in range(num_available_workers, 0, -1):
                                if not knapsack_evict[m, free_capacity]:
                                    worker_idx = avaiable_workers[m - 1][0]
                                    worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                                    if temp_throughput + worker_max_throughput > v_expected_throughput:
                                        worker_assigned_throughput = v_expected_throughput - temp_throughput
                                    else:
                                        worker_assigned_throughput = worker_max_throughput
                                    temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                                    temp_throughput += worker_assigned_throughput
                                    if temp_throughput >= v_expected_throughput:
                                        break
                                else:
                                    free_capacity -= avaiable_workers[m - 1][2]
                        else:
                            temp_assigned_workers = []
                            temp_throughput = 0. 
                    elif tier_allocation == "meet-in-the-middle":
                        avaiable_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                        shuffle(avaiable_workers)
                        num_available_workers = len(avaiable_workers)
                        workers_set_a = avaiable_workers[:num_available_workers // 2]
                        workers_set_b = avaiable_workers[num_available_workers // 2:]
                        subsets_a = powerset(workers_set_a)
                        subsets_b = powerset(workers_set_b)
                        subsets_a_marked = []
                        for subset in subsets_a:
                            subset_cost = sum([workers_cost[x] for x in subset])
                            subset_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in subset])
                            subsets_a_marked.append((subset, subset_cost, subset_throughput))
                        subsets_b_marked = []
                        for subset in subsets_b:
                            subset_cost = sum([workers_cost[x] for x in subset])
                            subset_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in subset])
                            subsets_b_marked.append((subset, subset_cost, subset_throughput))                    
                        subsets_b_marked = [subset for subset in subsets_b_marked if not any(x[1] <= subset[1] and x[2] > subset[2] for x in subsets_b_marked)]
                        subsets_b_marked.sort(key=lambda subset: subset[2])
                        subsets_b_tp = [subset[2] for subset in subsets_b_marked]
                        min_cost = np.inf
                        min_subset_a = None
                        min_subset_b = None
                        for (subset_a, cost_a, throughput_a) in subsets_a_marked:
                            min_throughput_b = v_expected_throughput - throughput_a
                            if min_throughput_b <= 0:
                                if cost_a < min_cost:
                                    min_cost = cost_a
                                    min_subset_a = set(subset_a)
                                    min_subset_b = set()
                            else:
                                pos = bisect.bisect_left(subsets_b_tp, min_throughput_b)
                                if pos < len(subsets_b_marked):
                                    subset_b, cost_b, throughput_b = min(subsets_b_marked[pos:], key=lambda x: x[1])
                                    assert throughput_a + throughput_b >= v_expected_throughput
                                    if cost_a + cost_b < min_cost:
                                        min_cost = cost_a + cost_b
                                        min_subset_a = set(subset_a)
                                        min_subset_b = set(subset_b)
                        temp_assigned_workers = []
                        temp_throughput = 0.
                        if not np.isinf(min_cost):
                            for worker_idx in chain(min_subset_a, min_subset_b):
                                worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                                if temp_throughput + worker_max_throughput > v_expected_throughput:
                                    worker_assigned_throughput = v_expected_throughput - temp_throughput
                                else:
                                    worker_assigned_throughput = worker_max_throughput   
                                temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                                temp_throughput += worker_assigned_throughput
                    else:
                        raise ValueError("invalid tier worker allocation strategy {}".format(tier_allocation))

                    if temp_throughput >= v_expected_throughput:
                        v_compute_cost = sum([workers_cost[x[0]] for x in temp_assigned_workers])
                        v_communication_cost = 0.
                        for u in inputs_nodes:
                            if u in sources_nodes:
                                total_v_throughput = v_expected_throughput
                                total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
                                v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in temp_assigned_workers]
                                for worker_idx, worker_rx in v_per_worker_rx:
                                    v_communication_cost += sources_to_workers_comm_cost[u][worker_idx] * worker_rx * 3600. / 1024. / 1024.
                            else:
                                u_assigned_workers = workers_assignment[u]
                                total_u_throughput = nodes_total_throughput[u]
                                total_v_throughput = v_expected_throughput
                                total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
                                u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in temp_assigned_workers]                    
                                for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                    for v_worker_idx, v_worker_rx in v_per_worker_rx:
                                        link_bandwidth = u_worker_tx * (v_worker_rx / total_edge_bandwidth)
                                        v_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * link_bandwidth  * 3600. / 1024. / 1024.                      

                        new_overall_cost = total_overall_cost + v_compute_cost + v_communication_cost
                        new_compute_cost = total_compute_cost + v_compute_cost
                        new_tiers_assignment = deepcopy(tiers_assignment)
                        new_tiers_assignment[v] = i
                        new_workers_assignment = deepcopy(workers_assignment)
                        new_workers_assignment[v] = temp_assigned_workers.copy()
                        new_workers_is_assigned = deepcopy(workers_is_assigned)
                        for worker_idx, _ in temp_assigned_workers:
                            new_workers_is_assigned[worker_idx] = True
                        new_state = {
                            "total_overall_cost": new_overall_cost,
                            "total_compute_cost": new_compute_cost,
                            "tiers_assignment": new_tiers_assignment,
                            "workers_assignment": new_workers_assignment,
                            "workers_is_assigned": new_workers_is_assigned
                        }
                        new_states.append(new_state)

            new_states.sort(key=lambda x: x["total_overall_cost"])
            if best_k_assignments:
                search_states = new_states[:best_k_assignments]
    
        for state in search_states:
            assignment = {
                "total_overall_cost": state["total_overall_cost"],
                "total_compute_cost": state["total_compute_cost"],
                "workers_assignment": state["workers_assignment"]
            }
            best_workers_assignments.append(assignment)
        best_workers_assignments.sort(key=lambda x: x["total_overall_cost"])
        if best_k_assignments:
            best_workers_assignments = best_workers_assignments[:best_k_assignments]

    if len(best_workers_assignments) == 0:
        return None        
    return best_workers_assignments 


def compute_worker_assignment_multiple_tiers(
    workers_to_workers_comm_cost,
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
    message_sizes,
    sources_to_workers_comm_cost,
    max_num_assign_orders=None,
    best_k_assignments=None,
    tier_allocation="greedy"
):
    predecessors_list = dict()
    for u, v in logical_graph_edges:
        if v not in predecessors_list:
            predecessors_list[v] = set()
        if u not in predecessors_list:
            predecessors_list[u] = set()
        predecessors_list[v].add(u)
        
    graph = nx.DiGraph(incoming_graph_data=logical_graph_edges)
    all_topological_orders = list(nx.all_topological_sorts(graph))
    if max_num_assign_orders:
        all_topological_orders = all_topological_orders[:max_num_assign_orders]

    nodes_workers_execution_throughput = dict()
    for (v, worker_idx), latency in nodes_workers_execution_latency.items():
        if not np.isnan(latency):
            nodes_workers_execution_throughput[(v, worker_idx)] = 1000. / latency

    sources_nodes = set(sources_to_workers_comm_cost.keys())
    workers_partition_mapping = dict()
    for idx, partition in enumerate(workers_partition):
        for worker_idx in partition:
            if worker_idx in workers_partition_mapping:
                raise ValueError("worker {} is in multiple partitions".format(worker_idx))
            workers_partition_mapping[worker_idx] = idx
    num_workers = workers_to_workers_comm_cost.shape[0]
    num_tiers = len(workers_partition)

    best_workers_assignments = []
    for topological_order in all_topological_orders:
        init_state = {
            "total_overall_cost": 0.,
            "total_compute_cost": 0.,
            "tiers_assignment": dict(),
            "workers_assignment": dict(),
            "workers_is_assigned": np.zeros(num_workers, dtype=bool)
        }
        search_states = [init_state]
        nodes_total_throughput = dict()
        for v in topological_order:
            inputs_nodes = predecessors_list[v]
            if v in sources_nodes:
                v_expected_throughput = input_injection_rates[v]
            else:
                v_expected_throughput = min([nodes_total_throughput[u] * throughput_normalization_ratios[(u, v)] for u in inputs_nodes])
            nodes_total_throughput[v] = v_expected_throughput            
            
            new_states = []
            for state in search_states:
                total_overall_cost = state["total_overall_cost"]
                total_compute_cost = state["total_compute_cost"]
                tiers_assignment = state["tiers_assignment"]
                workers_assignment = state["workers_assignment"]
                workers_is_assigned = state["workers_is_assigned"]

                if v in sources_nodes:
                    new_overall_cost = total_overall_cost
                    new_compute_cost = total_compute_cost
                    new_tiers_assignment = deepcopy(tiers_assignment)
                    new_tiers_assignment[v] = -1
                    new_workers_assignment = deepcopy(workers_assignment)
                    new_workers_is_assigned = deepcopy(workers_is_assigned)
                    new_state = {
                        "total_overall_cost": new_overall_cost,
                        "total_compute_cost": new_compute_cost,
                        "tiers_assignment": new_tiers_assignment,
                        "workers_assignment": new_workers_assignment,
                        "workers_is_assigned": new_workers_is_assigned
                    }
                    new_states.append(new_state)                
                    continue
        
                highest_inputs_tier = max([tiers_assignment[u] for u in inputs_nodes])
                if highest_inputs_tier < 0:
                    highest_inputs_tier = 0

                # place the node up to tier i, the node can use any available workers from tier `highest_inputs_tier` to tier i
                for i in range(highest_inputs_tier, num_tiers):
                    if tier_allocation == "greedy":
                        avaiable_workers = [x for x in range(num_workers) if (workers_partition_mapping[x] >= highest_inputs_tier) and (workers_partition_mapping[x] <= i) and (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                        lowest_tier = min([workers_partition_mapping[x] for x in avaiable_workers], default=0)
                        satisfiable_workers = [x for x in avaiable_workers if (workers_partition_mapping[x] == lowest_tier) and (nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput)] 
                        if len(satisfiable_workers) > 0:
                            # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                            workers_unit_cost = []
                            for v_worker_idx in satisfiable_workers:
                                v_worker_communication_cost = 0.
                                for u in inputs_nodes:
                                    if u in sources_nodes:
                                        total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                        v_worker_communication_cost += sources_to_workers_comm_cost[u][v_worker_idx] * total_edge_bandwidth * 3600. / 1024. / 1024.
                                    else:
                                        u_assigned_workers = workers_assignment[u]
                                        total_u_throughput = nodes_total_throughput[u]
                                        total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                        u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                        for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                            v_worker_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * u_worker_tx * 3600. / 1024. / 1024.
                                workers_unit_cost.append((v_worker_idx, workers_cost[v_worker_idx] + v_worker_communication_cost))
                            workers_unit_cost.sort(key=lambda x: (x[1], nodes_workers_execution_throughput[(v, x[0])]))
                        else:
                            workers_unit_cost = []
                            for v_worker_idx in avaiable_workers:
                                v_worker_communication_cost = 0.
                                v_worker_throughput = nodes_workers_execution_throughput[(v, v_worker_idx)]
                                for u in inputs_nodes:
                                    if u in sources_nodes:
                                        edge_bandwidth = v_worker_throughput * message_sizes[(u, v)]
                                        v_worker_communication_cost += sources_to_workers_comm_cost[u][v_worker_idx] * edge_bandwidth * 3600. / 1024. / 1024.
                                    else:
                                        u_assigned_workers = workers_assignment[u]
                                        total_u_throughput = nodes_total_throughput[u]
                                        total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                        u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                        for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                            link_bandwidth = u_worker_tx * v_worker_throughput / v_expected_throughput
                                            v_worker_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * link_bandwidth * 3600. / 1024. / 1024.
                                workers_unit_cost.append((v_worker_idx,
                                    (workers_cost[v_worker_idx] + v_worker_communication_cost) / v_worker_throughput,
                                    workers_cost[v_worker_idx] + v_worker_communication_cost
                                ))
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))

                        temp_assigned_workers = []
                        temp_throughput = 0.                        
                        while temp_throughput < v_expected_throughput:
                            if len(workers_unit_cost) == 0:
                                break
                            worker_idx = workers_unit_cost.pop(0)[0]
                            worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                            if temp_throughput + worker_max_throughput > v_expected_throughput:
                                worker_assigned_throughput = v_expected_throughput - temp_throughput
                            else:
                                worker_assigned_throughput = worker_max_throughput
                            temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                            temp_throughput += worker_assigned_throughput
                            if temp_throughput < v_expected_throughput:
                                lowest_tier = min([workers_partition_mapping[x[0]] for x in workers_unit_cost], default=0)
                                #satisfiable_workers = [x for x in avaiable_workers if workers_partition_mapping[x] == lowest_tier and (nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput)]                                 
                                satisfiable_workers = [x[0] for x in workers_unit_cost if (workers_partition_mapping[x[0]] == lowest_tier) and (nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput)]
                                if len(satisfiable_workers) > 0:
                                    workers_unit_cost = []
                                    for v_worker_idx in satisfiable_workers:
                                        v_worker_communication_cost = 0.
                                        for u in inputs_nodes:
                                            if u in sources_nodes:
                                                edge_bandwidth = (v_expected_throughput - temp_throughput) * message_sizes[(u, v)]
                                                v_worker_communication_cost += sources_to_workers_comm_cost[u][v_worker_idx] * edge_bandwidth * 3600. / 1024. / 1024.
                                            else:
                                                u_assigned_workers = workers_assignment[u]
                                                total_u_throughput = nodes_total_throughput[u]
                                                total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                                u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                                for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                                    link_bandwidth = u_worker_tx * (v_expected_throughput - temp_throughput) / v_expected_throughput
                                                    v_worker_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * u_worker_tx * 3600. / 1024. / 1024.                             
                                        workers_unit_cost.append((v_worker_idx, workers_cost[v_worker_idx] + v_worker_communication_cost))
                                    workers_unit_cost.sort(key=lambda x: (x[1], nodes_workers_execution_throughput[(v, x[0])]))
                    elif tier_allocation == "greedy-backtrace":
                        avaiable_workers = [x for x in range(num_workers) if (workers_partition_mapping[x] >= highest_inputs_tier) and (workers_partition_mapping[x] <= i) and (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                        lowest_tier = min([workers_partition_mapping[x] for x in avaiable_workers], default=0)
                        satisfiable_workers = [x for x in avaiable_workers if (workers_partition_mapping[x] == lowest_tier) and (nodes_workers_execution_throughput[(v, x)] >= v_expected_throughput)] 
                        if len(satisfiable_workers) > 0:
                            # if there are workers on this tier that can satisfy the input rate with the worker alone, we just use the worker with lowest price
                            workers_unit_cost = []
                            for v_worker_idx in satisfiable_workers:
                                v_worker_communication_cost = 0.
                                for u in inputs_nodes:
                                    if u in sources_nodes:
                                        total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                        v_worker_communication_cost += sources_to_workers_comm_cost[u][v_worker_idx] * total_edge_bandwidth * 3600. / 1024. / 1024.
                                    else:
                                        u_assigned_workers = workers_assignment[u]
                                        total_u_throughput = nodes_total_throughput[u]
                                        total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                        u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                        for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                            v_worker_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * u_worker_tx * 3600. / 1024. / 1024.
                                workers_unit_cost.append((v_worker_idx, workers_cost[v_worker_idx] + v_worker_communication_cost))
                            workers_unit_cost.sort(key=lambda x: x[1])
                        else:
                            workers_unit_cost = []
                            for v_worker_idx in avaiable_workers:
                                v_worker_communication_cost = 0.
                                v_worker_throughput = nodes_workers_execution_throughput[(v, v_worker_idx)]
                                for u in inputs_nodes:
                                    if u in sources_nodes:
                                        edge_bandwidth = v_worker_throughput * message_sizes[(u, v)]
                                        v_worker_communication_cost += sources_to_workers_comm_cost[u][v_worker_idx] * edge_bandwidth * 3600. / 1024. / 1024.
                                    else:
                                        u_assigned_workers = workers_assignment[u]
                                        total_u_throughput = nodes_total_throughput[u]
                                        total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                        u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                        for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                            link_bandwidth = u_worker_tx * v_worker_throughput / v_expected_throughput
                                            v_worker_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * link_bandwidth * 3600. / 1024. / 1024.                       
                                workers_unit_cost.append((v_worker_idx,
                                    (workers_cost[v_worker_idx] + v_worker_communication_cost) / v_worker_throughput,
                                    workers_cost[v_worker_idx] + v_worker_communication_cost
                                ))
                            workers_unit_cost.sort(key=lambda x: (x[1], x[2]))

                        temp_assigned_workers = []
                        temp_throughput = 0.                        
                        while temp_throughput < v_expected_throughput:
                            if len(workers_unit_cost) == 0:
                                break
                            worker_idx = workers_unit_cost.pop(0)[0]
                            worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                            temp_assigned_workers.append((worker_idx, worker_max_throughput))
                            temp_throughput += worker_max_throughput
                            if temp_throughput < v_expected_throughput:
                                lowest_tier = min([workers_partition_mapping[x[0]] for x in workers_unit_cost], default=0)
                                satisfiable_workers = [x[0] for x in workers_unit_cost if (workers_partition_mapping[x[0]] == lowest_tier) and (nodes_workers_execution_throughput[(v, x[0])] > v_expected_throughput - temp_throughput)]
                                if len(satisfiable_workers) > 0:
                                    workers_unit_cost = []
                                    for v_worker_idx in satisfiable_workers:
                                        v_worker_communication_cost = 0.
                                        for u in inputs_nodes:
                                            if u in sources_nodes:
                                                edge_bandwidth = (v_expected_throughput - temp_throughput) * message_sizes[(u, v)]
                                                v_worker_communication_cost += sources_to_workers_comm_cost[u][v_worker_idx] * edge_bandwidth * 3600. / 1024. / 1024.
                                            else:
                                                u_assigned_workers = workers_assignment[u]
                                                total_u_throughput = nodes_total_throughput[u]
                                                total_edge_bandwidth = v_expected_throughput * message_sizes[(u, v)]
                                                u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                                for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                                    link_bandwidth = u_worker_tx * (v_expected_throughput - temp_throughput) / v_expected_throughput
                                                    v_worker_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * u_worker_tx * 3600. / 1024. / 1024.
                                        workers_unit_cost.append((v_worker_idx, workers_cost[v_worker_idx] + v_worker_communication_cost))
                                    workers_unit_cost.sort(key=lambda x: x[1])
                        temp_assigned_workers.sort(key=lambda x: x[1])
                        start = -1
                        removed_throughput = 0
                        while removed_throughput < temp_throughput - v_expected_throughput:
                            if removed_throughput + temp_assigned_workers[start + 1][1] > temp_throughput - v_expected_throughput:
                                break 
                            else:
                                start += 1
                                removed_throughput += temp_assigned_workers[start][1]
                        temp_assigned_workers_filtered = temp_assigned_workers[start + 1:]
                        temp_assigned_workers = []
                        temp_throughput = 0.
                        for worker_idx, worker_max_throughput in temp_assigned_workers_filtered:
                            if temp_throughput + worker_max_throughput > v_expected_throughput:
                                worker_assigned_throughput = v_expected_throughput - temp_throughput
                            else:
                                worker_assigned_throughput = worker_max_throughput
                            temp_assigned_workers.append((worker_idx, worker_assigned_throughput))
                            temp_throughput += worker_assigned_throughput                    
                    else:
                        raise ValueError("invalid tier worker allocation strategy {}".format(tier_allocation))

                    if temp_throughput >= v_expected_throughput:
                        v_compute_cost = sum([workers_cost[x[0]] for x in temp_assigned_workers])
                        v_communication_cost = 0.
                        for u in inputs_nodes:
                            if u in sources_nodes:
                                total_v_throughput = v_expected_throughput
                                total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
                                v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in temp_assigned_workers]
                                for worker_idx, worker_rx in v_per_worker_rx:
                                    v_communication_cost += sources_to_workers_comm_cost[u][worker_idx] * worker_rx * 3600. / 1024. / 1024.
                            else:
                                u_assigned_workers = workers_assignment[u]
                                total_u_throughput = nodes_total_throughput[u]
                                total_v_throughput = v_expected_throughput
                                total_edge_bandwidth = total_v_throughput * message_sizes[(u, v)]
                                u_per_worker_tx = [(x[0], x[1] * total_edge_bandwidth / total_u_throughput) for x in u_assigned_workers]
                                v_per_worker_rx = [(x[0], x[1] * total_edge_bandwidth / total_v_throughput) for x in temp_assigned_workers]                    
                                for u_worker_idx, u_worker_tx in u_per_worker_tx:
                                    for v_worker_idx, v_worker_rx in v_per_worker_rx:
                                        link_bandwidth = u_worker_tx * (v_worker_rx / total_edge_bandwidth)
                                        v_communication_cost += workers_to_workers_comm_cost[u_worker_idx, v_worker_idx] * link_bandwidth * 3600. / 1024. / 1024.

                        new_overall_cost = total_overall_cost + v_compute_cost + v_communication_cost
                        new_compute_cost = total_compute_cost + v_compute_cost
                        new_tiers_assignment = deepcopy(tiers_assignment)
                        # v is placed on a tier >= highest of the inputs and <= i
                        new_tiers_assignment[v] = max([workers_partition_mapping[x[0]] for x in temp_assigned_workers])
                        new_workers_assignment = deepcopy(workers_assignment)
                        new_workers_assignment[v] = temp_assigned_workers.copy()
                        new_workers_is_assigned = deepcopy(workers_is_assigned)
                        for worker_idx, _ in temp_assigned_workers:
                            new_workers_is_assigned[worker_idx] = True
                        new_state = {
                            "total_overall_cost": new_overall_cost,
                            "total_compute_cost": new_compute_cost,
                            "tiers_assignment": new_tiers_assignment,
                            "workers_assignment": new_workers_assignment,
                            "workers_is_assigned": new_workers_is_assigned
                        }
                        new_states.append(new_state)

            new_states.sort(key=lambda x: x["total_overall_cost"])
            if best_k_assignments:
                search_states = new_states[:best_k_assignments]
    
        for state in search_states:
            assignment = {
                "total_overall_cost": state["total_overall_cost"],
                "total_compute_cost": state["total_compute_cost"],
                "workers_assignment": state["workers_assignment"]
            }
            best_workers_assignments.append(assignment)
        best_workers_assignments.sort(key=lambda x: x["total_overall_cost"])
        if best_k_assignments:
            best_workers_assignments = best_workers_assignments[:best_k_assignments]

    if len(best_workers_assignments) == 0:
        return None        
    return best_workers_assignments     