from copy import deepcopy
from graphlib import TopologicalSorter
from itertools import chain, combinations
import numpy as np


def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))


def search_all_worker_assignment(
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
    restrict_partition=False
):
    if restrict_partition:
        return search_all_worker_assignment_with_tier_restriction(
            workers_partition,
            workers_cost,
            logical_graph_edges,
            nodes_workers_execution_latency,
            input_injection_rates,
            throughput_normalization_ratios,
        )
    else:
        return search_all_worker_assignment_without_tier_restriction(
            workers_cost,
            logical_graph_edges,
            nodes_workers_execution_latency,
            input_injection_rates,
            throughput_normalization_ratios,
        )


def search_all_worker_assignment_with_tier_restriction(
    workers_partition,
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
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
    
    sources_nodes = set(input_injection_rates.keys())
    num_workers = len(workers_cost)
    num_tiers = len(workers_partition)
    nodes_total_throughput = dict()
    init_state = {
        "workers_assignment": dict(),
        "tiers_assignment": dict(),
        "workers_is_assigned": np.zeros(num_workers, dtype=bool)
    }
    search_states = [init_state]
    for v in topological_order:
        inputs_nodes = predecessors_list[v]
        if v in sources_nodes:
            v_expected_throughput = input_injection_rates[v]
        else:
            v_expected_throughput = min([nodes_total_throughput[u] * throughput_normalization_ratios[(u, v)] for u in inputs_nodes])
        nodes_total_throughput[v] = v_expected_throughput
        
        new_states = []
        for state in search_states:
            tiers_assignment = state["tiers_assignment"]
            workers_assignment = state["workers_assignment"]
            workers_is_assigned = state["workers_is_assigned"]
            
            if v in sources_nodes:
                new_tiers_assignment = deepcopy(tiers_assignment)
                new_tiers_assignment[v] = -1
                new_workers_assignment = deepcopy(workers_assignment)
                new_workers_is_assigned = deepcopy(workers_is_assigned)
                new_state = {
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
                available_workers = [x for x in workers_partition[i] if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
                available_workers_sets = powerset(available_workers)
                for workers_set in available_workers_sets:
                    workers_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in workers_set])
                    if workers_throughput < v_expected_throughput:
                        # infeasible worker assignment
                        continue
                    new_tiers_assignment = deepcopy(tiers_assignment)
                    new_tiers_assignment[v] = i                    
                    new_workers_assignment = deepcopy(workers_assignment)
                    new_workers_is_assigned = deepcopy(workers_is_assigned)
                    temp_throughput = 0.
                    assigned_workers = []
                    for worker_idx in workers_set:
                        worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                        if temp_throughput + worker_max_throughput > v_expected_throughput:
                            worker_assigned_throughput = v_expected_throughput - temp_throughput
                        else:
                            worker_assigned_throughput = worker_max_throughput    
                        assigned_workers.append((worker_idx, worker_assigned_throughput))
                        temp_throughput += worker_assigned_throughput                
                    new_workers_assignment[v] = assigned_workers                   
                    for worker_idx in workers_set:
                        new_workers_is_assigned[worker_idx] = True
                    new_state = {
                        "tiers_assignment": new_tiers_assignment,
                        "workers_assignment": new_workers_assignment,
                        "workers_is_assigned": new_workers_is_assigned
                    }
                    new_states.append(new_state)
        search_states = new_states
    
    all_assignments = []
    for plan in search_states:
        total_compute_cost = 0.
        for v in topological_order:
            if v in sources_nodes:
                continue
            else:
                assigned_workers_set = plan["workers_assignment"][v]
                total_compute_cost += sum([workers_cost[x[0]] for x in assigned_workers_set])
        assignment =  {
            "total_compute_cost": total_compute_cost,
            "workers_assignment": plan["workers_assignment"]
        }
        all_assignments.append(assignment)
    
    return all_assignments


def search_all_worker_assignment_without_tier_restriction(
    workers_cost,
    logical_graph_edges,
    nodes_workers_execution_latency,
    input_injection_rates,
    throughput_normalization_ratios,
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
    
    sources_nodes = set(input_injection_rates.keys())
    num_workers = len(workers_cost)
    nodes_total_throughput = dict()
    init_state = {
        "workers_assignment": dict(),
        "workers_is_assigned": np.zeros(num_workers, dtype=bool)
    }
    search_states = [init_state]
    for v in topological_order:
        inputs_nodes = predecessors_list[v]
        if v in sources_nodes:
            v_expected_throughput = input_injection_rates[v]
        else:
            v_expected_throughput = min([nodes_total_throughput[u] * throughput_normalization_ratios[(u, v)] for u in inputs_nodes])
        nodes_total_throughput[v] = v_expected_throughput
        
        new_states = []
        for state in search_states:
            workers_assignment = state["workers_assignment"]
            workers_is_assigned = state["workers_is_assigned"]
            if v in sources_nodes:
                new_workers_assignment = deepcopy(workers_assignment)
                new_workers_is_assigned = deepcopy(workers_is_assigned)
                new_state = {
                    "workers_assignment": new_workers_assignment,
                    "workers_is_assigned": new_workers_is_assigned
                }
                new_states.append(new_state)                
                continue

            available_workers = [x for x in range(num_workers) if (not workers_is_assigned[x]) and ((v, x) in nodes_workers_execution_throughput)]
            available_workers_sets = powerset(available_workers)
            for workers_set in available_workers_sets:
                workers_throughput = sum([nodes_workers_execution_throughput[(v, x)] for x in workers_set])
                if workers_throughput < v_expected_throughput:
                    # infeasible worker assignment
                    continue
                new_workers_assignment = deepcopy(workers_assignment)
                new_workers_is_assigned = deepcopy(workers_is_assigned)
                temp_throughput = 0.
                assigned_workers = []
                for worker_idx in workers_set:
                    worker_max_throughput = nodes_workers_execution_throughput[(v, worker_idx)]
                    if temp_throughput + worker_max_throughput > v_expected_throughput:
                        worker_assigned_throughput = v_expected_throughput - temp_throughput
                    else:
                        worker_assigned_throughput = worker_max_throughput    
                    assigned_workers.append((worker_idx, worker_assigned_throughput))
                    temp_throughput += worker_assigned_throughput                
                new_workers_assignment[v] = assigned_workers    
                for worker_idx in workers_set:
                    new_workers_is_assigned[worker_idx] = True
                new_state = {
                    "workers_assignment": new_workers_assignment,
                    "workers_is_assigned": new_workers_is_assigned
                }
                new_states.append(new_state)
        search_states = new_states
    
    all_assignments = []
    for plan in search_states:
        total_compute_cost = 0.
        for v in topological_order:
            if v in sources_nodes:
                continue
            else:
                assigned_workers_set = plan["workers_assignment"][v]
                total_compute_cost += sum([workers_cost[x[0]] for x in assigned_workers_set])
        assignment =  {
            "total_compute_cost": total_compute_cost,
            "workers_assignment": plan["workers_assignment"]
        }
        all_assignments.append(assignment)
    
    return all_assignments