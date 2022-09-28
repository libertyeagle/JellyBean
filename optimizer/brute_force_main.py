import argparse
import numpy as np
import json
import yaml
from tqdm import tqdm

from config import get_cfg_defaults
from brute_force_search import search_all_worker_assignment
from tier_partition import partition_workers_into_tiers
from utils import read_inputs, convert_inputs_to_index_based, assign_model_variants
from communication_cost import compute_communication_cost

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", type=str, required=True,
                        help="Input data dir that contians all required files.")
    parser.add_argument("--config", type=str, required=False,
                        help="Configuration files to specify beam search and input processing strategy.")
    parser.add_argument("--output", type=str, required=False,
                        help="Path to output worker assignment.")
    parser.add_argument('--tiers', action=argparse.BooleanOptionalAction,
                        help="Impose tier placement restrictions.")
    parser.add_argument('--nobackward', action=argparse.BooleanOptionalAction,
                        help="Does not allow backward (tier) traffic.")

    args = parser.parse_args()
    
    cfg = get_cfg_defaults()
    if args.config:
        cfg.merge_from_file(args.config)
    cfg.freeze()

    inputs = read_inputs(args.inputs)
    input_injection_rates = inputs["input_injection_rates"]
    nodes_accuracy_profiles = inputs["nodes_accuracy_profiles"]
    accuracy_threshold = inputs["accuracy_threshold"]
    nodes_workers_execution_profiles = inputs["nodes_workers_execution_profiles"]
    message_sizes_profiles = inputs["message_sizes_profiles"]
    workers_partition = inputs["workers_partition"]
    workers_workers_link_comm_cost = inputs["workers_workers_link_comm_cost"]
    workers_cost = inputs["workers_cost"]
    logical_graph_edges = inputs["logical_graph_edges"]
    throughput_normalization_ratios = inputs["throughput_normalization_ratios"]
    sources_workers_link_comm_cost = inputs["sources_workers_link_comm_cost"]

    inputs_converted = convert_inputs_to_index_based(
        workers_workers_link_comm_cost=workers_workers_link_comm_cost,
        workers_cost=workers_cost,
        logical_graph_edges=logical_graph_edges,
        input_injection_rates=input_injection_rates, 
        throughput_normalization_ratios=throughput_normalization_ratios,
        sources_workers_link_comm_cost=sources_workers_link_comm_cost,
        workers_partition=workers_partition,
        comm_cost_scaling_factor=cfg.INPUT.COMM_COST_SCALING_FACTOR
    )
    idx_workers_mapping = inputs_converted["idx_workers_mapping"]
    idx_nodes_mapping = inputs_converted["idx_nodes_mapping"]

    end_to_end_acc_profile = nodes_accuracy_profiles["end_to_end_profile"]
    model_assignments = []
    for model_combs, acc in end_to_end_acc_profile.acc_profile.items():
        if acc < accuracy_threshold:
            continue
        variants_assignment = {}
        for op, variant in zip(end_to_end_acc_profile.operators, model_combs):
            variants_assignment[op] = variant
        model_assignment = {
            "model_assignment": variants_assignment,
            "accuracy": acc
        }
        model_assignments.append(model_assignment)

    workers_partition = inputs_converted["workers_partition"]
    if workers_partition is None:
        kwargs = {
            "strategy": cfg.WORKER_PARTITION.STRATEGY,
            "num_tiers": cfg.WORKER_PARTITION.NUM_TIERS,
            "louvain_resolution": cfg.WORKER_PARTITION.LOUVAIN_RESOLUTION,
            "louvain_threshold": cfg.WORKER_PARTITION.LOUVAIN_THRESHOLD,
            "greedy_modularity_resolution": cfg.WORKER_PARTITION.GREEDY_MODULARITY_RESOLUTION
        }
        workers_partition = partition_workers_into_tiers(
            inputs_converted["workers_to_workers_comm_cost"].copy(),
            inputs_converted["workers_cost"],
            **kwargs
        )
    
    min_overall_cost = np.inf
    max_acc = -np.inf
    best_assignment_compute_cost = np.inf
    best_worker_assignment = None
    best_model_assignment = None
    num_states = 0
    for model_assignment in tqdm(model_assignments):
        nodes_workers_execution_latency, message_sizes, throughput_normalization_coefficients = assign_model_variants(
            models_assignment=model_assignment["model_assignment"],
            node_idx_mapping=inputs_converted["nodes_idx_mapping"],
            worker_idx_mapping=inputs_converted["workers_idx_mapping"],
            nodes_workers_execution_profiles=nodes_workers_execution_profiles,
            message_sizes_profiles=message_sizes_profiles,
            throughput_normalization_ratios=inputs_converted["throughput_normalization_ratios"],
            latency_overestimate_ratio=cfg.INPUT.LATENCY_OVERESTIMATE_RATIO
        )
        worker_assignments = search_all_worker_assignment(
            workers_partition=workers_partition,
            workers_cost=inputs_converted["workers_cost"],
            logical_graph_edges=inputs_converted["logical_graph_edges"],
            nodes_workers_execution_latency=nodes_workers_execution_latency,
            input_injection_rates=inputs_converted["input_injection_rates"],
            throughput_normalization_ratios=throughput_normalization_coefficients,
            restrict_partition=args.tiers
        ) 
        num_states += len(worker_assignments)
        if worker_assignments is None:
            continue
        if not isinstance(worker_assignments, list):
            worker_assignments = [worker_assignments]
        for worker_assignment in worker_assignments:
            comm_cost = compute_communication_cost(
                workers_to_workers_comm_cost=inputs_converted["workers_to_workers_comm_cost"],
                workers_partition=workers_partition,
                logical_graph_edges=inputs_converted["logical_graph_edges"],
                workers_assignment=worker_assignment["workers_assignment"],
                message_sizes=message_sizes,
                sources_to_workers_comm_cost=inputs_converted["sources_to_workers_comm_cost"],
                allow_backward_traffic=not args.nobackward
            )

            same_cost_higher_acc = np.allclose(worker_assignment["total_compute_cost"] + comm_cost, min_overall_cost) and model_assignment["accuracy"] > max_acc
            if worker_assignment["total_compute_cost"] + comm_cost < min_overall_cost or same_cost_higher_acc:
                mapped_assignment = dict()
                for node_idx, assigned_workers_idx in worker_assignment["workers_assignment"].items():
                    node = idx_nodes_mapping[node_idx]
                    assignd_workers = [idx_workers_mapping[x[0]] for x in assigned_workers_idx]
                    mapped_assignment[node] = assignd_workers
                min_overall_cost = worker_assignment["total_compute_cost"] + comm_cost
                max_acc = model_assignment["accuracy"]
                best_assignment_compute_cost = worker_assignment["total_compute_cost"]                
                best_worker_assignment = mapped_assignment
                best_model_assignment = model_assignment["model_assignment"]

    best_assignment = {
        "compute_cost": best_assignment_compute_cost,  
        "communication_cost": min_overall_cost - best_assignment_compute_cost,
        "model_assignment": best_model_assignment,
        "worker_assignment": best_worker_assignment,
        "end_to_end_accuracy": max_acc
    }

    print(json.dumps(best_assignment, sort_keys=True, indent=4))
    print("num state:", num_states)
    if args.output:
        if args.output.endswith(".yaml"):
            with open(args.output, 'wt') as f:
                json.dump(best_assignment, f, sort_keys=True, indent=4)
        elif args.output.endswith(".json"):
            with open(args.output, 'wt') as f:
                yaml.dump(best_assignment, f,)
        else:
            raise ValueError("invalid output file format")


if __name__ == "__main__":
    main()