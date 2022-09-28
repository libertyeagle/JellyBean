from dataclasses import dataclass
from collections import namedtuple
from graphlib import TopologicalSorter
from copy import deepcopy

import numpy as np
from scipy.interpolate import LinearNDInterpolator, RBFInterpolator

# ([input_1_acc, input_2_acc, ...], output_acc)
AccSample = namedtuple("AccSample", ['inputs_acc', 'output_acc'])

@dataclass
class EndToEndAccuracyProfile:
    """
    operators: list[str]
        operators for which we need to assign model variants
    acc_profile: dict[tuple[str, ...], float]
        For each model variant assignment (the variant for each operator in the same order as `operators`),
        the end-to-end accuracy
    """
    operators: list[str]
    acc_profile: dict[tuple[str, ...], float]


@dataclass
class OperatorAccuracyProfile:
    """
    op_name: str
        Operator name
    inputs: list of str
        The names of input opeartors (parents)
    acc_profile: dict
        Input-output accuracy profile for each model variant
        variant-name -> [
            acc_sample_1: AccSample,
            acc_sample 2: AccSample
        ]
    cost_profile: dict
        The cost of each model variant
        variant-name -> cost (float)
    weight: float
        weight of this operator in computing the overall cost
    """
    op_name: str
    inputs: list
    acc_profile: dict
    cost_profile: dict
    weight: float


@dataclass
class PartialAssignment:
    """
    variants_assignments: dict
        The partial variant assignments
        op_name -> variant_name (str)
    input_acc_requirements: dict
        Required minimal input accuracy of the upstream operators
        op_name -> float
    """
    variants_assignments: dict
    input_acc_requirements: dict


def sample_acc_profile(op_profile, variant, output_threshold, limit=10):
    """Sample data points from the operator's accuracy profile, that 
    the output accuracy >= `output_threshold`

    Parameters
    ----------
    op_profile: OperatorAccuracyProfile
        The accuracy profile of this operator.
    variant: str 
        Which variant to use for this operator?
    output_threshold: float
        The output accuracy threshold.
    limit: int
        How many accuracy samples to consider?
    """
    variant_acc_profile = op_profile.acc_profile[variant]
    variant_acc_profile = list(filter(lambda x: x.output_acc >= output_threshold, variant_acc_profile))
    if len(variant_acc_profile) == 0:
        return None
    variant_acc_profile.sort(key=lambda x: (x.output_acc, tuple(x.inputs_acc)))
    return variant_acc_profile[:limit]


class MLDataflowGraph:
    def __init__(self, op_profiles, sink):
        """Initialize beam search
        Parameters
        ----------
        op_profiles: list[OperatorAccuracyProfile] or dict
            All operators' profiles, or optionally contains the end-to-end accuracy profile
        sink: str
            Which node (operator) is the sink node
        """
        graph = {}
        self.op_profiles = {}
        if isinstance(op_profiles, dict):
            # end_to_end_profile should be an instance of EndToEndAccuracyProfile
            self.end_to_end_profile = op_profiles["end_to_end_profile"]
            op_profiles = op_profiles["per_operator_profiles"]
        else:
            self.end_to_end_profile = None
        for profile in op_profiles:
            curr_node = profile.op_name
            if profile.inputs is not None and len(profile.inputs) > 0:
                predecessors = set(profile.inputs)
                graph[curr_node] = predecessors
            self.op_profiles[curr_node] = profile

        ts = TopologicalSorter(graph)
        ts = ts.static_order()
        ts = list(reversed(list(ts)))
        ts.remove(sink)
        ts.insert(0, sink)
        self.sink = sink
        self.assign_order = ts


    def interpolate_acc_profile(self, points_per_input=5, method='linear'):
        for _, profile in self.op_profiles.items():
            if not profile.inputs:
                continue
            for _, variant_profile in profile.acc_profile.items():
                samples_input_acc = [x.inputs_acc for x in variant_profile]
                samples_input_acc = np.array(samples_input_acc)
                samples_output_acc = np.array([x.output_acc for x in variant_profile])
                samples_min_acc_per_input = list(samples_input_acc.min(axis=0))
                samples_max_acc_per_input = list(samples_input_acc.max(axis=0))
                inputs_linspace = []
                for min_acc, max_acc in zip(samples_min_acc_per_input, samples_max_acc_per_input):
                    interpolate_coords = np.linspace(start=min_acc, stop=max_acc, num=points_per_input)
                    inputs_linspace.append(interpolate_coords)
                interpolate_points = np.meshgrid(*inputs_linspace)
                interpolate_points = [x.ravel() for x in interpolate_points]
                if method == "linear":
                    interp = LinearNDInterpolator(samples_input_acc, samples_output_acc)
                else:
                    interp = RBFInterpolator(samples_input_acc, samples_output_acc)
                interpolate_values = interp(np.vstack(interpolate_points).transpose())
                interpolate_samples = []
                for input_point, output_acc in zip(zip(*interpolate_points), interpolate_values):
                    if not np.isnan(output_acc):
                        sample = AccSample(inputs_acc=input_point, output_acc=output_acc)
                        interpolate_samples.append(sample)
                variant_profile.extend(interpolate_samples)


    def beam_search(self, end_to_end_threshold, best_k=10, acc_profile_sample_limit=None, per_assignment_node_expansion_limit=None): 
        """Perform beam search
        Parameters
        ----------
        end_to_end_threshold: float
            End-to-end accuracy threshold
        best_k: int
            Keep top-K lowest-cost results
        acc_profile_sample_limit: int
            Maximum number of samples to use when sampling the accuracy profile for a model variant
        per_assignment_node_expansion_limit: int
            Control the number of new assignments each partial assignment can expand to, when considering a new node.
        Returns
        -------
            list[(dict[str, str], float)]
            A set of complete model variant assignments and their corresponding cost.
        """
        candidates = []
        nodes_to_assign = self.assign_order
        initial_candididate = PartialAssignment(
            variants_assignments={},
            input_acc_requirements={self.sink: end_to_end_threshold}
        )
        candidates.append(initial_candididate)

        while nodes_to_assign:
            v = nodes_to_assign.pop(0)
            new_candidates = []
            for partial_assignment in candidates:
                curr_input_acc_req = partial_assignment.input_acc_requirements
                v_threshold_req = curr_input_acc_req[v]
                feasible_variants = []
                for variant in self.op_profiles[v].acc_profile.keys():
                    variant_max_acc = max(map(lambda x: x.output_acc, self.op_profiles[v].acc_profile[variant]))
                    if variant_max_acc >= v_threshold_req:
                        feasible_variants.append(variant)
                
                if len(feasible_variants) == 0:
                    # infeasible
                    return None

                if self.op_profiles[v].inputs is None or len(self.op_profiles[v].inputs) == 0:
                    for variant in feasible_variants:
                        extend_assignment = deepcopy(partial_assignment.variants_assignments) 
                        extend_assignment[v] = variant
                        extend_acc_req = deepcopy(partial_assignment.input_acc_requirements)
                        extend_acc_req.pop(v, None)
                        extend_candidate = PartialAssignment(
                            variants_assignments=extend_assignment, 
                            input_acc_requirements=extend_acc_req
                        )
                        new_candidates.append(extend_candidate)
                else:
                    for variant in feasible_variants:
                        # remove min_acc_only parameter
                        # sort acc reqs accroding to the input accuracy (one by one)
                        v_input_acc_reqs = sample_acc_profile(self.op_profiles[v], variant, v_threshold_req, limit=acc_profile_sample_limit)
                        if per_assignment_node_expansion_limit:
                            v_input_acc_reqs = v_input_acc_reqs[:per_assignment_node_expansion_limit - len(new_candidates)]
                        for v_input_acc_req in v_input_acc_reqs:
                            extend_acc_req = deepcopy(curr_input_acc_req)
                            for input_node_name, acc_req in zip(self.op_profiles[v].inputs, v_input_acc_req.inputs_acc):
                                if input_node_name in extend_acc_req:
                                    extend_acc_req[input_node_name] = max(extend_acc_req[input_node_name], acc_req)
                                else:
                                    extend_acc_req[input_node_name] = acc_req
                            extend_assignment = deepcopy(partial_assignment.variants_assignments)
                            extend_assignment[v] = variant
                            extend_acc_req.pop(v, None)
                            extend_candidate = PartialAssignment(
                                variants_assignments=extend_assignment,
                                input_acc_requirements=extend_acc_req
                            )
                            new_candidates.append(extend_candidate)
                        if per_assignment_node_expansion_limit and len(new_candidates) >= per_assignment_node_expansion_limit:
                            break

            # sort new candidates first according to cost, and then according to the input accuracy requirements            
            inputs_nodes = list(new_candidates[0].input_acc_requirements.keys())
            extract_input_accs = lambda x: tuple([x.input_acc_requirements[node] for node in inputs_nodes])
            new_candidates.sort(key=lambda x: (self.eval_cost(x.variants_assignments), extract_input_accs(x)))
            if best_k:
                new_candidates = new_candidates[:best_k]
            candidates = new_candidates
        
        models_assignments = []
        for assignment in candidates:
            variants_assignments = assignment.variants_assignments
            end_to_end_acc = self.lookup_end_to_end_accuracy(variants_assignments)
            total_cost = self.eval_cost(variants_assignments)
            state = {
                "model_assignment": variants_assignments,
                "cost": total_cost,
                "accuracy": end_to_end_acc
            }
            models_assignments.append(state)
            
        return models_assignments


    def eval_cost(self, partial_assignment):
        """
        Evaulate the cost of a partial model assignment
        Parameters
        ----------
        partial_assignment: dict[str, str]
            A partial model assignment
        Returns
        -------
        float
            Evaluated cost
        """
        costs = [self.op_profiles[op_name].weight * self.op_profiles[op_name].cost_profile[variant]
                 for op_name, variant in partial_assignment.items()]
        return sum(costs)


    def lookup_end_to_end_accuracy(self, complete_assignment):
        """If end-to-end accuracy profile is given, look up the accuracy of a complete variant assignment
        Parameters
        ----------
        complete_assignment: dict[str, str]
            A complete model assignment
        Returns
        -------
        float
            End-to-end accuracy
        """
        if self.end_to_end_profile is None:
            return None
        variants = tuple(complete_assignment[node] for node in self.end_to_end_profile.operators)
        return self.end_to_end_profile.acc_profile[variants]