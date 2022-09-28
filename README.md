# JellyBean
Here is the prototype for JellyBean, a system for serving and optimizing machine learning inference workflows on heterogeneous infrastructures. 

## Example
Here we provide an example to run JellyBean's query optimizer for the medium infrastructure setup, on VQA dataset, and execute the workflow with the query processor.

### Datasets and pretrained models
We provide the sampled dataset and pretrained models of all the variants we used.

The dataset is a processed version of the following two datasets:
- [VQA](https://visualqa.org/)
- [Speech-based VQA](https://github.com/zted/sbvqa).

The dataset can be obtained from: 
<https://duke.box.com/s/wu7o721wrqu0ei75ob8yojcw1ntg3tfj>

Place the dataset in `VQA/data/VQA_Workflow_Datasets`.

The models for VQA are trained using the code repo <https://github.com/Cadene/vqa.pytorch>. 
Our pretrained models can be obtained from:
<https://duke.box.com/s/c1pfwbxq6qc51jx9sw91mnp8zstpo3zn>

The models should be placed in `VQA/python/trained_models/variants/`.

### Query Optimizer
The input data for the query optimizer are located in `VQA/examples/optimizer_inputs`.

#### Profile
We already provided in the optimizer's inputs the profile results for our medium infrastructure setup. But if you want to profile the models on an worker of your own: 
To compute average message (model output) sizes:
```
cd VQA
python python/models_profiler/compute_message_size.py --dataset data/VQA_Workflow_Datasets --output ~/VQAWorkflowProfile
```

Then we profile the execution latency of the models
```
cd VQA
python python/models_profiler/profile_asr.py --dataset data/VQA_Workflow_Datasets --output ~/VQAWorkflowProfile --device {DEVICE}
python python/models_profiler/profile_image_model.py --output ~/VQAWorkflowProfile --device {DEVICE}
python python/models_profiler/profile_vqa.py --dataset data/VQA_Workflow_Datasets --output ~/VQAWorkflowProfile --device {DEVICE}
```
here `{DEVICE}` is the device identifier on the worker, e.g., `cuda:0`, `cpu`.

#### Optimize
To run query optimizer using our provided input data:
```
python optimizer/main.py --inputs VQA/examples/optimizer_inputs/ --config VQA/examples/optimizer_inputs/config.yaml
```

It outputs the worker assginment and estimated compute and communication costs:
```
{
    "communication_cost": 1.2667783071018048,
    "compute_cost": 6.242,
    "end_to_end_accuracy": 56.59,
    "model_assignment": {
        "ExtractImageFeature": "resnet18",
        "SpeechRecognition": "wav2vec2-large-960h-lv60-self",
        "VQA": "default"
    },
    "worker_assignment": {
        "ExtractImageFeature": [
            "workflow-compute-cpu-1",
            "workflow-compute-cpu-2",
            "workflow-compute-cpu-5"
        ],
        "SpeechRecognition": [
            "workflow-compute-gpu-1",
            "workflow-compute-cpu-3"
        ],
        "VQA": [
            "workflow-compute-cpu-6"
        ]
    }
}
```
which we can then use the outputs to configurate thed query processor

### Query Processor
`executor/MLdataflow` contains our query processor built on timely dataflow.

`examples/VQA/src/bin/workflow` contains the code that wraps the Python models and the query processor to distribute the VQA workflow.    

`examples/config.json` is the query processor's config, which can be constructed from the query optimizer's output. It contains the worker assignment, how data should be distributed among the workers (load balancing).

```
cd VQA
cargo run --release --bin workflow -- -c examples/config.json -p [PIPELINE_INDEX] -i [WORKER_INDEX]
```
where `[PIPELINE_INDEX]` is 0/1/2/3/4, they correspond to:
- 0,1: represents the data emitter, which read the image and audio files
- 2: speech recognition
- 3: image feature extractor
- 4: VQA inference

`[WORKER_INDEX]` represents the worker ID with respect to the pipeline stage.