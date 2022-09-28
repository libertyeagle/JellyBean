mod relay;
mod worker;
mod config;

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::Arc;
use std::any::Any;

use structopt::StructOpt;
use shellexpand::tilde;

use mlflow::{PipelineConfig, ExecutionConfig};

use crate::config::VQAWorkflowConfig;
use crate::relay::run_pipeline_relay;
use crate::worker::run_pipeline_worker;

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "VQA Workflow")]
pub struct Opts {
    #[structopt(short, long, parse(from_os_str))]
    pub config: PathBuf,

    #[structopt(short, long)]
    pub worker: bool,

    #[structopt(short, long)]
    pub relay: bool,

    /// Which pipeline
    #[structopt(short, long)]
    pub pipeline: usize,

    #[structopt(short, long)]
    pub index: usize
}

// cargo run --bin workflow -- -c [CONFIG_PATH] -p [PIPELINE_INDEX] -i [WORKER_INDEX] -r (or -w)
fn main() {
    let opt = Opts::from_args();
    if opt.worker || opt.relay {
        assert!(opt.worker ^ opt.relay, "run either pipeline worker or relay");
    }
    let config_path = opt.config;

    let file = File::open(config_path).unwrap();
    let reader = BufReader::new(file);
    // Read the JSON contents of the file as an instance of `User`.
    let config: VQAWorkflowConfig = serde_json::from_reader(reader).unwrap();
    let request_rate = config.request_rate;
    let mut pipeline_specs = config.pipeline_specs;

    let buffer_read = if let Some(buffer_read) = config.buffer_read {
        buffer_read
    }
    else { false };

    let logging_dir = tilde(&config.logging_dir).into_owned();
    let logging_dir = PathBuf::from(logging_dir);
    let dataset_path = tilde(&config.dataset_path).into_owned();
    let num_instances = config.num_instances;

    let node_index = opt.index;
    let pipeline_index = opt.pipeline;
    let num_workers = pipeline_specs.get(&format!("pipeline_{}", pipeline_index)).expect("wrong pipeline index").worker_addrs.len();
    let num_relays = pipeline_specs.get(&format!("pipeline_{}", pipeline_index)).expect("wrong pipeline index").relay_addrs.len();
    if opt.worker {
        assert!(node_index < num_workers, "invalid worker index");
    }
    else if opt.relay {
        assert!(node_index < num_relays, "invalid relay index");
    }
    if !opt.relay && !opt.worker {
        assert!(num_relays == num_workers, "#workers and #relays are not equal")
    }

    let pipeline_spec = pipeline_specs.remove("pipeline_0").unwrap();
    let mut builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
    builder_configs.insert(String::from("dataset_path"), Arc::new(dataset_path.clone()));
    builder_configs.insert(String::from("model_assignments"), Arc::new(HashMap::<String, String>::new()));
    builder_configs.insert(String::from("model_device_placements"), Arc::new(HashMap::<(String, usize), String>::new()));
    let pipeline_0_config;
    if buffer_read {
        pipeline_0_config = PipelineConfig {
            pipeline_index: 0,
            assigned_ops: vec![
                String::from("ReadSpeechAudio"),
            ],
            required_input_ops: Some(vec![]),
            output_ops: Some(vec![
                String::from("ReadSpeechAudio")
            ]),
            worker_addrs: pipeline_spec.worker_addrs,
            relay_addrs: pipeline_spec.relay_addrs,
            relay_load_balance_weights: pipeline_spec.relay_weights,
            input_pipelines: vec![],
            output_pipelines: vec![2],
            builder_configs,
            operator_configs: HashMap::new(),
            request_rates: HashMap::from_iter([(String::from("ReadSpeechAudio"), request_rate)]),
            source_operators: HashSet::new(),
        };
    }
    else {
        pipeline_0_config = PipelineConfig {
            pipeline_index: 0,
            assigned_ops: vec![
                String::from("InputSpeechPath"), 
                String::from("ReadSpeechAudio"),
            ],
            required_input_ops: Some(vec![]),
            output_ops: Some(vec![
                String::from("ReadSpeechAudio")
            ]),
            worker_addrs: pipeline_spec.worker_addrs,
            relay_addrs: pipeline_spec.relay_addrs,
            relay_load_balance_weights: pipeline_spec.relay_weights,
            input_pipelines: vec![],
            output_pipelines: vec![2],
            builder_configs,
            operator_configs: HashMap::new(),
            request_rates: HashMap::from_iter([(String::from("InputSpeechPath"), request_rate)]),
            source_operators: HashSet::from_iter([String::from("ReadSpeechAudio")])
        };
    }


    let pipeline_spec = pipeline_specs.remove("pipeline_1").unwrap();
    let mut builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
    builder_configs.insert(String::from("dataset_path"), Arc::new(dataset_path.clone()));
    builder_configs.insert(String::from("model_assignments"), Arc::new(HashMap::<String, String>::new()));
    builder_configs.insert(String::from("model_device_placements"), Arc::new(HashMap::<(String, usize), String>::new()));
    let pipeline_1_config;
    if buffer_read {
        pipeline_1_config = PipelineConfig {
            pipeline_index: 1,
            assigned_ops: vec![
                String::from("ReadImage"),
            ],
            required_input_ops: Some(vec![]),
            output_ops: Some(vec![
                String::from("ReadImage")
            ]),
            worker_addrs: pipeline_spec.worker_addrs,
            relay_addrs: pipeline_spec.relay_addrs,
            relay_load_balance_weights: pipeline_spec.relay_weights,
            input_pipelines: vec![],
            output_pipelines: vec![3],
            builder_configs,
            operator_configs: HashMap::new(),
            request_rates: HashMap::from_iter([(String::from("ReadImage"), request_rate)]),
            source_operators: HashSet::new()
        };
    }
    else {
        pipeline_1_config = PipelineConfig {
            pipeline_index: 1,
            assigned_ops: vec![
                String::from("InputImagePath"),
                String::from("ReadImage"),
            ],
            required_input_ops: Some(vec![]),
            output_ops: Some(vec![
                String::from("ReadImage")
            ]),
            worker_addrs: pipeline_spec.worker_addrs,
            relay_addrs: pipeline_spec.relay_addrs,
            relay_load_balance_weights: pipeline_spec.relay_weights,
            input_pipelines: vec![],
            output_pipelines: vec![3],
            builder_configs,
            operator_configs: HashMap::new(),
            request_rates: HashMap::from_iter([(String::from("InputImagePath"), request_rate)]),
            source_operators: HashSet::from_iter([String::from("ReadImage")])
        };
    }
    
    let pipeline_spec = pipeline_specs.remove("pipeline_2").unwrap();
    let mut builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
    builder_configs.insert(String::from("model_assignments"), Arc::new(pipeline_spec.model_assignments.clone()));
    let device_placements = pipeline_spec.device_placements;
    let mut device_placements_converted = HashMap::new();
    for (op_name, placements) in device_placements.into_iter() {
        for (worker_idx, placement) in placements.into_iter().enumerate() {
            device_placements_converted.insert((op_name.clone(), worker_idx), placement);
        }
    }
    builder_configs.insert(String::from("model_device_placements"), Arc::new(device_placements_converted));
    let mut operator_configs = HashMap::new();
    if pipeline_spec.simulate_network_latency.is_some() {
        for (op_name, net_latency) in pipeline_spec.simulate_network_latency.unwrap().into_iter() {
            let net_lat_config = HashMap::from([(String::from("simulate_network_latency"), Arc::new(net_latency) as Arc<dyn Any + Send + Sync>)]);
            operator_configs.insert(op_name, net_lat_config);
        }
    }
    let pipeline_2_config = PipelineConfig {
        pipeline_index: 2,
        assigned_ops: vec![
            String::from("SpeechRecognition"),
        ],
        required_input_ops: Some(vec![
            String::from("ReadSpeechAudio")
        ]),
        output_ops: Some(vec![
            String::from("SpeechRecognition")
        ]),
        worker_addrs: pipeline_spec.worker_addrs,
        relay_addrs: pipeline_spec.relay_addrs,
        relay_load_balance_weights: pipeline_spec.relay_weights,
        input_pipelines: vec![0],
        output_pipelines: vec![4],
        builder_configs,
        operator_configs,
        request_rates: HashMap::new(),
        source_operators: HashSet::new(),
    };

    let pipeline_spec = pipeline_specs.remove("pipeline_3").unwrap();
    let mut builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
    builder_configs.insert(String::from("model_assignments"), Arc::new(pipeline_spec.model_assignments.clone()));
    let device_placements = pipeline_spec.device_placements;
    let mut device_placements_converted = HashMap::new();
    for (op_name, placements) in device_placements.into_iter() {
        for (worker_idx, placement) in placements.into_iter().enumerate() {
            device_placements_converted.insert((op_name.clone(), worker_idx), placement);
        }
    }
    builder_configs.insert(String::from("model_device_placements"), Arc::new(device_placements_converted));
    let mut operator_configs = HashMap::new();
    if pipeline_spec.simulate_network_latency.is_some() {
        for (op_name, net_latency) in pipeline_spec.simulate_network_latency.unwrap().into_iter() {
            let net_lat_config = HashMap::from([(String::from("simulate_network_latency"), Arc::new(net_latency) as Arc<dyn Any + Send + Sync>)]);
            operator_configs.insert(op_name, net_lat_config);
        }
    }    
    let pipeline_3_config = PipelineConfig {
        pipeline_index: 3,
        assigned_ops: vec![
            String::from("ImageFeatureExtract")
        ],
        required_input_ops: Some(vec![
            String::from("ReadImage")
        ]),
        output_ops: Some(vec![
            String::from("ImageFeatureExtract")
        ]),
        worker_addrs: pipeline_spec.worker_addrs,
        relay_addrs: pipeline_spec.relay_addrs,
        relay_load_balance_weights: pipeline_spec.relay_weights,
        input_pipelines: vec![1],
        output_pipelines: vec![4],
        builder_configs,
        operator_configs,
        request_rates: HashMap::new(),
        source_operators: HashSet::new(),
    };
  
    let pipeline_spec = pipeline_specs.remove("pipeline_4").unwrap();
    let mut builder_configs: HashMap<String, Arc<dyn Any + Send + Sync>> = HashMap::new();
    builder_configs.insert(String::from("model_assignments"), Arc::new(pipeline_spec.model_assignments.clone()));
    let device_placements = pipeline_spec.device_placements;
    let mut device_placements_converted = HashMap::new();
    for (op_name, placements) in device_placements.into_iter() {
        for (worker_idx, placement) in placements.into_iter().enumerate() {
            device_placements_converted.insert((op_name.clone(), worker_idx), placement);
        }
    }    
    builder_configs.insert(String::from("model_device_placements"), Arc::new(device_placements_converted));
    let mut operator_configs = HashMap::new();
    if pipeline_spec.simulate_network_latency.is_some() {
        for (op_name, net_latency) in pipeline_spec.simulate_network_latency.unwrap().into_iter() {
            let net_lat_config = HashMap::from([(String::from("simulate_network_latency"), Arc::new(net_latency) as Arc<dyn Any + Send + Sync>)]);
            operator_configs.insert(op_name, net_lat_config);
        }
    }    
    let pipeline_4_config = PipelineConfig {
        pipeline_index: 4,
        assigned_ops: vec![
            String::from("JoinImageQuestion"),
            String::from("VQAInference"),
            String::from("GatherImageFeat"),
            String::from("GatherQuestion"),
            String::from("RedistributeVQAInput"),
            String::from("GatherResults"),
            String::from("InspectAnswer")
        ],
        required_input_ops: Some(vec![
            String::from("ImageFeatureExtract"),
            String::from("SpeechRecognition")
        ]),
        output_ops: Some(vec![]),
        worker_addrs: pipeline_spec.worker_addrs,
        relay_addrs: pipeline_spec.relay_addrs,
        relay_load_balance_weights: pipeline_spec.relay_weights,
        input_pipelines: vec![2, 3],
        output_pipelines: vec![],
        builder_configs,
        operator_configs,
        request_rates: HashMap::new(),
        source_operators: HashSet::new(),
    };


    let pipeline_configs = HashMap::from_iter([
        (0, pipeline_0_config),
        (1, pipeline_1_config),
        (2, pipeline_2_config),
        (3, pipeline_3_config),
        (4, pipeline_4_config)        
    ]);

    let timely_message_buffer_size = Some(1);
    let config = ExecutionConfig::new_with_default_mapping(pipeline_configs, Some(logging_dir), timely_message_buffer_size);
    if opt.worker {
        run_pipeline_worker(config, opt.pipeline, node_index, buffer_read, num_instances);
    }
    else if opt.relay {
        run_pipeline_relay(config, opt.pipeline, node_index);
    }
    else {
        let relay_config = config.clone();
        let handle = std::thread::spawn(move || run_pipeline_relay(relay_config, pipeline_index, node_index));
        std::thread::sleep(std::time::Duration::from_secs(1));
        run_pipeline_worker(config, pipeline_index, node_index, buffer_read, num_instances);
        handle.join().unwrap();
    }
}