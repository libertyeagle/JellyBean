use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::rc::Rc;

use mlflow::handle::Exchange;
use vqa_workload::config::ResourcesSetupConfig;
use vqa_workload::config::{VQAInferenceConfig, FeatureExtractionConfig, SpeechRecognitionConfig};
use vqa_workload::data::{VQAImageQuestionPair, VQAImageContained, VQAImageFeatureContained};
use vqa_workload::resources::PyResources;
use vqa_workload::speech_recognition::data::VQAQuestionRawSpeechContained;
use vqa_workload::utils::{read_image, read_audio, read_dataset};
use vqa_workload::extract_image_features;
use vqa_workload::vqa_model_inference;
use vqa_workload::transcribe_speech;


use mlflow::{Map, Join, Inspect};
use mlflow::PipelineGraphBuilder;
use mlflow::ExecutionConfig;
use mlflow::pipeline_worker_execute;

const READ_BUFFER_SIZE: usize = 1024;

pub fn run_pipeline_worker(config: ExecutionConfig, pipeline_index: usize, worker_index: usize, buffer_input_read: bool, num_instances: Option<usize>) {
    let builder = move |builder: &mut PipelineGraphBuilder<u64>| {
        let worker_index = builder.worker_index();
        let assigned_ops = builder.get_assigned_operators().expect("could not acquire assigned operators");

        // model assignments: operator_name -> model_name
        let model_assignments = builder.get_config::<HashMap<String, String>>("model_assignments")
                                                                .expect("model assignments not specified");
        // device placements for the models: (operator_name, worker_index) -> model_name
        let device_assignments = builder.get_config::<HashMap<(String, usize), String>>("model_device_placements")
                                                            .expect("model device placements not specified");
        let asr_config = if assigned_ops.contains(&String::from("SpeechRecognition")) {
            Some(SpeechRecognitionConfig {
                model: model_assignments.get(&String::from("SpeechRecognition")).unwrap().to_owned(),
                device: device_assignments.get(&(String::from("SpeechRecognition"), worker_index)).unwrap().to_owned(),
                sampling_rate: 16000,
                cache_dir: String::new(),
            })
        }
        else { None };

        let image_model_config = if assigned_ops.contains(&String::from("ImageFeatureExtract")) {
            Some(FeatureExtractionConfig {
                model: model_assignments.get(&String::from("ImageFeatureExtract")).unwrap().to_owned(),
                device:  device_assignments.get(&(String::from("ImageFeatureExtract"), worker_index)).unwrap().to_owned(),
                image_size: 448,
                hub_dir: String::new(),
                use_attention_feature: false,
            })
        }
        else { None };
        
        let vqa_config = if assigned_ops.contains(&String::from("VQAInference")) {
            let model_choice = model_assignments.get(&String::from("VQAInference")).unwrap().to_owned();
            let src_dir = env!("CARGO_MANIFEST_DIR");
            let (config_path, ckpt_path) = match &model_choice[..] {
                "resnet18" => {
                    let config_path = Path::new(src_dir).join("python/configs/variants/mutan_noatt_resnet18.yaml");
                    let ckpt_path = Path::new(src_dir).join("python/trained_models/variants/mutan_noatt_resnet18");
                    let config_path = config_path.to_str().unwrap().to_string();
                    let ckpt_path = ckpt_path.to_str().unwrap().to_string();
                    (config_path, ckpt_path)
                },
                "resnet34" => {
                    let config_path = Path::new(src_dir).join("python/configs/variants/mutan_noatt_resnet34.yaml");
                    let ckpt_path = Path::new(src_dir).join("python/trained_models/variants/mutan_noatt_resnet34");
                    let config_path = config_path.to_str().unwrap().to_string();
                    let ckpt_path = ckpt_path.to_str().unwrap().to_string();
                    (config_path, ckpt_path)
                }
                "resnet50" => {
                    let config_path = Path::new(src_dir).join("python/configs/variants/mutan_noatt_resnet50.yaml");
                    let ckpt_path = Path::new(src_dir).join("python/trained_models/variants/mutan_noatt_resnet50");
                    let config_path = config_path.to_str().unwrap().to_string();
                    let ckpt_path = ckpt_path.to_str().unwrap().to_string();
                    (config_path, ckpt_path)
                }
                "resnet101" => {
                    let config_path = Path::new(src_dir).join("python/configs/variants/mutan_noatt_resnet101.yaml");
                    let ckpt_path = Path::new(src_dir).join("python/trained_models/variants/mutan_noatt_resnet101");
                    let config_path = config_path.to_str().unwrap().to_string();
                    let ckpt_path = ckpt_path.to_str().unwrap().to_string();
                    (config_path, ckpt_path)
                }
                "resnet152" => {
                    let config_path = Path::new(src_dir).join("python/configs/variants/mutan_noatt_resnet152.yaml");
                    let ckpt_path = Path::new(src_dir).join("python/trained_models/variants/mutan_noatt_resnet152");
                    let config_path = config_path.to_str().unwrap().to_string();
                    let ckpt_path = ckpt_path.to_str().unwrap().to_string();
                    (config_path, ckpt_path)
                }
                _ => {
                    panic!("invalid model choice for VQA")
                }
            };
            Some(VQAInferenceConfig {
                config_path: config_path,
                ckpt_path: ckpt_path,
                resume_ckpt: String::from("ckpt"),
                device: device_assignments.get(&(String::from("VQAInference"), worker_index)).unwrap().to_owned(),
            })
        }
        else { None };

        let setup_config = ResourcesSetupConfig {
            load_asr_model: asr_config,
            load_image_model: image_model_config,
            load_vqa_model: vqa_config,
            load_utils_module: true
        };

        let resources = Rc::new(PyResources::new(setup_config));

        let (image_paths, speech_paths) = if assigned_ops.contains(&String::from("InputImagePath")) 
                || assigned_ops.contains(&String::from("InputSpeechPath")) 
                || assigned_ops.contains(&String::from("ReadImage"))
                || assigned_ops.contains(&String::from("ReadSpeechAudio")) {
            let dataset_root = builder.get_config::<String>("dataset_path").expect("dataset path not specified");
            let num_instances = num_instances.unwrap_or(1024);
            read_dataset(dataset_root, num_instances)
        } 
        else {
            (VecDeque::new(), VecDeque::new())
        };

        let image_handle;
        let shared_resources = resources.clone();
        if buffer_input_read {
            image_handle = builder.new_input_buffered_from_source_distributed(
                image_paths, 
                move |(uid, path)| (VQAImageContained::from(read_image(path, uid, &shared_resources).unwrap()), uid),
                READ_BUFFER_SIZE,
                "ReadImage"
            );
        }
        else {
            let handle = builder.new_input_from_source_distributed(
                image_paths, 
                |input, _| input.0,
                "InputImagePath"
            );
            image_handle = handle.map(
                move |(uid, path)| VQAImageContained::from(read_image(path, uid, &shared_resources).unwrap()),
                "ReadImage"
            );
        }

        let speech_handle;
        let shared_resources = resources.clone();
        if buffer_input_read {
            speech_handle = builder.new_input_buffered_from_source_distributed(
                speech_paths,
                move |(uid, path)| (VQAQuestionRawSpeechContained::from(
                    read_audio(path, uid, 16000, &shared_resources).unwrap()
                ), uid),
                READ_BUFFER_SIZE,
                "ReadSpeechAudio"
            );
        }
        else {
            let handle = builder.new_input_from_source_distributed(
                speech_paths, 
                |input, _| input.0,
                "InputSpeechPath"
            );
            speech_handle = handle.map(
                move |(uid, path)| VQAQuestionRawSpeechContained::from(
                    read_audio(path, uid, 16000, &shared_resources).unwrap()
                ),
                "ReadSpeechAudio"
            );            
        }

        let shared_resources = resources.clone();
        let speech_handle = speech_handle.map(
            move |speech| transcribe_speech(speech.into(), &shared_resources).unwrap(),
            "SpeechRecognition"
        );        

        let shared_resources = resources.clone();
        let image_handle = image_handle.map(
            move |img| {
                let feat = extract_image_features(img.into(), &shared_resources).unwrap();
                VQAImageFeatureContained::from(feat)
            },
            "ImageFeatureExtract" 
        );

        let handle = image_handle.intra_pipeline_gather(0, "GatherImageFeat")
        .concat(
            &speech_handle.intra_pipeline_gather(0, "GatherQuestion"),
            |x| x.uid,
            |x| x.uid,
            "JoinImageQuestion"
        ).intra_pipeline_exchange(
            |(x, _y)| x.uid, 
            "RedistributeVQAInput"
        );

        let shared_resources = resources.clone();
        let _ = handle.map(move |(img, question)| {
            debug_assert_eq!(img.uid, question.uid);
            let iq_pair = VQAImageQuestionPair {
                uid: img.uid,
                image_feat: img.feat.into(),
                question: question.text
            };
            vqa_model_inference(iq_pair, &shared_resources).unwrap()
        }, "VQAInference")
        .intra_pipeline_gather(0, "GatherResults")
        .inspect(|x| {
            let uid = x.uid;
            let answer = x.answer.clone();
            println!("Image-Question Pair #{}, answer={}", uid, answer); 
        }, "InspectAnswer");
    };
    
    pipeline_worker_execute(builder, &config, pipeline_index, worker_index);

}