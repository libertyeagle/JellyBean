use std::path::Path;
use std::rc::Rc;


use vqa_workload::config::ResourcesSetupConfig;
use vqa_workload::config::VQAInferenceConfig;
use vqa_workload::config::FeatureExtractionConfig;
use vqa_workload::data::VQAImageQuestionPair;
use vqa_workload::resources::PyResources;
use vqa_workload::utils::read_image;
use vqa_workload::extract_image_features;
use vqa_workload::vqa_model_inference;

#[test]
fn test_vqa_inference() {
    let image_model_config = FeatureExtractionConfig::default();
    let vqa_model_config = VQAInferenceConfig::default();
    let setup_config = ResourcesSetupConfig {
        load_asr_model: None,
        load_image_model: Some(image_model_config),
        load_vqa_model: Some(vqa_model_config),
        load_utils_module: true
    };
    let resources = Rc::new(PyResources::new(setup_config));
    let src_dir = env!("CARGO_MANIFEST_DIR");

    let image_path = Path::new(src_dir).join("data/examples/COCO_val2014_000000262175.jpg");
    let image = read_image(
        image_path,
        0,
        &resources
    ).unwrap();

    let result = extract_image_features(image, &resources).unwrap();
    let feat = result.feat;
    let iq_pair = VQAImageQuestionPair {
        uid: 0,
        question: String::from("Where was the picture taken of the man?"),
        image_feat: feat
    };

    let answer = vqa_model_inference(iq_pair, &resources).unwrap();
    assert_eq!(answer.answer, "forest");
}