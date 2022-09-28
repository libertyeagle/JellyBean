use std::path::Path;
use std::rc::Rc;

use float_cmp::approx_eq;

use vqa_workload::config::ResourcesSetupConfig;
use vqa_workload::config::FeatureExtractionConfig;
use vqa_workload::extract_image_features;
use vqa_workload::image_feature_extract::data::CNNFeat;
use vqa_workload::resources::PyResources;
use vqa_workload::utils::read_image;

#[test]
fn test_extract_features() {
    let image_model_config = FeatureExtractionConfig::default();
    let setup_config = ResourcesSetupConfig {
        load_image_model: Some(image_model_config),
        load_asr_model: None,
        load_vqa_model: None,
        load_utils_module: true
    };
    let resources = Rc::new(PyResources::new(setup_config));
    let src_dir = env!("CARGO_MANIFEST_DIR");

    let image_path = Path::new(src_dir).join("data/examples/COCO_val2014_000000262162.jpg");
    let image = read_image(
        image_path,
        0,
        &resources
    ).unwrap();

    let result = extract_image_features(image, &resources).unwrap();

    if let CNNFeat::ConvFeat(feat) = result.feat {
        assert_eq!(feat.shape(), &[2048, 14, 14]);
        assert!(approx_eq!(f32, feat[(0,0,0)], 0., epsilon=0.0000001));
        assert!(approx_eq!(f32, feat[(0,0,1)], 0., epsilon=0.0000001));
        assert!(approx_eq!(f32, feat[(0,0,2)], 0.00687669, epsilon=0.0000001));
        assert!(approx_eq!(f32, feat[(0,1,1)], 0.00805745, epsilon=0.0000001));
        assert!(approx_eq!(f32, feat[(0,2,13)], 0.05446234, epsilon=0.0000001));
    }

}