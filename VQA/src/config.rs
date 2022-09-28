pub use crate::image_feature_extract::config::FeatureExtractionConfig;
pub use crate::vqa_inference::config::VQAInferenceConfig;
pub use crate::speech_recognition::config::SpeechRecognitionConfig;

pub struct ResourcesSetupConfig {
    pub load_asr_model: Option<SpeechRecognitionConfig>,
    pub load_image_model: Option<FeatureExtractionConfig>,
    pub load_vqa_model: Option<VQAInferenceConfig>,
    pub load_utils_module: bool
}