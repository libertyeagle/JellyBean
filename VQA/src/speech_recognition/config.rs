#[derive(Debug, Clone)]
pub struct SpeechRecognitionConfig {
    pub model: String,
    pub device: String,
    pub sampling_rate: u32,
    pub cache_dir: String
}

impl Default for SpeechRecognitionConfig {
    fn default() -> Self {
        Self { 
            model: String::from("wav2vec2-base-960"),
            device: String::from("0"),
            sampling_rate: 16000,
            cache_dir: String::new()
        }
    }
}