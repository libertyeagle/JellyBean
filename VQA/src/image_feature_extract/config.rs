#[derive(Debug, Clone)]
pub struct FeatureExtractionConfig {
    pub model: String,
    pub device: String,
    pub image_size: u32,
    pub hub_dir: String,
    pub use_attention_feature: bool,
}

impl Default for FeatureExtractionConfig {
    fn default() -> Self {
        Self { 
            model: String::from("fbresnet152"),
            device: String::from("0"), 
            image_size: 448, 
            hub_dir: String::new(),
            use_attention_feature: false,
        }
    }
}