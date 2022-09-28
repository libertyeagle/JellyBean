use std::path::Path;

#[derive(Debug, Clone)]
pub struct VQAInferenceConfig {
    pub config_path: String,
    pub ckpt_path: String,
    pub resume_ckpt: String,
    pub device: String
}

impl Default for VQAInferenceConfig {
    fn default() -> Self {
        let src_dir = env!("CARGO_MANIFEST_DIR");
        let config_file = Path::new(src_dir).join("python/configs/vqa/mutan_att_trainval.yaml");
        let config_file = config_file.to_str().unwrap().to_string();
        let ckpt_path = Path::new(src_dir).join("python/trained_models/vqa/mutan_att_trainval");
        let ckpt_path = ckpt_path.to_str().unwrap().to_string();
        Self { 
            config_path: config_file,
            ckpt_path: ckpt_path,
            resume_ckpt: String::from("ckpt"),
            device: String::from("0")
        }
    }
}