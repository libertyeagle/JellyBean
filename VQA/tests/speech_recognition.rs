use std::path::Path;
use std::rc::Rc;

use vqa_workload::config::ResourcesSetupConfig;
use vqa_workload::config::SpeechRecognitionConfig;
use vqa_workload::resources::PyResources;
use vqa_workload::utils::read_audio;
use vqa_workload::transcribe_speech;

#[test]
fn test_speech_recognition() {
    let asr_model_config = SpeechRecognitionConfig::default();
    let setup_config = ResourcesSetupConfig {
        load_asr_model: Some(asr_model_config),
        load_image_model: None,
        load_vqa_model: None,
        load_utils_module: true
    };
    let resources = Rc::new(PyResources::new(setup_config));
    let src_dir = env!("CARGO_MANIFEST_DIR");

    let audio_path = Path::new(src_dir).join("data/examples/6930-75918-0008.flac");
    let raw_speech = read_audio(
        audio_path,
        0,
        16000,
        &resources
    ).unwrap();

    let result = transcribe_speech(raw_speech, &resources).unwrap();
    let transcription = result.text;
    
    assert_eq!(transcription, "CAN YOU IMAGINE WHY BUCKINGHAM HAS BEEN SO VIOLENT I SUSPECT");
}