use std::rc::Rc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use numpy::IntoPyArray;

use super::config::SpeechRecognitionConfig;
use super::data::{VQAQuestionRawSpeech, VQAQuestionText};
use crate::resources::PyResources;


pub fn setup_asr_engine(config: SpeechRecognitionConfig, module: &PyModule) -> PyResult<PyObject> {
    let py = module.py();
    let config_dict_py = PyDict::new(py);
    let model = config.model;
    let device = config.device;
    let sampling_rate = config.sampling_rate;
    let cache_dir = config.cache_dir;

    config_dict_py.set_item("model", model)?;    
    config_dict_py.set_item("device", device)?;
    config_dict_py.set_item("sampling_rate", sampling_rate)?;
    config_dict_py.set_item("cache_dir", cache_dir)?;

    let args = (config_dict_py, );
    let result = module.call_method1("SpeechRecognitionEngine", args)?;
    Ok(result.to_object(py))
}

pub fn transcribe_speech(raw_speech: VQAQuestionRawSpeech, resources: &Rc<PyResources>) -> PyResult<VQAQuestionText> {
    let pool = unsafe {resources.gil_guard.python().new_pool()};
    let py = pool.python();
    let uid = raw_speech.uid;
    let sampling_rate = raw_speech.sampling_rate;

    let waveform = raw_speech.waveform.into_pyarray(py);
    let args = (waveform, sampling_rate);
    let asr_model = resources.asr_engine.as_ref().unwrap();
    let result = asr_model.as_ref(py).call_method1("transcribe_audio", args)?;
    let transcription = result.extract()?;

    Ok(VQAQuestionText{
        uid,
        text: transcription
    })
}