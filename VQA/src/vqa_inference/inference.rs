use std::rc::Rc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use numpy::IntoPyArray;

use super::config::VQAInferenceConfig;
use super::data::{VQAImageQuestionPair, VQAAnswer};
use crate::resources::PyResources;
use crate::image_feature_extract::data::CNNFeat;


pub fn setup_vqa_engine(config: VQAInferenceConfig, module: &PyModule) -> PyResult<PyObject> {
    let py = module.py();
    let config_dict_py = PyDict::new(py);
    let config_path = config.config_path;
    let ckpt_path = config.ckpt_path;
    let resume_ckpt = config.resume_ckpt;
    let device = config.device;

    config_dict_py.set_item("config_path", config_path)?;
    config_dict_py.set_item("ckpt_path", ckpt_path)?;
    config_dict_py.set_item("resume_ckpt", resume_ckpt)?;
    config_dict_py.set_item("device", device)?;

    let args = (config_dict_py, );
    let result = module.call_method1("VQAInferenceEngine", args)?;
    Ok(result.to_object(py))
}

pub fn vqa_model_inference(iq_pair: VQAImageQuestionPair, resources: &Rc<PyResources>) -> PyResult<VQAAnswer> {
    let pool = unsafe {resources.gil_guard.python().new_pool()};
    let py = pool.python();
    let uid = iq_pair.uid;

    let question = iq_pair.question;
    let vqa_model = resources.vqa_engine.as_ref().unwrap();
    let result = match iq_pair.image_feat {
        CNNFeat::ConvFeat(feat) => {
            let feat = feat.into_pyarray(py);
            let args = (feat, question);
            vqa_model.as_ref(py).call_method1("generate_answer", args)?
        },
        CNNFeat::FlattenFeat(feat) => {
            let feat = feat.into_pyarray(py);
            let args = (feat, question);
            vqa_model.as_ref(py).call_method1("generate_answer", args)?
        }
    };
    let answer = result.extract()?;

    Ok(VQAAnswer{
        uid,
        answer
    })
}