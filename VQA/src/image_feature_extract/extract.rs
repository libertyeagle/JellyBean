use std::rc::Rc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use numpy::{IntoPyArray, PyArray1, PyArray3};

use super::config::FeatureExtractionConfig;
use super::data::{VQAImage, VQAImageFeature, CNNFeat};
use crate::resources::PyResources;

pub fn setup_image_feature_extractor(config: FeatureExtractionConfig, module: &PyModule) -> PyResult<PyObject> {
    let py = module.py();
    let config_dict_py = PyDict::new(py);
    let model = config.model;
    let device = config.device;
    let img_size = config.image_size;
    let hub_dir = config.hub_dir;
    let use_att_feat = config.use_attention_feature;

    config_dict_py.set_item("model", model)?;
    config_dict_py.set_item("device", device)?;
    config_dict_py.set_item("img_size", img_size)?;
    config_dict_py.set_item("hub_dir", hub_dir)?;
    config_dict_py.set_item("use_att_feat", use_att_feat)?;

    let args = (config_dict_py, );
    let result = module.call_method1("ImageFeatureExtractor", args)?;
    Ok(result.to_object(py))
}

pub fn extract_features(img: VQAImage, resources: &Rc<PyResources>) -> PyResult<VQAImageFeature> {
    let pool = unsafe {resources.gil_guard.python().new_pool()};
    let py = pool.python();
    let uid = img.uid;

    let img = img.image.into_pyarray(py);
    let args = (img, );
    let image_model = resources.image_feature_extractor.as_ref().unwrap();

    let use_att_feat = resources.image_feature_extractor_config.as_ref().unwrap().use_attention_feature;
    let result = image_model.as_ref(py).call_method1("extract_features", args)?;
    let feat = if use_att_feat { 
        CNNFeat::ConvFeat(result.extract::<&PyArray3<f32>>()?.to_owned_array())
    }
    else {
        CNNFeat::FlattenFeat(result.extract::<&PyArray1<f32>>()?.to_owned_array())
    };

    Ok(VQAImageFeature{
        uid,
        feat
    })
}