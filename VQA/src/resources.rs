use std::path::Path;

use pyo3::GILGuard;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::config::ResourcesSetupConfig;
use crate::image_feature_extract::config::FeatureExtractionConfig;
use crate::image_feature_extract::extract::setup_image_feature_extractor;
use crate::vqa_inference::inference::setup_vqa_engine;
use crate::speech_recognition::transcribe::setup_asr_engine;

pub struct PyResources {
    pub(crate) gil_guard: GILGuard,
    pub(crate) asr_engine: Option<PyObject>,
    pub(crate) image_feature_extractor: Option<PyObject>,
    pub(crate) image_feature_extractor_config: Option<FeatureExtractionConfig>,
    pub(crate) vqa_engine: Option<PyObject>,
    pub(crate) utils: Option<Py<PyModule>>,
}

impl PyResources {
    pub fn new(config: ResourcesSetupConfig) -> Self {
        let src_dir = env!("CARGO_MANIFEST_DIR");
        let gil = Python::acquire_gil();
        let pool = unsafe {gil.python().new_pool()};
        let py = pool.python();
        let syspath = py.import("sys")
            .unwrap()
            .getattr("path")
            .unwrap()
            .downcast::<PyList>()
            .unwrap();        
        let py_src_dir = Path::new(src_dir).join("python");
        let py_src_dir = py_src_dir.to_str().unwrap();
        syspath.insert(0, py_src_dir).unwrap();

        let image_model_config = config.load_image_model.clone();
        let image_model = match config.load_image_model {
            Some(config) => {
                let module = py.import("feature_extractor").unwrap();
                let model = setup_image_feature_extractor(config, module).unwrap();
                Some(model)
            },
            None => None
        };

        let asr_model = match config.load_asr_model {
            Some(config) => {
                let module = py.import("speech_recognition").unwrap();
                let model = setup_asr_engine(config, module).unwrap();
                Some(model)
            },
            None => None
        };

        let vqa_engine = match config.load_vqa_model {
            Some(config) => {
                let module = py.import("vqa_inference").unwrap();
                let model = setup_vqa_engine(config, module).unwrap();
                Some(model)
            },
            None => None
        };        

        let utils_module = if config.load_utils_module {
            let module: Py<PyModule> = py.import("utils").unwrap().into_py(py);
            Some(module)
        }
        else {
            None
        };

        PyResources {
            gil_guard: gil,
            asr_engine: asr_model,
            image_feature_extractor: image_model,
            image_feature_extractor_config: image_model_config,
            vqa_engine,
            utils: utils_module
        }
    }
}