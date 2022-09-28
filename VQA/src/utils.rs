use std::collections::VecDeque;
use std::rc::Rc;
use std::path::Path;
use std::convert::AsRef;
use std::fs::File;
use std::io::BufReader;

use numpy::{PyArray3, PyArray1};
use pyo3::prelude::*;
use serde_json::Value as JSONValue;

use crate::resources::PyResources;
use crate::image_feature_extract::data::VQAImage;
use crate::speech_recognition::data::VQAQuestionRawSpeech;


pub fn read_image(path: impl AsRef<Path>, uid: u64, resources: &Rc<PyResources>) -> PyResult<VQAImage> {
    let pool = unsafe {resources.gil_guard.python().new_pool()};
    let py = pool.python();
    let utils_module = resources.utils.as_ref().unwrap();

    let image_path = path.as_ref().to_str().unwrap().to_string();
    let args = (image_path, );
    let result = utils_module.as_ref(py).call_method1("load_images", args)?;

    let image = result.extract::<&PyArray3<u8>>()?.to_owned_array();

    Ok(VQAImage{
        uid,
        image
    })
}

pub fn read_audio(path: impl AsRef<Path>, uid: u64, sampling_rate: u32, resources: &Rc<PyResources>) -> PyResult<VQAQuestionRawSpeech> {
    let pool = unsafe {resources.gil_guard.python().new_pool()};
    let py = pool.python();
    let utils_module = resources.utils.as_ref().unwrap();

    let audio_path = path.as_ref().to_str().unwrap().to_string();
    let args = (audio_path, sampling_rate);
    let result = utils_module.as_ref(py).call_method1("read_audio_files", args)?;
    let waveform = result.extract::<&PyArray1<f32>>()?.to_owned_array();

    Ok(VQAQuestionRawSpeech {
        uid,
        sampling_rate,
        waveform
    })
}

pub fn read_dataset(dataset_root: impl AsRef<Path>, num_instances: usize) -> (VecDeque<(u64, String)>, VecDeque<(u64, String)>) {
    let json_path = dataset_root.as_ref().join("MultipleChoice_mscoco_val2014_questions.json");
    let file = File::open(json_path).unwrap();
    let reader = BufReader::new(file);

    let image_dir = dataset_root.as_ref().join("coco_images/val2014");
    let audio_dir = dataset_root.as_ref().join("questions_speech/val2014");
    let mut image_paths = Vec::new();
    let mut audio_paths = Vec::new();

    
    let content: JSONValue = serde_json::from_reader(reader).unwrap();
    if let JSONValue::Array(questions) = &content["questions"] {
        for question in questions {
            let image_id = match &question["image_id"] {
                JSONValue::Number(id) => id.as_u64().unwrap(),
                _ => panic!("fail to parse image_id")
            };
            let question_id = match &question["question_id"] {
                JSONValue::Number(id) => id.as_u64().unwrap(),
                _ => panic!("fail to parse image_id")
            };
            let image_path = image_dir.join(format!("COCO_val2014_{:012}.jpg", image_id)).to_str().unwrap().to_owned();
            let audio_path = audio_dir.join(format!("{}.flac", question_id)).to_str().unwrap().to_owned();
            image_paths.push((question_id, image_path));
            audio_paths.push((question_id, audio_path));
        }
    }

    image_paths.sort_by(|x, y| x.0.cmp(&y.0));
    audio_paths.sort_by(|x, y| x.0.cmp(&y.0));

    let image_paths = image_paths.drain(..num_instances).collect::<Vec<_>>();
    let audio_paths = audio_paths.drain(..num_instances).collect::<Vec<_>>();
    let image_paths = image_paths.into_iter().enumerate().map(|(i, (_x,y))| (i as u64, y)).collect::<Vec<_>>();
    let audio_paths = audio_paths.into_iter().enumerate().map(|(i, (_x,y))| (i as u64, y)).collect::<Vec<_>>();

    let image_paths = VecDeque::from(image_paths);
    let audio_paths = VecDeque::from(audio_paths);

    (image_paths, audio_paths)
}