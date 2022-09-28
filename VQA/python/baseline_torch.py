import __main__

import os
import json
import torch
import time
import argparse

from feature_extractor import ImageFeatureExtractor
from speech_recognition import SpeechRecognitionEngine
from vqa_inference import VQAInferenceEngine
from utils import read_audio_files, load_images

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

    
def read_dataset(dataset_path, num_instances):
    question_json_path = os.path.join(dataset_path, "MultipleChoice_mscoco_val2014_questions.json")
    questions = json.load(open(question_json_path, 'rt'))["questions"]

    image_dir = os.path.join(dataset_path, "coco_images/val2014")
    audio_dir = os.path.join(dataset_path, "questions_speech/val2014")

    image_paths = []
    audio_paths = []
    for question in questions:
        image_id = question["image_id"]
        question_id = question["question_id"]
        image_path = os.path.join(image_dir, "COCO_val2014_{:012d}.jpg".format(image_id))
        audio_path = os.path.join(audio_dir, "{}.flac".format(question_id))
        image_paths.append((question_id, image_path))
        audio_paths.append((question_id, audio_path))
    
    image_paths.sort(key=lambda x: x[0])
    audio_paths.sort(key=lambda x: x[0])

    image_paths = image_paths[:num_instances]
    audio_paths = audio_paths[:num_instances]
    image_paths = list(map(lambda x: (x[0], x[1][1]), enumerate(image_paths)))
    audio_paths = list(map(lambda x: (x[0], x[1][1]), enumerate(audio_paths)))

    return image_paths, audio_paths


def main():
    parser = argparse.ArgumentParser(description='VQA workload baseline - local PyTorch')
    parser.add_argument('--config', type=str, help='path config file')
    args = parser.parse_args()

    with open(args.config, 'rt') as config_file:
        config = json.load(config_file)

    if config is None:
        raise ValueError("No config file provided.")
    source_path = os.path.dirname(DIR_PATH)

    device = "cuda:0" if torch.cuda.is_available() else 'cpu'
    feature_extractor_config = {
        "model": config["image_model"],
        "device": device,
        "img_size": 448,
        "hub_dir": None,
        "use_att_feat": False
    }
    feature_extractor = ImageFeatureExtractor(feature_extractor_config)

    asr_config = {
        "model": config["asr_model"],
        "device": device,
        "sampling_rate": 16000,
        "cache_dir": None
    }
    asr_engine = SpeechRecognitionEngine(asr_config)

    image_model_choice = config["image_model"]
    if image_model_choice == "resnet18":
        yaml_path = os.path.join(source_path, "python/configs/variants/mutan_noatt_resnet18.yaml")
        ckpt_path = os.path.join(source_path, "python/trained_models/variants/mutan_noatt_resnet18")
    elif image_model_choice == "resnet34":
        yaml_path = os.path.join(source_path, "python/configs/variants/mutan_noatt_resnet34.yaml")
        ckpt_path = os.path.join(source_path, "python/trained_models/variants/mutan_noatt_resnet34")
    elif image_model_choice == "resnet50":
        yaml_path = os.path.join(source_path, "python/configs/variants/mutan_noatt_resnet50.yaml")
        ckpt_path = os.path.join(source_path, "python/trained_models/variants/mutan_noatt_resnet50")
    elif image_model_choice == "resnet101":
        yaml_path = os.path.join(source_path, "python/configs/variants/mutan_noatt_resnet101.yaml")
        ckpt_path = os.path.join(source_path, "python/trained_models/variants/mutan_noatt_resnet101")
    elif image_model_choice == "resnet152":
        yaml_path = os.path.join(source_path, "python/configs/variants/mutan_noatt_resnet152.yaml")
        ckpt_path = os.path.join(source_path, "python/trained_models/variants/mutan_noatt_resnet152")
    else:
        raise ValueError("invalid image model")
    vqa_config = {
        "config_path": yaml_path,
        "ckpt_path": ckpt_path,
        "resume_ckpt": "ckpt",
        "device": device
    }
    vqa_engine = VQAInferenceEngine(vqa_config)

    dataset_dir = os.path.expanduser(config["dataset_path"])
    num_instances = config.get("num_instances", 1024)
    image_paths, audio_paths = read_dataset(dataset_dir, num_instances=num_instances)
    image_paths = [x[1] for x in image_paths]
    audio_paths = [x[1] for x in audio_paths]
  
    images = []
    audios = []
    for image_path, audio_path in zip(image_paths, audio_paths):
        audios.append(read_audio_files(audio_path, sampling_rate=16000)) 
        images.append(load_images(image_path))
    
    if device == 'cpu':
        start = time.perf_counter()
    else:
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        start.record()
    for id, (image, audio) in enumerate(zip(images, audios)):
        question = asr_engine.transcribe_audio(audio)
        image_feat = feature_extractor.extract_features(image)
        answer = vqa_engine.generate_answer(image_feat, question)
        print("Image-Question Pair #{:d}, Answer={}".format(id, answer))

    if device == 'cpu':
        end = time.perf_counter()
        runtime = (end - start) * 1000
    else:
        end.record()
        torch.cuda.synchronize()        
        runtime = start.elapsed_time(end)

    runtime =  runtime / 1000.
    print("Total runtime={:.2f}s".format(runtime))
    print("Throughput={:.4f}".format(num_instances / runtime))


if __name__ == "__main__":
    main()