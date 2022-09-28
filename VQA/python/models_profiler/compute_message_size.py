import argparse
import os
import json
import numpy as np
import librosa
import cv2
from utils import read_dataset

# python python/models_profiler/compute_message_size.py --dataset /data/VQA_Workflow_Datasets --output ~/VQAWorkflowProfile
# measures are in kilobytes (KB)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
parser = argparse.ArgumentParser(description='ASR model profiler')
parser.add_argument('--dataset', type=str, help='path to VQA workflow dataset')
parser.add_argument('--output', type=str, default='', help='output profile to file')
parser.add_argument("--num_instances", type=int, default=1024, help='number of instances to evaulate')

def compute_stats(x, return_kilobytes=False):
    if return_kilobytes:
        return {
            "mean": np.mean(x) / 1024.,
            "std": np.std(x) / 1024.,
            "min": float(np.min(x)) / 1024.,
            "max": float(np.max(x)) / 1024.,
            "median": np.median(x) / 1024.,
            "P75": np.percentile(x, q=75) / 1024.,
            "P90": np.percentile(x, q=90) / 1024.,
            "P95": np.percentile(x, q=95) / 1024.,
        }
    else:
        return {
            "mean": np.mean(x),
            "std": np.std(x),
            "min": float(np.min(x)),
            "max": float(np.max(x)),
            "median": np.median(x),
            "P75": np.percentile(x, q=75),
            "P90": np.percentile(x, q=90),
            "P95": np.percentile(x, q=95),     
        }

def main():
    global args
    args = parser.parse_args()

    dataset_path = os.path.expanduser(args.dataset)
    image_paths, audio_paths = read_dataset(dataset_path, num_instances=args.num_instances)

    image_size = []
    audio_size = []
    for image_path, audio_path in zip(image_paths, audio_paths):
        image_path = image_path[1]
        audio_path = audio_path[1]
        img = cv2.imread(image_path, flags=cv2.IMREAD_COLOR)
        audio = librosa.load(audio_path, sr=16000, mono=True)[0]
        image_size.append(8 + img.size * img.itemsize)
        audio_size.append(8 + 4 + audio.size * audio.itemsize)
    
    metrics = {
        "VQAImage(Input->ImageModel)": compute_stats(image_size, return_kilobytes=True),
        "VQAQuestionRawSpeech(Input->SpeechRecognition)": compute_stats(audio_size, return_kilobytes=True),
        "ImageModel->VQA@512FeatDim": (4 + 512 * 4) / 1024.,
        "ImageModel->VQA@2048FeatDim": (4 + 2048 * 4) / 1024.,
        "SpeechRecognition->VQA": 100 / 1024.
    }
    print(metrics)

    if args.output:
        args.output = os.path.expanduser(args.output)
        if not os.path.exists(args.output):
            os.makedirs(args.output)
        profile_path = os.path.join(args.output, "message_size.json")
        json.dump(metrics, open(profile_path, 'wt'), indent=4)

if __name__ == "__main__":
    main()        