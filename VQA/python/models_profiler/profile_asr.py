import argparse
import os
import json
import time
from collections import OrderedDict
from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
import torch
import numpy as np
import librosa
from tqdm import tqdm
from utils import read_dataset

# python python/models_profiler/profile_asr.py --dataset /data/VQA_Workflow_Datasets --output ~/VQAWorkflowProfile --device {DEVICE}

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
MODEL_LISTS = [
    "wav2vec2-base-960h",
    "wav2vec2-large-960h-lv60-self",
]

parser = argparse.ArgumentParser(description='ASR model profiler')
parser.add_argument('--dataset', type=str, help='path to VQA workflow dataset')
parser.add_argument('--device', type=str, default='cuda:0', help='which device to use')
parser.add_argument('--output', type=str, default='', help='output profile to file')
parser.add_argument("--num_instances", type=int, default=1024, help='number of instances to evaulate')

class SpeechRecognitionEngine:
    def __init__(self, arch, device):
        self.model = None
        self.processor = None
        if arch == "wav2vec2-base-960h":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-base-960h")
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-base-960h")
        elif arch == "wav2vec2-large-960h-lv60-self":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-large-960h-lv60-self")
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-large-960h-lv60-self")
          
        if self.model == None:
            raise ValueError("invalid model configuration")

        self.arch = arch
        self.use_attn_mask = self.processor.feature_extractor.return_attention_mask

        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0')       

        self.model = self.model.to(self.device)
        self.model.eval()

    def forward(self, speech):
        inputs = self.processor(speech, padding="longest", return_tensors="pt", sampling_rate=16000)
        speech_features = inputs.input_values if hasattr(inputs, "input_values") else inputs.input_features
        if self.processor.feature_extractor.return_attention_mask:
            attn_mask = inputs.attention_mask
            attn_mask = attn_mask.to(self.device)
        else:
            attn_mask = None

        speech_features = speech_features.to(self.device)
        with torch.no_grad():
            logits = self.model(speech_features, attention_mask=attn_mask).logits.squeeze(0)
            predicted_ids = torch.argmax(logits, dim=-1)

        if predicted_ids.ndim > 1:
            transcriptions = self.processor.batch_decode(predicted_ids, skip_special_tokens=True)
        else:
            transcriptions = self.processor.decode(predicted_ids, skip_special_tokens=True)

        return transcriptions

def main():
    global args
    args = parser.parse_args()

    dataset_path = os.path.expanduser(args.dataset)
    _, audio_paths = read_dataset(dataset_path, num_instances=args.num_instances)

    dummy_audio = np.random.randn(45000)
    model_profiles = OrderedDict()
    for model_choice in MODEL_LISTS:
        model = SpeechRecognitionEngine(model_choice, args.device)
        latency_logs = []
        # warmup
        model.forward(dummy_audio)
        model.forward(dummy_audio)

        if args.device == 'cpu':
            for _, audio_path in tqdm(audio_paths):
                speech = librosa.load(audio_path, sr=16000)[0]
                start = time.perf_counter()
                model.forward(speech)
                end = time.perf_counter()
                latency = (end - start) * 1000
                latency_logs.append(latency)
        else:
            for _, audio_path in tqdm(audio_paths):
                speech = librosa.load(audio_path, sr=16000)[0]
                start = torch.cuda.Event(enable_timing=True)
                end = torch.cuda.Event(enable_timing=True)
                start.record()
                model.forward(speech)
                end.record()
                torch.cuda.synchronize()
                latency = start.elapsed_time(end)
                latency_logs.append(latency)
                
        profile = OrderedDict()
        profile["mean"] = np.mean(latency_logs)
        profile["std"] = np.std(latency_logs)
        profile["min"] = np.min(latency_logs)
        profile["max"] = np.max(latency_logs)        
        profile["P10"] = np.percentile(latency_logs, q=10)
        profile["P50"] = np.percentile(latency_logs, q=50)
        profile["P75"] = np.percentile(latency_logs, q=75)
        profile["P90"] = np.percentile(latency_logs, q=90)
        profile["P95"] = np.percentile(latency_logs, q=95)
        profile["P99"] = np.percentile(latency_logs, q=99)
        print('=' * 10)
        print(model_choice)
        print(profile)
        model_profiles[model_choice] = profile
    
    if args.output:
        args.output = os.path.expanduser(args.output)
        if not os.path.exists(args.output):
            os.makedirs(args.output)
        profile_path = os.path.join(args.output, "profile_speech_recognition.json")
        json.dump(model_profiles, open(profile_path, 'wt'), indent=4)

if __name__ == "__main__":
    main()        