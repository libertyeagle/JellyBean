import time
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
ROOT_PATH = os.path.dirname(DIR_PATH)
sys.path.append(ROOT_PATH)
import yaml
import json
import pickle
from collections import OrderedDict
import argparse
from tqdm import tqdm

import torch
from torch.nn.utils.rnn import pad_sequence
import numpy as np

import vqa.lib.utils as utils
import vqa.datasets as datasets
import vqa.models as models
from vqa.datasets.vqa_processed import tokenize_mcb
from train import load_checkpoint

# python python/models_profiler/profile_vqa.py --dataset /data/VQA_Workflow_Datasets --output ~/VQAWorkflowProfile --device {DEVICE}

MODEL_LISTS = [
    "resnet18",
    "resnet34",
    "resnet50",
    "resnet101",
    "resnet152"
]

parser = argparse.ArgumentParser(description='VQA model profiler')
parser.add_argument('--dataset', type=str, help='path to VQA workflow dataset')
parser.add_argument('--device', type=str, default='cuda:0', help='which device to use')
parser.add_argument('--output', type=str, default='', help='output profile to file')
parser.add_argument("--num_instances", type=int, default=1024, help='number of instances to evaulate')

class VQAInferenceEngine:
    def __init__(self, image_model_choice, device):
        if image_model_choice == "resnet18":
            yaml_path = os.path.join(ROOT_PATH, "configs/variants/mutan_noatt_resnet18.yaml")
            ckpt_path = os.path.join(ROOT_PATH, "trained_models/variants/mutan_noatt_resnet18")
        elif image_model_choice == "resnet34":
            yaml_path = os.path.join(ROOT_PATH, "configs/variants/mutan_noatt_resnet34.yaml")
            ckpt_path = os.path.join(ROOT_PATH, "trained_models/variants/mutan_noatt_resnet34")
        elif image_model_choice == "resnet50":
            yaml_path = os.path.join(ROOT_PATH, "configs/variants/mutan_noatt_resnet50.yaml")
            ckpt_path = os.path.join(ROOT_PATH, "trained_models/variants/mutan_noatt_resnet50")
        elif image_model_choice == "resnet101":
            yaml_path = os.path.join(ROOT_PATH, "configs/variants/mutan_noatt_resnet101.yaml")
            ckpt_path = os.path.join(ROOT_PATH, "trained_models/variants/mutan_noatt_resnet101")
        elif image_model_choice == "resnet152":
            yaml_path = os.path.join(ROOT_PATH, "configs/variants/mutan_noatt_resnet152.yaml")
            ckpt_path = os.path.join(ROOT_PATH, "trained_models/variants/mutan_noatt_resnet152")
        else:
            raise ValueError("invalid image model")
        resume_ckpt = "ckpt"

        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0')

        options = {
            'logs': {
                'dir_logs': ckpt_path
            },
        }    
        with open(yaml_path, 'r') as handle:
            options_yaml = yaml.safe_load(handle)
        options = utils.update_values(options, options_yaml)

        self.max_question_len = options['vqa']['maxlength']
        self.att_feat = not "NoAtt" in options['model']['arch']

        trainset = datasets.factory_VQA(options['vqa']['trainsplit'], options['vqa'])
        self.word_to_wid = trainset.word_to_wid
        self.aid_to_ans = trainset.aid_to_ans

        self.vqa_model = models.factory(options['model'],
                            trainset.vocab_words(),
                            trainset.vocab_answers())
        self.vqa_model = self.vqa_model.to(self.device)
        self.vqa_model.eval()
        load_checkpoint(self.vqa_model, None,
            os.path.join(options['logs']['dir_logs'], resume_ckpt))

    def generate_answer(self, image_features, questions):
        image_features = torch.from_numpy(image_features).to(self.device)
        image_features = image_features.unsqueeze(0)
        questions = self.process_questions(questions)
        questions = questions.to(self.device)
        with torch.no_grad():
            answers_scores = self.vqa_model(image_features, questions)
            answers_scores = torch.nn.functional.softmax(answers_scores, dim=-1)
            # answers_ids: (batch_size, )
            answers_ids = torch.argmax(answers_scores, dim=1)
            answers_ids = answers_ids.cpu().numpy()

        batch_answers = [self.aid_to_ans[aid] for aid in answers_ids]
        if len(batch_answers) == 1:
            batch_answers = batch_answers[0]

        return batch_answers

    def process_questions(self, questions):
        if type(questions) is not list:
            questions = [questions]
        with torch.no_grad():
            questions_wids = []
            for question_str in questions:
                question_tokens = tokenize_mcb(question_str)
                question_len = min(len(question_tokens), self.max_question_len)
                question_data = torch.zeros(question_len, dtype=torch.long)
                for i, word in enumerate(question_tokens[:question_len]):
                    if word in self.word_to_wid:
                        question_data[i] = self.word_to_wid[word]
                    else:
                        question_data[i] = self.word_to_wid['UNK']
                questions_wids.append(question_data)
            questions_wids = pad_sequence(questions_wids, batch_first=True)
        return questions_wids



def main():
    global args
    args = parser.parse_args()

    dataset_path = os.path.expanduser(args.dataset)
    with open(os.path.join(dataset_path, "ProfileTranscriptions.pkl"), 'rb') as f:
        transcriptions = pickle.load(f)[:args.num_instances]

    
    model_profiles = OrderedDict()
    for image_model_choice in MODEL_LISTS:
        model = VQAInferenceEngine(image_model_choice, args.device)
        if image_model_choice in ["resnet18", "resnet34"]:
            image_feat = np.random.randn(512).astype(np.float32)
        else:
            image_feat = np.random.randn(2048).astype(np.float32)
        latency_logs = []
        # warmup
        model.generate_answer(image_feat, transcriptions[0])
        model.generate_answer(image_feat, transcriptions[0])

        if args.device == 'cpu':
            for transcript in tqdm(transcriptions):
                start = time.perf_counter()
                model.generate_answer(image_feat, transcript)
                end = time.perf_counter()
                latency = (end - start) * 1000
                latency_logs.append(latency)
        else:
            for transcript in tqdm(transcriptions):
                start = torch.cuda.Event(enable_timing=True)
                end = torch.cuda.Event(enable_timing=True)
                start.record()
                model.generate_answer(image_feat, transcript)
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
        print(image_model_choice)
        print(profile)
        model_profiles[image_model_choice] = profile
    
    if args.output:
        args.output = os.path.expanduser(args.output)
        if not os.path.exists(args.output):
            os.makedirs(args.output)
        profile_path = os.path.join(args.output, "profile_vqa.json")
        json.dump(model_profiles, open(profile_path, 'wt'), indent=4)

if __name__ == "__main__":
    main()        