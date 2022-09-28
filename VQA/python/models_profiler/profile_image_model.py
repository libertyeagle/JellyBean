import argparse
import os
import sys
import json
import time
from collections import OrderedDict
import torch
import numpy as np
from tqdm import tqdm
import torch
import torchvision.transforms as transforms
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(DIR_PATH))
import vqa.models.convnets as convnets

# python python/models_profiler/profile_image_model.py --output ~/VQAWorkflowProfile --device {DEVICE}

NUM_TRAILS = 200
MODEL_LISTS = [
    "resnet18",
    "resnet34",
    "resnet50",
    "resnet101",
    "resnet152"
]

parser = argparse.ArgumentParser(description='ASR model profiler')
parser.add_argument('--device', '-d', type=str, default='cuda:0', help='which device to use')
parser.add_argument('--output', '-o', type=str, default='', help='output profile to file')

class ImageFeatureExtractor():
    def __init__(self, model_choice, device):
        normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
        # image transform pipelines                                                                        
        self.transform = transforms.Compose([
            transforms.ToPILImage(),
            transforms.Scale(448),
            transforms.CenterCrop(448),
            transforms.ToTensor(),
            normalize,
        ])

        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0')
    
        opt_factory_cnn = {
            'arch': model_choice
        }
        self.att_feat = False
        self.image_model = convnets.factory(opt_factory_cnn)
        self.image_model = self.image_model.to(self.device)
        self.image_model.eval()

    def extract_features(self, images):
        with torch.no_grad():
            tensor_images = []
            if images.ndim == 4:
                images = np.ascontiguousarray(images[:, :, :, ::-1])
                num_images = images.shape[0]
                for i in range(num_images):
                    image = self.transform(images[i])
                    tensor_images.append(image)
            else:
                images = np.ascontiguousarray(images[:, :, ::-1])
                tensor_images.append(self.transform(images))

            tensor_images = torch.stack(tensor_images, dim=0)
            tensor_images = tensor_images.to(self.device)
            visual_features = self.image_model(tensor_images)
            
            if not self.att_feat:
                nb_regions = visual_features.size(2) * visual_features.size(3)
                visual_features = visual_features.sum(3).sum(2).div(nb_regions).view(-1, visual_features.size(1))

            visual_features = visual_features.squeeze(0)
            visual_features = visual_features.cpu().numpy()

        return visual_features


def main():
    global args
    args = parser.parse_args()

    dummy_image = np.random.randint(0, 255, size=(448, 448, 3), dtype=np.uint8)
    model_profiles = OrderedDict()
    for model_choice in MODEL_LISTS:
        model = ImageFeatureExtractor(model_choice, args.device)
        latency_logs = []
        # warmup
        model.extract_features(dummy_image)
        model.extract_features(dummy_image)
        if args.device == 'cpu':
            for _ in tqdm(range(NUM_TRAILS)):
                start = time.perf_counter()
                model.extract_features(dummy_image)
                end = time.perf_counter()
                latency = (end - start) * 1000
                latency_logs.append(latency)
        else:
            for _ in tqdm(range(NUM_TRAILS)):
                start = torch.cuda.Event(enable_timing=True)
                end = torch.cuda.Event(enable_timing=True)
                start.record()
                model.extract_features(dummy_image)
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
        profile_path = os.path.join(args.output, "profile_image_model.json")        
        json.dump(model_profiles, open(profile_path, 'wt'), indent=4)

if __name__ == "__main__":
    main()