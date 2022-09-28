import argparse
import os
from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
import torch
import librosa
import pickle
from tqdm import tqdm
from utils import read_dataset

# python python/models_profiler/extract_transcriptions.py --dataset /data/VQA_Workflow_Datasets --device {DEVICE}

NUM_INSTANCES = 4096
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
MODEL_CHOICE = "wav2vec2-large-960h-lv60-self"

parser = argparse.ArgumentParser(description='VQA question transcription generator')
parser.add_argument('--dataset', type=str, help='path to VQA workflow dataset')
parser.add_argument('--output', type=str, default='', help='output transcriptions path')
parser.add_argument('--device', type=str, default='cuda:0', help='which device to use')
parser.add_argument("--num_instances", type=int, default=1024, help='number of instances to evaulate')

class SpeechRecognitionEngine:
    def __init__(self, arch, device):
        self.model = None
        self.processor = None
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
    _, audio_paths = read_dataset(dataset_path, num_instances=NUM_INSTANCES)

    model = SpeechRecognitionEngine(MODEL_CHOICE, args.device)
    transcriptions = []
    for _, audio_path in tqdm(audio_paths):
        speech = librosa.load(audio_path, sr=16000)[0]
        transcript = model.forward(speech)
        transcriptions.append(transcript)
    
    if not args.output:
        args.output = dataset_path
    else:
        args.output = os.path.expanduser(args.output)

    if not os.path.exists(args.output):
        os.makedirs(args.output)
    
    transcription_path = os.path.join(args.output, "ProfileTranscriptions.pkl")
    with open(transcription_path, 'wb') as f:
        pickle.dump(transcriptions, f)
    

if __name__ == "__main__":
    main()