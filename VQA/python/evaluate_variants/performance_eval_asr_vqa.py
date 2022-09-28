import argparse
import os
import sys
import yaml
from tqdm import tqdm
import torch
from jiwer import wer

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(dir_path))
from asr_engine import SpeechRecognitionEngine
import vqa.lib.utils as utils
from vqa.datasets.vqa_processed import tokenize_mcb
import vqa.datasets as datasets
import vqa.datasets.vqa_speech as vqa_speech_dataset


parser = argparse.ArgumentParser(description='Generate VQA question transcriptions with ASR')

parser.add_argument('--path_opt', default='options/vqa/default.yaml', type=str, 
                    help='path to a yaml options file')
parser.add_argument('--workers', '-j', default=2, type=int, help='number of data loading workers')
parser.add_argument('--asr_model', type=str, default='wav2vec2-base-960h', help='ASR model')
parser.add_argument('--batch_size', '-b', type=int, default=32, help='batch size')

def main():
    global args
    args = parser.parse_args()

    options = {
        'vqa' : {
            'trainsplit': "train"
        }
    }
    if args.path_opt is not None:
        with open(args.path_opt, 'r') as handle:
            options_yaml = yaml.load(handle, Loader=yaml.SafeLoader)
        options = utils.update_values(options, options_yaml)

    asr_model = SpeechRecognitionEngine(args.asr_model, cuda=True, data_parallel=True)

    valset = vqa_speech_dataset.factory('val', asr_model.processor, options['vqa'])
    val_loader = valset.data_loader(batch_size=args.batch_size,
                                    num_workers=args.workers)
    wer_score, avg_time = compute_wer_score(val_loader, asr_model)
    print("WER={:.4f}, avg_time/sample={:.6f} sec".format(wer_score * 100, avg_time / 1000))    


def compute_wer_score(loader, asr_engine):
    results_transcription = []
    results_text = []

    total_time = 0.
    total_samples = 0
    for sample in tqdm(loader):
        question_text = sample["question"]
        speech_features = sample["speech_features"].cuda()
        if "speech_attn_mask" in sample:
            speech_attention_mask = sample["speech_attn_mask"].cuda()
        else:
            speech_attention_mask = None
        
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        start.record()
        transcription = asr_engine.transcribe(speech_features, speech_attention_mask)
        end.record()
        torch.cuda.synchronize()        
        total_time += start.elapsed_time(end)
        total_samples += len(question_text)

        results_text.extend(question_text)
        results_transcription.extend(transcription)

    wer_score = wer(results_text, results_transcription)

    # time in milliseconds
    return wer_score, total_time / total_samples

if __name__ == '__main__':
    main()
