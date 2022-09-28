import argparse
import os
import sys
import yaml
import json
from tqdm import tqdm

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

    trainset = datasets.factory_VQA(options['vqa']['trainsplit'],
                                    options['vqa'])

    asr_model = SpeechRecognitionEngine(args.asr_model, cuda=True, data_parallel=True)

    valset = vqa_speech_dataset.factory('val', asr_model.processor, options['vqa'])
    val_loader = valset.data_loader(batch_size=args.batch_size,
                                    num_workers=args.workers)
    
    val_transcriptions = transcribe_quesitons(val_loader, asr_model)
    word_to_wid = trainset.word_to_wid
    max_question_length = options['vqa']['maxlength']
    save_transcriptions(val_transcriptions, asr_model.arch, valset.split_name(),
                        options['vqa']['dir'], word_to_wid, max_question_length)
    

def transcribe_quesitons(loader, asr_engine):
    results = []

    for sample in tqdm(loader):
        speech_features = sample["speech_features"].cuda()
        batch_size = sample['speech_features'].size(0)
        if "speech_attn_mask" in sample:
            speech_attention_mask = sample["speech_attn_mask"].cuda()
        else:
            speech_attention_mask = None

        transcription = asr_engine.transcribe(speech_features, speech_attention_mask)
        for j in range(batch_size):
            results.append({'question_id': sample['question_id'][j].item(),
                            'question_transcription': transcription[j]})

    return results


def save_transcriptions(results, asr_model_arch, split_name, dir_vqa, word_to_wid, max_question_len):
    results_dict = {}
    for question in results:
        question_str = question["question_transcription"]
        question_tokens = tokenize_mcb(question_str)
        question_len = min(len(question_tokens), max_question_len)
        question_wids = [0] * question_len
        for i, word in enumerate(question_tokens[:question_len]):
            if word in word_to_wid:
                question_wids[i] = word_to_wid[word]
            else:
                question_wids[i] = word_to_wid['UNK']
        results_dict[question["question_id"]] = {
            "question_transcription": question_str,
            "question_transcription_wids": question_wids
        }

    dir_trans = os.path.join(dir_vqa, "transcriptions/{}".format(asr_model_arch))
    transcription_filename = os.path.join(dir_trans, "{}_questions.json".format(split_name))
    os.system('mkdir -p ' + dir_trans)
    with open(transcription_filename, 'w') as handle:
        json.dump(results_dict, handle)


if __name__ == '__main__':
    main()
