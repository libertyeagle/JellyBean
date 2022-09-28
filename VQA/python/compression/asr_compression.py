import argparse
import os
import numpy as np

from datasets import load_dataset
from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
from transformers import HubertForCTC, SEWDForCTC
from transformers import Speech2TextProcessor, Speech2TextForConditionalGeneration
import torch

import torch_pruning as tp
from nni.algorithms.compression.pytorch.pruning import L1FilterPruner
from nni.compression.pytorch import ModelSpeedup

class SpeechRecognitionModelCompressor:
    def __init__(self, config, device=0):
        arch = config["arch"]

        self.model = None
        self.processor = None
        if arch == "wav2vec2-base-960h":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-base-960h")
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-base-960h")
        elif arch == "wav2vec2-large-960h-lv60-self":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-large-960h-lv60-self")
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-large-960h-lv60-self")
        elif arch == "wav2vec2-large-robust-ft-libri-960h":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-large-robust-ft-libri-960h")
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-large-robust-ft-libri-960h")
        elif arch == "s2t-small-librispeech-asr":
            self.processor = Speech2TextProcessor.from_pretrained("facebook/s2t-small-librispeech-asr", do_upper_case=True)
            self.model = Speech2TextForConditionalGeneration.from_pretrained("facebook/s2t-small-librispeech-asr")
        elif arch == "s2t-medium-librispeech-asr":
            self.processor = Speech2TextProcessor.from_pretrained("facebook/s2t-medium-librispeech-asr", do_upper_case=True)
            self.model = Speech2TextForConditionalGeneration.from_pretrained("facebook/s2t-medium-librispeech-asr")
        elif arch == "s2t-large-librispeech-asr":
            self.processor = Speech2TextProcessor.from_pretrained("facebook/s2t-large-librispeech-asr", do_upper_case=True)
            self.model = Speech2TextForConditionalGeneration.from_pretrained("facebook/s2t-large-librispeech-asr")      
        elif arch == "hubert-large-ls960-ft":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/hubert-large-ls960-ft")
            self.model = HubertForCTC.from_pretrained("facebook/hubert-large-ls960-ft")
        elif arch == "hubert-xlarge-ls960-ft":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/hubert-xlarge-ls960-ft")
            self.model = HubertForCTC.from_pretrained("facebook/hubert-xlarge-ls960-ft")
        elif arch == "sew-d-tiny-100k-ft-ls100h":
            self.processor = Wav2Vec2Processor.from_pretrained("asapp/sew-d-tiny-100k-ft-ls100h")
            self.model = SEWDForCTC.from_pretrained("asapp/sew-d-tiny-100k-ft-ls100h")
        elif arch == "sew-d-mid-400k-ft-ls100h":
            self.processor = Wav2Vec2Processor.from_pretrained("asapp/sew-d-mid-400k-ft-ls100h")
            self.model = SEWDForCTC.from_pretrained("asapp/sew-d-mid-400k-ft-ls100h")
        elif arch == "sew-d-base-plus-400k-ft-ls100h":
            self.processor = Wav2Vec2Processor.from_pretrained("asapp/sew-d-base-plus-400k-ft-ls100h")
            self.model = SEWDForCTC.from_pretrained("asapp/sew-d-base-plus-400k-ft-ls100h")
          
        if self.model == None:
            raise ValueError("invalid model configuration")

        self.arch = arch
        self.use_attn_mask = self.processor.feature_extractor.return_attention_mask
        self.model = self.model.to("cuda:{:d}".format(device))
        self.device = device
        self.model.eval()

    
    def channel_pruning(self, tool='torch-pruning'):
        """Evaluate ASR model performance on LibriSpeech dataset
        Return average time per sample (in milliseconds)
        """
        librispeech_dummy = load_dataset("patrickvonplaten/librispeech_asr_dummy", "clean", split="validation")
        def map_to_array(batch):
            batch["speech"] = batch["audio"]["array"]
            return batch
        librispeech_dummy = librispeech_dummy.map(map_to_array)

        dir_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        dir_path = os.path.join(dir_path, "comp_models")
        os.system('mkdir -p ' + dir_path)

        if tool == "nni":
            if not self.arch.startswith("s2t"):
                dummy_inputs = self.processor(librispeech_dummy[:2]["speech"], return_tensors="pt", padding="longest", sampling_rate=16000)
                dummy_input_values = dummy_inputs.input_values.to("cuda:{:d}".format(self.device))
                if self.use_attn_mask:
                    dummy_attention_mask = dummy_inputs.attention_mask.to("cuda:{:d}".format(self.device))
                else:
                    dummy_attention_mask = None

                logits = self.model(dummy_input_values, attention_mask=dummy_attention_mask).logits
                print("orig: input: {} => output: {}".format(dummy_input_values.size(), logits.size()))
                params = sum([np.prod(p.size()) for p in self.model.parameters()])
                print("orig model #parameters: %.1fM"%(params / 1e6))

                config_list = [{ 'sparsity': 0.8, 'op_types': ['Conv2d'] }]
                pruner = L1FilterPruner(self.model, config_list)
                pruner.compress()
                model_save_path = os.path.join(dir_path, "{}_channel_pruned_model.pth".format(self.arch)) 
                mask_save_path = os.path.join(dir_path, "{}_channel_pruned_mask.pth".format(self.arch)) 
                pruner.export_model(model_path=model_save_path, mask_path=mask_save_path)
                m_speedup = ModelSpeedup(self.model, (dummy_input_values, dummy_attention_mask), mask_save_path)
                m_speedup.speedup_model()
                print("pruned model #parameters with NNI speedup: %.1fM"%(params / 1e6))      
                logits = self.model(dummy_input_values, attention_mask=dummy_attention_mask).logits
                print("pruned: input: {} => output: {}".format(dummy_input_values.size(), logits.size()))                    

        else:
            raise ValueError("invalid pruning tool")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract')
    parser.add_argument('--arch', '-a', type=str, default='wav2vec2-base-960h', help='model architecture')
    parser.add_argument('--device', '-d', type=int, default=0, help='which device to use')

    args = parser.parse_args()
    config = {
        "arch": args.arch
    }

    engine = SpeechRecognitionModelCompressor(config, device=args.device)
    
    engine.channel_pruning('nni')