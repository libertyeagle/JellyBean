import argparse
import os

from datasets import load_dataset
from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
from transformers import HubertForCTC, SEWDForCTC
from transformers import Speech2TextProcessor, Speech2TextForConditionalGeneration
import torch
from jiwer import wer


class SpeechRecognitionEvaluator:
    def __init__(self, config, device=0):
        arch = config["model-arch"]
        compression = config.get("model-compression", None)

        self.model = None
        self.processor = None
        if arch == "wav2vec2-base-960h":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-base-960h")
                self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-base-960h")
        elif arch == "wav2vec2-large-960h-lv60-self":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-large-960h-lv60-self")
                self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-large-960h-lv60-self")
        elif arch == "wav2vec2-large-robust-ft-libri-960h":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-large-robust-ft-libri-960h")
                self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-large-robust-ft-libri-960h")
        elif arch == "s2t-small-librispeech-asr":
            if compression is None:
                self.processor = Speech2TextProcessor.from_pretrained("facebook/s2t-small-librispeech-asr", do_upper_case=True)
                self.model = Speech2TextForConditionalGeneration.from_pretrained("facebook/s2t-small-librispeech-asr")
        elif arch == "s2t-medium-librispeech-asr":
            if compression is None:
                self.processor = Speech2TextProcessor.from_pretrained("facebook/s2t-medium-librispeech-asr", do_upper_case=True)
                self.model = Speech2TextForConditionalGeneration.from_pretrained("facebook/s2t-medium-librispeech-asr")
        elif arch == "s2t-large-librispeech-asr":
            if compression is None:
                self.processor = Speech2TextProcessor.from_pretrained("facebook/s2t-large-librispeech-asr", do_upper_case=True)
                self.model = Speech2TextForConditionalGeneration.from_pretrained("facebook/s2t-large-librispeech-asr")      
        elif arch == "hubert-large-ls960-ft":
            if compression is None: 
                self.processor = Wav2Vec2Processor.from_pretrained("facebook/hubert-large-ls960-ft")
                self.model = HubertForCTC.from_pretrained("facebook/hubert-large-ls960-ft")
        elif arch == "hubert-xlarge-ls960-ft":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("facebook/hubert-xlarge-ls960-ft")
                self.model = HubertForCTC.from_pretrained("facebook/hubert-xlarge-ls960-ft")
        elif arch == "sew-d-tiny-100k-ft-ls100h":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("asapp/sew-d-tiny-100k-ft-ls100h")
                self.model = SEWDForCTC.from_pretrained("asapp/sew-d-tiny-100k-ft-ls100h")
        elif arch == "sew-d-mid-400k-ft-ls100h":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("asapp/sew-d-mid-400k-ft-ls100h")
                self.model = SEWDForCTC.from_pretrained("asapp/sew-d-mid-400k-ft-ls100h")
        elif arch == "sew-d-base-plus-400k-ft-ls100h":
            if compression is None:
                self.processor = Wav2Vec2Processor.from_pretrained("asapp/sew-d-base-plus-400k-ft-ls100h")
                self.model = SEWDForCTC.from_pretrained("asapp/sew-d-base-plus-400k-ft-ls100h")
          
        if self.model == None:
            raise ValueError("invalid model configuration")

        self.arch = arch
        self.use_attn_mask = self.processor.feature_extractor.return_attention_mask
        self.model = self.model.to("cuda:{:d}".format(device))
        self.device = device
        self.model.eval()

    def evaluate_asr_performance(self, batch_size=8, split="test"):
        """Evaluate ASR model performance on LibriSpeech dataset
        Return average time per sample (in milliseconds)
        """
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        cache_dir = os.path.join(os.path.dirname(dir_path), "data/huggingface/datasets")
        librispeech_eval = load_dataset("librispeech_asr", "clean", split=split, cache_dir=cache_dir)

        def map_to_array(batch):
            batch["speech"] = batch["audio"]["array"]
            return batch
        librispeech_eval = librispeech_eval.map(map_to_array)

        with torch.no_grad(): 
            batch = librispeech_eval[:batch_size]["speech"] 
            if not self.arch.startswith("s2t"):
                input_values = self.processor(batch, return_tensors="pt", padding="longest", sampling_rate=16000).input_values
                self.model(input_values.to("cuda:{:d}".format(self.device)))
            else:
                features = self.processor(batch, return_tensors="pt", padding=True, sampling_rate=16000)
                input_features = features.input_features.to("cuda:{:d}".format(self.device))
                attention_mask = features.attention_mask.to("cuda:{:d}".format(self.device)) 
                self.model.generate(inputs=input_features, attention_mask=attention_mask)

        total_time = 0.0
        total_samples = 0
        def map_to_pred(batch):
            if not self.arch.startswith("s2t"):
                inputs = self.processor(batch["speech"], return_tensors="pt", padding="longest", sampling_rate=16000)
                input_values = inputs.input_values.to("cuda:{:d}".format(self.device))
                if self.use_attn_mask:
                    attention_mask = inputs.attention_mask.to("cuda:{:d}".format(self.device))
                else:
                    attention_mask = None

            else:
                features = self.processor(batch["speech"], return_tensors="pt", padding=True, sampling_rate=16000)
                input_features = features.input_features.to("cuda:{:d}".format(self.device))
                attention_mask = features.attention_mask.to("cuda:{:d}".format(self.device))

            nonlocal total_time
            nonlocal total_samples
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            with torch.no_grad():
                start.record()
                if not self.arch.startswith("s2t"):
                    logits = self.model(input_values, attention_mask=attention_mask).logits
                else:
                    gen_tokens = self.model.generate(inputs=input_features, attention_mask=attention_mask)
                    
            if not self.arch.startswith("s2t"):
                predicted_ids = torch.argmax(logits, dim=-1)
                transcription = self.processor.batch_decode(predicted_ids)
            else:
                transcription = self.processor.batch_decode(gen_tokens, skip_special_tokens=True)
            end.record()
            torch.cuda.synchronize()
            total_time += start.elapsed_time(end)
            total_samples += len(batch["speech"])               
            batch["transcription"] = transcription
            return batch
        
        result = librispeech_eval.map(map_to_pred, batched=True, batch_size=batch_size, remove_columns=["speech"])
        wer_score = wer(result["text"], result["transcription"])

        return wer_score, total_time / total_samples 


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ASR Performance Evaluation')
    parser.add_argument('--arch', '-a', type=str, default='wav2vec2-base-960h', help='model architecture')
    parser.add_argument('--compression', '-c', type=str, default=None, help='model compression')
    parser.add_argument('--batch_size', '-b', type=int, default=8, help='batch size')
    parser.add_argument('--device', '-d', type=int, default=0, help='which device to use')
    parser.add_argument('--log_file', '-f', type=str, help='where to save the results')

    args = parser.parse_args()

    config = {
        "model-arch": args.arch,
        "model-compression": args.compression
    }

    engine = SpeechRecognitionEvaluator(config, device=args.device)
    wer_score, avg_time = engine.evaluate_asr_performance(batch_size=args.batch_size)
    if args.log_file:
        with open(args.log_file, 'at') as f:
            print("arch={}, WER={:.4f}, avg_time/sample={:.6f} sec".format(args.arch, wer_score * 100, avg_time / 1000), file=f)
    else:
        print("arch={}, WER={:.4f}, avg_time/sample={:.6f} sec".format(args.arch, wer_score * 100, avg_time / 1000))