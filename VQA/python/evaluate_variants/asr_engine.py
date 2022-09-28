from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
from transformers import HubertForCTC, SEWDForCTC
from transformers import Speech2TextProcessor, Speech2TextForConditionalGeneration
import torch
import torch.nn as nn


class SpeechRecognitionEngine:
    def __init__(self, arch, cuda=True, data_parallel=True, device=None):
        compression = None 

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

        if not device:
            if data_parallel and not self.arch.startswith("s2t"):
                self.model = nn.DataParallel(self.model).cuda()
                if not cuda:
                    raise ValueError
            if cuda:
                self.model.cuda()
        else:
            if device == 'cpu':
                self.model = self.model.to("cpu")
            else:
                self.model = self.model.to("cuda:{:d}".format(device))

        self.model.eval()

def transcribe(self, input_values, attention_mask=None):
    with torch.no_grad():
        if not self.arch.startswith("s2t"):
            logits = self.model(input_values, attention_mask=attention_mask).logits
        else:
            gen_tokens = self.model.generate(inputs=input_values, attention_mask=attention_mask)

    if not self.arch.startswith("s2t"):
        predicted_ids = torch.argmax(logits, dim=-1)
        transcription = self.processor.batch_decode(predicted_ids)
    else:
        transcription = self.processor.batch_decode(gen_tokens, skip_special_tokens=True)
    return transcription