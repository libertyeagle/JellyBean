from transformers import Wav2Vec2ForCTC, Wav2Vec2Processor
import torch
import librosa
import os


class SpeechRecognitionEngine:
    def __init__(self, config):
        device = config["device"]
        model = config["model"]
        sampling_rate = config["sampling_rate"] 
        cache_dir = config["cache_dir"]
        if not cache_dir:
            cache_dir = None
        
        device = str(device).strip().lower().replace('cuda:', '')  # to string, 'cuda:0' to '0'
        if device == 'cpu':
            self.device = torch.device('cpu')
        else:
            if device.isdecimal():
                self.device = torch.device('cuda:{:d}'.format(int(device)))
            else:
                self.device = torch.device('cuda:0')       

        if model == "wav2vec2-base-960h":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-base-960h", cache_dir=cache_dir)
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-base-960h", cache_dir=cache_dir)
        elif model == "wav2vec2-large-960h-lv60-self":
            self.processor = Wav2Vec2Processor.from_pretrained("facebook/wav2vec2-large-960h-lv60-self", cache_dir=cache_dir)
            self.model = Wav2Vec2ForCTC.from_pretrained("facebook/wav2vec2-large-960h-lv60-self", cache_dir=cache_dir)
        else:
            raise ValueError("invalid moodel choice: {}".format(model))

        if sampling_rate != self.processor.feature_extractor.sampling_rate:
            raise ValueError("Sampling rate does not match that the pretrained model is trained on")

        self.model = self.model.to(self.device)

    def transcribe_audio(self, raw_speech, sampling_rate=16000):
        """Transcribe audio files
        Parameters
        ----------
        raw_speech: (`np.ndarray`, `List[np.ndarray]`)
            a single audio sequence or a batch of audio sequences
            the raw speech should be sampled at sampling_rate
            caller should be responsible that raw_speech is sampled at the sampling rate the model trained at
        sampling_rate: int    
        Returns
        -------
        (`str`, `List[str]`)
            transcribed text
        """
        inputs = self.processor(raw_speech, padding="longest", return_tensors="pt", sampling_rate=sampling_rate)
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

    def read_audio_files(self, paths):
        """Read audio files
        paths: (`str`, `List[str]`)
            a filename or a batch of filenames
        Returns
        -------
        (`np.ndarray`, `List[np.ndarray]`)
            Raw audio waveform
        """
        sampling_rate = self.processor.feature_extractor.sampling_rate
        raw_speech = []
        if type(paths) is not list:
            paths = [paths]
        for path in paths:
            if not os.path.isfile(path):
                raise ValueError("File does not exist!")
            raw_speech.append(librosa.load(path, sr=sampling_rate, mono=True)[0])        
        if len(raw_speech) > 1:
            return raw_speech
        else:
            return raw_speech[0]