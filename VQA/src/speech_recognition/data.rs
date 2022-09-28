use abomonation_derive::Abomonation;
use ndarray::Array1;

use crate::data::ArrayContainer1;

#[derive(Clone, Debug)]
pub struct VQAQuestionRawSpeech {
    pub uid: u64,
    pub sampling_rate: u32,
    pub waveform: Array1<f32>
}

#[derive(Abomonation, Clone, Debug)]
pub struct VQAQuestionRawSpeechContained {
    pub uid: u64,
    pub sampling_rate: u32,
    pub waveform: ArrayContainer1<f32>
}

impl From<VQAQuestionRawSpeech> for VQAQuestionRawSpeechContained {
    fn from(speech: VQAQuestionRawSpeech) -> Self {
        VQAQuestionRawSpeechContained {
            uid: speech.uid,
            sampling_rate: speech.sampling_rate,
            waveform: speech.waveform.into()
        }
    }
}

impl From<VQAQuestionRawSpeechContained> for VQAQuestionRawSpeech {
    fn from(speech: VQAQuestionRawSpeechContained) -> Self {
        VQAQuestionRawSpeech {
            uid: speech.uid,
            sampling_rate: speech.sampling_rate,
            waveform: speech.waveform.into()
        }
    }
}
#[derive(Abomonation, Clone, Debug)]
pub struct VQAQuestionText {
    pub uid: u64,
    pub text: String
}