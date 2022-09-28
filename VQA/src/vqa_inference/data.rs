use abomonation_derive::Abomonation;

use crate::image_feature_extract::data::{CNNFeat, CNNFeatContained};

pub struct VQAImageQuestionPair {
    pub uid: u64,
    pub image_feat: CNNFeat,
    pub question: String,
}

#[derive(Abomonation, Clone, Debug)]
pub struct VQAImageQuestionPairContained {
    pub uid: u64,
    pub feat: CNNFeatContained
}

#[derive(Abomonation, Clone, Debug)]
pub struct VQAAnswer { 
    pub uid: u64,
    pub answer: String
}
