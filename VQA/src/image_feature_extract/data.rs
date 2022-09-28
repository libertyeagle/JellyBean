use abomonation_derive::Abomonation;
use ndarray::{Array1, Array3};

use crate::data::{ArrayContainer1, ArrayContainer3};

pub struct VQAImage {
    pub uid: u64,
    pub image: Array3<u8>,
}

#[derive(Abomonation, Clone, Debug)]
pub struct VQAImageContained {
    pub uid: u64,
    pub image: ArrayContainer3<u8>
}

impl From<VQAImage> for VQAImageContained {
    fn from(img: VQAImage) -> Self {
        VQAImageContained {
            uid: img.uid,
            image: img.image.into()
        }
    }
}

impl From<VQAImageContained> for VQAImage {
    fn from(img: VQAImageContained) -> Self {
        VQAImage {
            uid: img.uid,
            image: img.image.into()
        }
    }
}

pub enum CNNFeat {
    ConvFeat(Array3<f32>),
    FlattenFeat(Array1<f32>)
}

#[derive(Abomonation, Clone, Debug)]
pub enum CNNFeatContained {
    ConvFeat(ArrayContainer3<f32>),
    FlattenFeat(ArrayContainer1<f32>)
}

impl From<CNNFeat> for CNNFeatContained {
    fn from(feat: CNNFeat) -> Self {
        match feat {
            CNNFeat::ConvFeat(feat) => CNNFeatContained::ConvFeat(feat.into()),
            CNNFeat::FlattenFeat(feat) => CNNFeatContained::FlattenFeat(feat.into())
        }
    }
}

impl From<CNNFeatContained> for CNNFeat {
    fn from(feat: CNNFeatContained) -> Self {
        match feat {
            CNNFeatContained::ConvFeat(feat) => CNNFeat::ConvFeat(feat.into()),
            CNNFeatContained::FlattenFeat(feat) => CNNFeat::FlattenFeat(feat.into())
        }        
    }
}

pub struct VQAImageFeature {
    pub uid: u64,
    pub feat: CNNFeat
}
#[derive(Abomonation, Clone, Debug)]
pub struct VQAImageFeatureContained {
    pub uid: u64,
    pub feat: CNNFeatContained
}

impl From<VQAImageFeature> for VQAImageFeatureContained {
    fn from(img_feat: VQAImageFeature) -> Self {
        VQAImageFeatureContained {
            uid: img_feat.uid,
            feat: img_feat.feat.into()
        }
    }
}

impl From<VQAImageFeatureContained> for VQAImageFeature {
    fn from(img_feat: VQAImageFeatureContained) -> Self {
        VQAImageFeature {
            uid: img_feat.uid,
            feat: img_feat.feat.into()
        }
    }
}