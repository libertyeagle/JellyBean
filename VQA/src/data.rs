use abomonation_derive::Abomonation;
use ndarray::Array;
use ndarray::{Array1, Array2, Array3, ArrayD};

pub use crate::image_feature_extract::data::{VQAImage, VQAImageFeature, VQAImageContained, VQAImageFeatureContained};
pub use crate::vqa_inference::data::{VQAImageQuestionPair, VQAImageQuestionPairContained, VQAAnswer};

#[derive(Abomonation, Clone, Debug)]
pub struct ArrayContainer1<A> {
    shape: usize,
    raw: Vec<A>,
}

impl<A: Clone> From<Array1<A>> for ArrayContainer1<A> {
    fn from(arr: Array1<A>) -> Self {
        let shape = arr.shape()[0];
        let raw = if arr.is_standard_layout() {
            arr.into_raw_vec()
        }
        else {
            // If the current array is not in standard layout,
            // then the array is converted to the standard layout.
            // as_standard_layout(): if self is in standard layout, 
            // a COW view of the data is returned without cloning.
            // Otherwise, the data is cloned, and the returned array owns the cloned data.
            // match self_.data {
            //     CowRepr::View(_) => self_.to_owned(),
            //     CowRepr::Owned(data) => unsafe {
            //         // safe because the data is equivalent so ptr, dims remain valid
            //         ArrayBase::from_data_ptr(data, self_.ptr)
            //             .with_strides_dim(self_.strides, self_.dim)
            //     },
            // }

            // into_owned() will not clone the data
            let row_major_arr = arr.as_standard_layout().into_owned();
            row_major_arr.into_raw_vec()
        };
        ArrayContainer1 {
            shape,
            raw
        }
    }
}

impl<A> From<ArrayContainer1<A>> for Array1<A> {
    fn from(container: ArrayContainer1<A>) -> Self {
        let raw_vec = container.raw;
        let shape = container.shape;
        let arr = Array::from_shape_vec([shape], raw_vec).unwrap();
        arr
    }
}

#[derive(Abomonation, Clone, Debug)]
pub struct ArrayContainer2<A> {
    shape: (usize, usize),
    raw: Vec<A>,
}

impl<A: Clone> From<Array2<A>> for ArrayContainer2<A> {
    fn from(arr: Array2<A>) -> Self {
        let shape = (arr.shape()[0], arr.shape()[1]);
        let raw = if arr.is_standard_layout() {
            arr.into_raw_vec()
        }
        else {
            let row_major_arr = arr.as_standard_layout().into_owned();
            row_major_arr.into_raw_vec()
        };
        ArrayContainer2 {
            shape,
            raw
        }
    }
}

impl<A> From<ArrayContainer2<A>> for Array2<A> {
    fn from(container: ArrayContainer2<A>) -> Self {
        let raw_vec = container.raw;
        let shape = container.shape;
        let arr = Array::from_shape_vec(shape, raw_vec).unwrap();
        arr
    }
}

#[derive(Abomonation, Clone, Debug)]
pub struct ArrayContainer3<A> {
    shape: (usize, usize, usize),
    raw: Vec<A>,
}

impl<A: Clone> From<Array3<A>> for ArrayContainer3<A> {
    fn from(arr: Array3<A>) -> Self {
        let shape = arr.shape();
        let shape = (shape[0], shape[1], shape[2]);
        let raw = if arr.is_standard_layout() {
            arr.into_raw_vec()
        }
        else {
            let row_major_arr = arr.as_standard_layout().into_owned();
            row_major_arr.into_raw_vec()
        };
        ArrayContainer3 {
            shape,
            raw
        }
    }
}

impl<A> From<ArrayContainer3<A>> for Array3<A> {
    fn from(container: ArrayContainer3<A>) -> Self {
        let raw_vec = container.raw;
        let shape = container.shape;
        let arr = Array::from_shape_vec(shape, raw_vec).unwrap();
        arr
    }
}

#[derive(Abomonation, Clone, Debug)]
pub struct ArrayContainerD<A> {
    shape: Vec<usize>,
    raw: Vec<A>,
}

impl<A: Clone> From<ArrayD<A>> for ArrayContainerD<A> {
    fn from(arr: ArrayD<A>) -> Self {
        let shape = arr.shape().to_vec();
        let raw = if arr.is_standard_layout() {
            arr.into_raw_vec()
        }
        else {
            let row_major_arr = arr.as_standard_layout().into_owned();
            row_major_arr.into_raw_vec()
        };
        ArrayContainerD {
            shape,
            raw
        }
    }
}

impl<A> From<ArrayContainerD<A>> for ArrayD<A> {
    fn from(container: ArrayContainerD<A>) -> Self {
        let raw_vec = container.raw;
        let shape = container.shape;
        let arr = Array::from_shape_vec(shape, raw_vec).unwrap();
        arr
    }
}