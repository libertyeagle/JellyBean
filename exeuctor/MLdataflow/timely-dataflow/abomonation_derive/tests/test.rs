#![deny(unused_mut)]

#[macro_use]
extern crate abomonation_derive;

#[cfg(test)]
mod tests {
    use abomonation::*;

    #[derive(Eq, PartialEq, Abomonation)]
    pub struct Struct {
        a: String,
        b: u64,
        c: Vec<u8>,
    }

    #[test]
    fn test_struct() {
        // create some test data out of abomonation-approved types
        let record = Struct { a: "test".to_owned(), b: 0, c: vec![0, 1, 2] };

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<Struct>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }

    #[derive(Eq, PartialEq, Abomonation)]
    pub struct EmptyStruct;

    #[test]
    fn test_empty_struct() {
        // create some test data out of abomonation-approved types
        let record = EmptyStruct;

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<EmptyStruct>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }

    #[derive(Eq, PartialEq, Abomonation)]
    pub struct TupleStruct(String, u64, Vec<u8>);

    #[test]
    fn test_tuple_struct() {
        // create some test data out of abomonation-approved types
        let record = TupleStruct("test".to_owned(), 0, vec![0, 1, 2]);

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<TupleStruct>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }

    #[derive(Eq, PartialEq, Abomonation)]
    pub struct GenericStruct<T, U>(T, u64, U);

    #[test]
    fn test_generic_struct() {
        // create some test data out of abomonation-approved types
        let record = GenericStruct("test".to_owned(), 0, vec![0, 1, 2]);

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<GenericStruct<String, Vec<u8>>>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }

    #[allow(dead_code)]
    #[derive(Eq, PartialEq, Abomonation)]
    pub enum BasicEnum {
        Apples,
        Pears,
        Chicken
    }

    #[test]
    fn test_basic_enum() {
        // create some test data out of abomonation-approved types
        let record = BasicEnum::Apples;

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<BasicEnum>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }

    #[allow(dead_code)]
    #[derive(Eq, PartialEq, Abomonation)]
    pub enum DataEnum {
        A(String, u64, Vec<u8>),
        B,
        C(String, String, String)
    }

    #[test]
    fn test_data_enum() {
        // create some test data out of abomonation-approved types
        let record = DataEnum::A("test".to_owned(), 0, vec![0, 1, 2]);

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<DataEnum>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }

    pub trait SomeTrait {}

    #[allow(dead_code)]
    #[derive(Abomonation)]
    pub enum GenericEnumWithBounds<T: SomeTrait> {
        A(T),
        B
    }

    #[test]
    fn test_ignore_attribute() {

        #[derive(Abomonation)]
        pub struct StructWithPhantomMarker<T> {
            data: usize,
            // test fails to built without this attribute.
            #[unsafe_abomonate_ignore]
            _phantom: ::std::marker::PhantomData<T>,
        }

        struct NonAbomonable { };

        // create some test data with a phantom non-abomonable type.
        let record = StructWithPhantomMarker {
            data: 0,
            _phantom: ::std::marker::PhantomData::<NonAbomonable>,
        };

        // encode vector into a Vec<u8>
        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));
    }

    #[derive(Abomonation, Eq, PartialEq)]
    pub struct StructUsingCratePath {
        pub header: crate::tests::EmptyStruct,
    }

    #[test]
    fn test_path_beginning_with_crate() {
        let record = StructUsingCratePath { header: EmptyStruct {} };

        let mut bytes = Vec::new();
        unsafe { encode(&record, &mut bytes).unwrap(); }

        assert_eq!(bytes.len(), measure(&record));

        // decode from binary data
        if let Some((result, rest)) = unsafe { decode::<StructUsingCratePath>(&mut bytes) } {
            assert!(result == &record);
            assert!(rest.len() == 0);
        }
    }
}
