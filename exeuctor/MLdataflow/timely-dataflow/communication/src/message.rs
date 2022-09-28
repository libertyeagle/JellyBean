// READ
//! Types wrapping typed data.

use std::sync::Arc;

use abomonation;

use bytes::arc::Bytes;

use crate::Data;

/// Either an immutable or mutable reference.
pub enum RefOrMut<'a, T> where T: 'a {
    /// An immutable reference.
    Ref(&'a T),
    /// A mutable reference.
    Mut(&'a mut T),
}

impl<'a, T: 'a> ::std::ops::Deref for RefOrMut<'a, T> {
    type Target = T;
	// return 
    fn deref(&self) -> &Self::Target {
        match self {
			// reference's type is && T' or &&mut T
			// Rust auto deref returns &'a T
            RefOrMut::Ref(reference) => reference,
            RefOrMut::Mut(reference) => reference,
        }
    }
}

impl<'a, T: Clone+'a> RefOrMut<'a, T> {
    /// Extracts the contents of `self`, either by cloning or swapping.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
    pub fn swap<'b>(self, element: &'b mut T) {
		// we don't have to worry about lifetime when swapping values at two mutable locations
		// the lifetime of `reference` and `element` (and their original bindings) does not change after the swapping.
		// element will get the contents in `self`
        match self {
            RefOrMut::Ref(reference) => element.clone_from(reference),
            RefOrMut::Mut(reference) => ::std::mem::swap(reference, element),
        };
    }
    /// Extracts the contents of `self`, either by cloning or swapping.
    ///
    /// This consumes `self` because its contents are now in an unknown state.
	// takes ownership of element
    pub fn replace(self, mut element: T) -> T {
        self.swap(&mut element);
        element
    }
}

/// A wrapped message which may be either typed or binary data.
pub struct Message<T> {
    payload: MessageContents<T>,
}

/// Possible returned representations from a channel.
enum MessageContents<T> {
    /// Binary representation. Only available as a reference.
    #[allow(dead_code)]
    Binary(abomonation::abomonated::Abomonated<T, Bytes>),
    /// Rust typed instance. Available for ownership.
    Owned(T),
    /// Atomic reference counted. Only available as a reference.
    Arc(Arc<T>),
}

impl<T> Message<T> {
    /// Wrap a typed item as a message.
    pub fn from_typed(typed: T) -> Self {
        // take ownership
        Message { payload: MessageContents::Owned(typed) }
    }
    /// Wrap a shared typed item as a message.
    pub fn from_arc(typed: Arc<T>) -> Self {
        // take
        Message { payload: MessageContents::Arc(typed) }
    }
    /// Destructures and returns any typed data.
    pub fn if_typed(self) -> Option<T> {
        match self.payload {
            MessageContents::Binary(_) => None,
            MessageContents::Owned(typed) => Some(typed),
            MessageContents::Arc(_) => None,
        }
    }
    /// Returns a mutable reference, if typed.
    pub fn if_mut(&mut self) -> Option<&mut T> {
        // &mut self.payload is &mut MessageContents<T>
        // we will get a reference to the content holding in the enum type in pattern matching
        match &mut self.payload {
            MessageContents::Binary(_) => None,
            MessageContents::Owned(typed) => Some(typed),
            MessageContents::Arc(_) => None,
        }
    }
    /// Returns an immutable or mutable typed reference.
    ///
    /// This method returns a mutable reference if the underlying data are typed Rust
    /// instances, which admit mutation, and it returns an immutable reference if the
    /// data are serialized binary data.
    pub fn as_ref_or_mut(&mut self) -> RefOrMut<T> {
        match &mut self.payload {
            // returns immutable reference
            MessageContents::Binary(bytes) => { RefOrMut::Ref(bytes) },
            // returns mutable reference
            MessageContents::Owned(typed) => { RefOrMut::Mut(typed) },
            // returns immutable reference, &mut Arc<T> converted to & T
            // type coercions
            // type coersion is allowed between the following types:
            // T to U if T is a subtype of U (reflexive case)
            // T_1 to T_3 where T_1 coerces to T_2 and T_2 coerces to T_3 (transitive case)
            // &mut T to &T
            // *mut T to *const T
            // &T to *const T
            // &mut T to *mut T
            // &T or &mut T to &U if T implements Deref<Target = U>
            // &mut T to &mut U if T implements DerefMut<Target = U>
            // TyCtor(T) to TyCtor(U), where TyCtor(T) is one of: &T, & mutT, *const T, *mut T, Box<T>
            // Non capturing closures to fn pointers
            // ! to any T
            
            // in this case, &mut Arc<T> -> &T
            MessageContents::Arc(typed) => { RefOrMut::Ref(typed) },
        }
    }
}

// These methods require `T` to implement `Abomonation`, for serialization functionality.
#[cfg(not(feature = "bincode"))]
impl<T: Data> Message<T> {
    /// Wrap bytes as a message.
    ///
    /// # Safety
    ///
    /// This method is unsafe, in that `Abomonated::new()` is unsafe: it presumes that
    /// the binary data can be safely decoded, which is unsafe for e.g. UTF8 data and
    /// enumerations (perhaps among many other types).
    pub unsafe fn from_bytes(bytes: Bytes) -> Self {
        // Abomonated owns the u8 bytes received
    
        // only expose immutable reference of the decoded data to the outside
        // in the new step, the data is already decoded
        // now from bytes, we can recover the correct instance of T
        // after decode, all structures owning data have pointers point to the buffer in `bytes`
        let abomonated = abomonation::abomonated::Abomonated::new(bytes).expect("Abomonated::new() failed.");
        Message { payload: MessageContents::Binary(abomonated) }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        // &self.payload is &MessageContents<T>
        // we will automatically get a reference in the following pattern matching
        match &self.payload {
            MessageContents::Binary(bytes) => { bytes.as_bytes().len() },
            MessageContents::Owned(typed) => { abomonation::measure(typed) },
            MessageContents::Arc(typed) =>{ abomonation::measure::<T>(&**typed) } ,
        }
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match &self.payload {
            MessageContents::Binary(bytes) => {
                writer.write_all(bytes.as_bytes()).expect("Message::into_bytes(): write_all failed.");
            },
            MessageContents::Owned(typed) => {
                unsafe { abomonation::encode(typed, writer).expect("Message::into_bytes(): Abomonation::encode failed"); }
            },
            MessageContents::Arc(typed) => {
                unsafe { abomonation::encode(&**typed, writer).expect("Message::into_bytes(): Abomonation::encode failed"); }
            },
        }
    }
}

#[cfg(feature = "bincode")]
impl<T: Data> Message<T> {
    /// Wrap bytes as a message.
    pub fn from_bytes(bytes: Bytes) -> Self {
        let typed = ::bincode::deserialize(&bytes[..]).expect("bincode::deserialize() failed");
        Message { payload: MessageContents::Owned(typed) }
    }

    /// The number of bytes required to serialize the data.
    pub fn length_in_bytes(&self) -> usize {
        match &self.payload {
            MessageContents::Binary(bytes) => { bytes.as_bytes().len() },
            MessageContents::Owned(typed) => {
                ::bincode::serialized_size(&typed).expect("bincode::serialized_size() failed") as usize
            },
            MessageContents::Arc(typed) => {
                ::bincode::serialized_size(&**typed).expect("bincode::serialized_size() failed") as usize
            },
        }
    }

    /// Writes the binary representation into `writer`.
    pub fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        match &self.payload {
            MessageContents::Binary(bytes) => {
                writer.write_all(bytes.as_bytes()).expect("Message::into_bytes(): write_all failed.");
            },
            MessageContents::Owned(typed) => {
                ::bincode::serialize_into(writer, &typed).expect("bincode::serialize_into() failed");
            },
            MessageContents::Arc(typed) => {
                ::bincode::serialize_into(writer, &**typed).expect("bincode::serialize_into() failed");
            },
        }
    }
}

impl<T> ::std::ops::Deref for Message<T> {
    type Target = T;
    // deref returns &T
    fn deref(&self) -> &Self::Target {
        // TODO: In principle we have already decoded, but let's go again
        match &self.payload {
            MessageContents::Binary(bytes) => { bytes },
            MessageContents::Owned(typed) => { typed },
            MessageContents::Arc(typed) => { typed },
        }
    }
}

impl<T: Clone> Message<T> {
    /// Produces a typed instance of the wrapped element.
    // consumes self
    pub fn into_typed(self) -> T {
        match self.payload {
            // let name = "Alice".to_owned();
            // let name_clone = Clone::clone(&name);
            MessageContents::Binary(bytes) => bytes.clone(),
            MessageContents::Owned(instance) => instance,
            // TODO: Could attempt `Arc::try_unwrap()` here.
            MessageContents::Arc(instance) => (*instance).clone(),
        }
    }
    /// Ensures the message is typed data and returns a mutable reference to it.
    pub fn as_mut(&mut self) -> &mut T {
        // if the message is borrowed (Abomonated or Arc), then we create a clone
        // otherwise, we directly return a mutable reference to the carried payload.
        let cloned: Option<T> = match &self.payload {
            // rust's auto refd & deref
            // in the first dereference step, we set U = T
            // 1. if there is a method whose receiver type matches U (T), then we use this method directly (by value)
            // 2. otherweise, we add one autoref (& or &mut) to U (T)
            // we continue to the next dereference step (by setting U = *T) to see whether there is a match
            // https://stackoverflow.com/questions/28519997/what-are-rusts-exact-auto-dereferencing-rules
            // there can be at most one valid method for sub-step 1 and one valid method for sub-step 2 in each dereference step
            // if both 1 and 2 have valid methods, then Rust will take 1's as priority.
            // at most one auto-reference will be added

            // *bytes: Abomonated<T, Bytes> -> get deref to T, then add an autoref to call clone (which takes &T) 
            // **bytes = *((*bytes).deref())
            MessageContents::Binary(bytes) => Some((*bytes).clone()),
            MessageContents::Owned(_) => None,
            // TODO: Could attempt `Arc::try_unwrap()` here/
            // **typed is T, an autoref is added
            MessageContents::Arc(typed) => Some((**typed).clone()),
        };

        if let Some(cloned) = cloned {
            self.payload = MessageContents::Owned(cloned);
        }

        if let MessageContents::Owned(typed) = &mut self.payload {
            typed
        }
        else {
            unreachable!()
        }
    }
}