#[derive(Debug)]
pub enum Buf<'a> {
    Shared(&'a mut [u8]),
    Owned(Vec<u8>),
}

impl<'a, 'b> Buf<'a> {
    pub fn len(&self) -> usize {
        match *self {
            Buf::Shared(ref val) => val.len(),
            Buf::Owned(ref val) => val.len(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match *self {
            Buf::Shared(ref val) => val.to_vec(),
            Buf::Owned(ref val) => val.clone(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match *self {
            Buf::Shared(ref val) => &val,
            Buf::Owned(ref val) => &&val,
        }
    }
}
