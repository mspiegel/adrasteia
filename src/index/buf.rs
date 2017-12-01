use std::cmp::Ordering;

#[derive(Debug)]
pub enum Buf<'a> {
    Shared(&'a mut [u8]),
    Owned(Vec<u8>),
}

impl<'a> Buf<'a> {
    pub fn len(&self) -> usize {
        match *self {
            Buf::Shared(ref val) => val.len(),
            Buf::Owned(ref val) => val.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            Buf::Shared(ref val) => val.is_empty(),
            Buf::Owned(ref val) => val.is_empty(),
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
            Buf::Shared(ref val) => val,
            Buf::Owned(ref val) => val,
        }
    }
}

impl<'a> Ord for Buf<'a> {
    fn cmp(&self, other: &Buf) -> Ordering {
        self.bytes().cmp(other.bytes())
    }
}

impl<'a> PartialOrd for Buf<'a> {
    fn partial_cmp(&self, other: &Buf) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for Buf<'a> {
    fn eq(&self, other: &Buf) -> bool {
        self.bytes() == other.bytes()
    }
}

impl<'a> Eq for Buf<'a> {}
