pub enum Buf<'a> {
    Shared(&'a mut [u8]),
    Unique(Vec<u8>),
}

impl<'a> Buf<'a> {
    pub fn bytes(&self) -> &[u8] {
        match *self {
            Buf::Shared(ref val) => &val,
            Buf::Unique(ref val) => &&val,
        }
    }
}