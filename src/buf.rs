pub enum Buf<'a> {
    Shared(&'a mut [u8]),
    Unique(Vec<u8>),
}