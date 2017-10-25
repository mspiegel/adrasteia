use super::buf::Buf;

use super::message::Operation;
use super::message::Message;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use std::mem::size_of;

use std::io::Cursor;
use std::io::Result;
use std::io::Write;

use std::slice::from_raw_parts;

pub struct WriteLeaf<'a> {
    id: u64,
    epoch: u64,
    size: usize,
    #[allow(dead_code)]
    data: &'a [u8],
    keys: Vec<Buf<'a>>,
    vals: Vec<Buf<'a>>,
}

impl<'a> WriteLeaf<'a> {

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let loc = self.keys.binary_search_by_key(&key, |buf| {
            match *buf {
                Buf::Shared(ref val) => &val,
                Buf::Unique(ref val) => &&val,
            }
        });
        match loc {
            Ok(pos) => {
                let buf = self.vals.get(pos).unwrap();
                match *buf {
                    Buf::Shared(ref val) => Some(&val),
                    Buf::Unique(ref val) => Some(&&val),
                }
            }
            Err(_) => None,
        }
    }

    pub fn upsert(&mut self, msg : &Message) {
        let key = msg.key;
        let loc = self.keys.binary_search_by_key(&msg.key, |buf| {
            match *buf {
                Buf::Shared(ref val) => &val,
                Buf::Unique(ref val) => &&val,
            }
        });
        match loc {
            Ok(pos) => {
                msg.apply(self.vals.get_mut(pos).unwrap());
            },
            Err(pos) => {
                let key = msg.key.to_vec();
                let val = msg.create().to_vec();
                self.keys.insert(pos, Buf::Unique(key));
                self.vals.insert(pos, Buf::Unique(val));
                self.size += 1;
            },
        };
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_readleaf() {
        let empty: &[u8] = &[];
        let input = WriteLeaf {
            id: 0,
            epoch: 0,
            size: 0,
            data: empty,
            keys: vec![],
            vals: vec![],
        };
        assert_eq!(input.get(b"hello"), None);
        let input = WriteLeaf {
            id: 0,
            epoch: 0,
            size: 0,
            data: empty,
            keys: vec![Buf::Unique(b"hello".to_vec())],
            vals: vec![Buf::Unique(b"world".to_vec())],
        };
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
    }

    #[test]
    fn upsert_readleaf() {
        let empty: &[u8] = &[];
        let mut input = WriteLeaf{
            id: 0,
            epoch: 0,
            size: 0,
            data: empty,
            keys: vec![],
            vals: vec![],
        };
        let msg = Message{
            op: Operation::Assign,
            key: b"hello",
            data: b"world",
        };
        input.upsert(&msg);
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
    }
}