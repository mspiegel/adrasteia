use super::buf::Buf;
use super::ownmessage::OwnedMessage;
use super::tree::WriteTree;

use std::io::Cursor;
use std::io::Result;
use std::io::Write;
use std::mem::size_of;
use std::slice::from_raw_parts_mut;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct WriteLeaf<'a> {
    id: u64,
    epoch: u64,
    #[allow(dead_code)]
    data: Buf<'a>,
    keys: Vec<Buf<'a>>,
    vals: Vec<Buf<'a>>,
}

impl<'a,'b> WriteLeaf<'a> {

    pub fn serialize(&self, wtr: &mut Write) -> Result<usize> {
        let size = self.keys.len();
        let mut total = 3 * size_of::<u64>() + 2 * size * size_of::<u64>();
        wtr.write_u64::<LittleEndian>(self.id)?;
        wtr.write_u64::<LittleEndian>(self.epoch)?;
        wtr.write_u64::<LittleEndian>(size as u64)?;
        for key in &self.keys {
            let len = key.bytes().len();
            total += len;
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }
        for val in &self.vals {
            let len = val.bytes().len();
            total += len;
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }
        for key in &self.keys {
            wtr.write_all(key.bytes())?;
        }
        for val in &self.vals {
            wtr.write_all(val.bytes())?;
        }
        Ok(total)
    }

    pub fn deserialize(input: &mut [u8]) -> Result<WriteLeaf> {
        let input_ptr = input.as_mut_ptr();
        let mut rdr = Cursor::new(input);
        let id = rdr.read_u64::<LittleEndian>()?;
        let epoch = rdr.read_u64::<LittleEndian>()?;
        let size = rdr.read_u64::<LittleEndian>()? as usize;

        let mut keys = Vec::with_capacity(size);
        let mut vals = Vec::with_capacity(size);

        let mut offset = (3 * size_of::<u64>() + 2 * size * size_of::<u64>()) as isize;

        for _ in 0..size {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                keys.push(Buf::Shared(from_raw_parts_mut(input_ptr.offset(offset), len)));
            }
            offset += len as isize;
        }

        for _ in 0..size {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                vals.push(Buf::Shared(from_raw_parts_mut(input_ptr.offset(offset), len)));
            }
            offset += len as isize;
        }

        Ok(WriteLeaf {
            id: id,
            epoch: epoch,
            data: Buf::Shared(rdr.into_inner()),
            keys: keys,
            vals: vals,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let loc = self.keys.binary_search_by_key(&key, |buf| buf.bytes());
        match loc {
            Ok(pos) => {
                let buf = self.vals.get(pos).unwrap();
                Some(buf.bytes())
            }
            Err(_) => None,
        }
    }

    fn split(&mut self, tree : &mut WriteTree) -> WriteLeaf<'b> {
        let size = self.keys.len();
        let split = size / 2;
        let mut total = 0 as usize;
        for i in split..size {
            total += self.keys[i].bytes().len();
            total += self.vals[i].bytes().len();
        }

        let mut sib_data = Vec::with_capacity(total);
        let mut sib_keys = Vec::with_capacity(size - split);
        let mut sib_vals = Vec::with_capacity(size - split);
        let sib_ptr = sib_data.as_mut_ptr();
        let mut offset = 0 as isize;

        for i in split..size {
            let buf = self.keys[i].bytes();
            let len = buf.len();
            sib_data.extend_from_slice(buf);
            unsafe {
                let data = from_raw_parts_mut(sib_ptr.offset(offset), len);
                let key = Buf::Shared(data);
                sib_keys.push(key);
            }
            offset += len as isize;
        }

        for i in split..size {
            let buf = self.vals[i].bytes();
            let len = buf.len();
            sib_data.extend_from_slice(buf);
            unsafe {
                let data = from_raw_parts_mut(sib_ptr.offset(offset), len);
                let val = Buf::Shared(data);
                sib_vals.push(val);
            }
            offset += len as isize;
        }

        self.keys.truncate(split);
        self.vals.truncate(split);

        WriteLeaf {
            id: tree.next_id(),
            epoch: tree.epoch,
            data: Buf::Owned(sib_data),
            keys: sib_keys,
            vals: sib_vals,
        }
    }

    pub fn upsert_owned(&mut self, tree : &mut WriteTree, msg : OwnedMessage) -> Option<WriteLeaf<'b>> {
        let loc = self.keys.binary_search_by_key(&msg.key.as_slice(), |buf| buf.bytes());
        match loc {
            Ok(pos) => {
                msg.apply(self.vals.get_mut(pos).unwrap());
            },
            Err(pos) => {
                let (key, val) = msg.create();
                self.keys.insert(pos, Buf::Owned(key));
                self.vals.insert(pos, Buf::Owned(val));
            },
        };
        if self.keys.len() < (tree.max_pivots + tree.max_buffer) {
            None
        } else {
            Some(self.split(tree))
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use operation::Operation;

    #[test]
    fn get_writeleaf() {
        let input = WriteLeaf {
            id: 0,
            epoch: 0,
            data: Buf::Owned(vec![]),
            keys: vec![],
            vals: vec![],
        };
        assert_eq!(input.get(b"hello"), None);
        let input = WriteLeaf {
            id: 0,
            epoch: 0,
            data: Buf::Owned(vec![]),
            keys: vec![Buf::Owned(b"hello".to_vec())],
            vals: vec![Buf::Owned(b"world".to_vec())],
        };
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
    }

    #[test]
    fn upsert_writeleaf() {
        let mut tree = WriteTree::new(4, 16);
        let mut input = WriteLeaf{
            id: 0,
            epoch: 0,
            data: Buf::Owned(vec![]),
            keys: vec![],
            vals: vec![],
        };
        let msg = OwnedMessage{
            op: Operation::Assign,
            key: b"hello".to_vec(),
            data: b"world".to_vec(),
        };
        input.upsert_owned(&mut tree, msg);
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
        let msg = OwnedMessage{
            op: Operation::Assign,
            key: b"hello".to_vec(),
            data: b"hello".to_vec(),
        };
        input.upsert_owned(&mut tree, msg);
        assert_eq!(input.get(b"hello"), Some(&b"hello"[..]));
        let msg = OwnedMessage{
            op: Operation::Assign,
            key: b"hello".to_vec(),
            data: b"worlds".to_vec(),
        };
        input.upsert_owned(&mut tree, msg);
        assert_eq!(input.get(b"hello"), Some(&b"worlds"[..]));
    }

    #[test]
    fn split_writeleaf() {
        let mut tree = WriteTree::new(1, 1);
        let mut input = WriteLeaf{
            id: 0,
            epoch: 0,
            data: Buf::Owned(vec![]),
            keys: vec![],
            vals: vec![],
        };
        {
            let msg = OwnedMessage{
                op: Operation::Assign,
                key: b"foo".to_vec(),
                data: b"abc".to_vec(),
            };
            let sibling = input.upsert_owned(&mut tree, msg);
            assert!(sibling.is_none());
        }
        {
            let msg = OwnedMessage{
                op: Operation::Assign,
                key: b"bar".to_vec(),
                data: b"xyz".to_vec(),
            };
            let sibling = input.upsert_owned(&mut tree, msg);
            assert!(sibling.is_some());
            let sibling = sibling.unwrap();
            assert_eq!(sibling.get(b"foo"), Some(&b"abc"[..]));
            assert_eq!(sibling.get(b"bar"), None);
            assert_eq!(input.get(b"foo"), None);
            assert_eq!(input.get(b"bar"), Some(&b"xyz"[..]));
        }
    }
}