use super::buf::Buf;
use super::message::Message;
use super::node::NewSibling;
use super::node::Body;
use super::tree::Tree;

use std::io;
use std::io::Cursor;
use std::io::Write;
use std::mem::size_of;
use std::slice::from_raw_parts_mut;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct Leaf<'a> {
    #[allow(dead_code)]
    pub data: Vec<u8>,
    pub keys: Vec<Buf<'a>>,
    pub vals: Vec<Buf<'a>>,
}

impl<'a> Leaf<'a> {
    pub fn serialize(&self, wtr: &mut Write) -> io::Result<()> {
        let size = self.keys.len();
        wtr.write_u64::<LittleEndian>(size as u64)?;
        for key in &self.keys {
            let len = key.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }
        for val in &self.vals {
            let len = val.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }
        for key in &self.keys {
            wtr.write_all(key.bytes())?;
        }
        for val in &self.vals {
            wtr.write_all(val.bytes())?;
        }
        Ok(())
    }

    pub fn deserialize(mut input: Vec<u8>) -> io::Result<Leaf<'a>> {
        let input_ptr = input.as_mut_ptr();
        let mut rdr = Cursor::new(input);
        let size = rdr.read_u64::<LittleEndian>()? as usize;

        let mut keys = Vec::with_capacity(size);
        let mut vals = Vec::with_capacity(size);

        let mut offset = (size_of::<u64>() + 2 * size * size_of::<u64>()) as isize;

        for _ in 0..size {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                keys.push(Buf::Shared(
                    from_raw_parts_mut(input_ptr.offset(offset), len),
                ));
            }
            offset += len as isize;
        }

        for _ in 0..size {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                vals.push(Buf::Shared(
                    from_raw_parts_mut(input_ptr.offset(offset), len),
                ));
            }
            offset += len as isize;
        }

        Ok(Leaf {
            data: rdr.into_inner(),
            keys: keys,
            vals: vals,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let loc = self.keys.binary_search_by_key(&key, |buf| buf.bytes());
        match loc {
            Ok(pos) => {
                let buf = &self.vals[pos];
                Some(buf.bytes())
            }
            Err(_) => None,
        }
    }

    fn split(&mut self) -> NewSibling<'a> {
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

        let key = sib_keys[0].bytes().to_vec();
        let body = Leaf {
            data: sib_data,
            keys: sib_keys,
            vals: sib_vals,
        };
        NewSibling {
            key: key,
            body: Body::Leaf(body),
        }
    }

    pub fn upsert(&mut self, msg: Message) {
        let loc = self.keys.binary_search_by_key(
            &msg.key.as_slice(),
            |buf| buf.bytes(),
        );
        match loc {
            Ok(pos) => {
                msg.apply(&mut self.vals[pos]);
            }
            Err(pos) => {
                let (key, val) = msg.create();
                self.keys.insert(pos, Buf::Owned(key));
                self.vals.insert(pos, Buf::Owned(val));
            }
        };
    }

    pub fn upsert_msg(&mut self, tree: &mut Tree, msg: Message) -> Option<NewSibling<'a>> {
        self.upsert(msg);
        if self.keys.len() < (tree.max_pivots + tree.max_buffer) {
            None
        } else {
            Some(self.split())
        }
    }

    pub fn upsert_msgs(&mut self, tree: &mut Tree, msgs: Vec<Message>) -> Option<NewSibling<'a>> {
        for msg in msgs {
            self.upsert(msg);
        }
        if self.keys.len() < (tree.max_pivots + tree.max_buffer) {
            None
        } else {
            Some(self.split())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mode::Mode;
    use operation::Operation;

    #[test]
    fn get_leaf() {
        let input = Leaf {
            data: vec![],
            keys: vec![],
            vals: vec![],
        };
        assert_eq!(input.get(b"hello"), None);
        let input = Leaf {
            data: vec![],
            keys: vec![Buf::Owned(b"hello".to_vec())],
            vals: vec![Buf::Owned(b"world".to_vec())],
        };
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
    }

    #[test]
    fn upsert_leaf() {
        let mut tree = Tree::new(4, 16, Mode::Test);
        let mut input = Leaf {
            data: vec![],
            keys: vec![],
            vals: vec![],
        };
        let msg = Message {
            op: Operation::Assign,
            key: b"hello".to_vec(),
            data: b"world".to_vec(),
        };
        input.upsert_msg(&mut tree, msg);
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
        let msg = Message {
            op: Operation::Assign,
            key: b"hello".to_vec(),
            data: b"hello".to_vec(),
        };
        input.upsert_msg(&mut tree, msg);
        assert_eq!(input.get(b"hello"), Some(&b"hello"[..]));
        let msg = Message {
            op: Operation::Assign,
            key: b"hello".to_vec(),
            data: b"worlds".to_vec(),
        };
        input.upsert_msg(&mut tree, msg);
        assert_eq!(input.get(b"hello"), Some(&b"worlds"[..]));
    }

    #[test]
    fn roundtrip_empty_leaf() {
        let input = Leaf {
            data: vec![],
            keys: vec![],
            vals: vec![],
        };
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        let output = Leaf::deserialize(wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(0, output.keys.len());
        assert_eq!(0, output.vals.len());
    }

    #[test]
    fn roundtrip_nonempty_leaf() {
        let input = Leaf {
            data: vec![],
            keys: vec![Buf::Owned(b"hello".to_vec())],
            vals: vec![Buf::Owned(b"world".to_vec())],
        };
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        assert_eq!(
            3 * size_of::<u64>() + "hello".len() + "world".len(),
            wtr.len()
        );
        let output = Leaf::deserialize(wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(b"hello", output.keys[0].bytes());
        assert_eq!(b"world", output.vals[0].bytes());
    }

    #[test]
    fn split_leaf() {
        let mut tree = Tree::new(1, 1, Mode::Test);
        let mut input = Leaf {
            data: vec![],
            keys: vec![],
            vals: vec![],
        };
        {
            let msg = Message {
                op: Operation::Assign,
                key: b"foo".to_vec(),
                data: b"abc".to_vec(),
            };
            let sibling = input.upsert_msg(&mut tree, msg);
            assert!(sibling.is_none());
        }
        {
            let msg = Message {
                op: Operation::Assign,
                key: b"bar".to_vec(),
                data: b"xyz".to_vec(),
            };
            let sibling = input.upsert_msg(&mut tree, msg);
            assert!(sibling.is_some());
            let sibling = match sibling.unwrap().body {
                Body::Leaf(node) => node,
                Body::Internal(_) => panic!("expected leaf node"),
            };
            assert_eq!(sibling.get(b"foo"), Some(&b"abc"[..]));
            assert_eq!(sibling.get(b"bar"), None);
            assert_eq!(input.get(b"foo"), None);
            assert_eq!(input.get(b"bar"), Some(&b"xyz"[..]));
        }
    }
}
