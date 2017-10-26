use super::buf::Buf;

use super::message::Message;

use super::tree::WriteTree;

use std::slice::from_raw_parts_mut;

pub struct WriteLeaf<'a> {
    id: u64,
    epoch: u64,
    size: usize,
    #[allow(dead_code)]
    data: Buf<'a>,
    keys: Vec<Buf<'a>>,
    vals: Vec<Buf<'a>>,
}

impl<'a,'b> WriteLeaf<'a> {

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
        let split = self.size / 2;
        let mut total = 0 as usize;
        for i in split..self.size {
            total += self.keys[i].bytes().len();
            total += self.vals[i].bytes().len();
        }

        let mut sib_data = Vec::with_capacity(total);
        let mut sib_keys = Vec::with_capacity(self.size - split);
        let mut sib_vals = Vec::with_capacity(self.size - split);
        let sib_ptr = sib_data.as_mut_ptr();
        let mut offset = 0 as isize;

        for i in split..self.size {
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

        for i in split..self.size {
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

        self.size = split;
        self.keys.truncate(split);
        self.vals.truncate(split);

        WriteLeaf {
            id: tree.next_id(),
            epoch: tree.epoch,
            size: self.size - split,
            data: Buf::Unique(sib_data),
            keys: sib_keys,
            vals: sib_vals,
        }
    }

    pub fn upsert(&mut self, tree : &mut WriteTree, msg : &Message) -> Option<WriteLeaf<'b>> {
        let loc = self.keys.binary_search_by_key(&msg.key, |buf| buf.bytes());
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
        if self.size < (tree.max_pivots + tree.max_buffer) {
            None
        } else {
            Some(self.split(tree))
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use message::Operation;

    #[test]
    fn get_writeleaf() {
        let input = WriteLeaf {
            id: 0,
            epoch: 0,
            size: 0,
            data: Buf::Unique(vec![]),
            keys: vec![],
            vals: vec![],
        };
        assert_eq!(input.get(b"hello"), None);
        let input = WriteLeaf {
            id: 0,
            epoch: 0,
            size: 0,
            data: Buf::Unique(vec![]),
            keys: vec![Buf::Unique(b"hello".to_vec())],
            vals: vec![Buf::Unique(b"world".to_vec())],
        };
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
    }

    #[test]
    fn upsert_writeleaf() {
        let mut tree = WriteTree::new(4, 16);
        let mut input = WriteLeaf{
            id: 0,
            epoch: 0,
            size: 0,
            data: Buf::Unique(vec![]),
            keys: vec![],
            vals: vec![],
        };
        let msg = Message{
            op: Operation::Assign,
            key: b"hello",
            data: b"world",
        };
        input.upsert(&mut tree, &msg);
        assert_eq!(input.get(b"hello"), Some(&b"world"[..]));
        let msg = Message{
            op: Operation::Assign,
            key: b"hello",
            data: b"hello",
        };
        input.upsert(&mut tree, &msg);
        assert_eq!(input.get(b"hello"), Some(&b"hello"[..]));
        let msg = Message{
            op: Operation::Assign,
            key: b"hello",
            data: b"worlds",
        };
        input.upsert(&mut tree, &msg);
        assert_eq!(input.get(b"hello"), Some(&b"worlds"[..]));
    }

    #[test]
    fn split_writeleaf() {
        let mut tree = WriteTree::new(1, 1);
        let mut input = WriteLeaf{
            id: 0,
            epoch: 0,
            size: 0,
            data: Buf::Unique(vec![]),
            keys: vec![],
            vals: vec![],
        };
        {
            let msg = Message{
                op: Operation::Assign,
                key: b"foo",
                data: b"abc",
            };
            let sibling = input.upsert(&mut tree, &msg);
            assert!(sibling.is_none());
        }
        {
            let msg = Message{
                op: Operation::Assign,
                key: b"bar",
                data: b"xyz",
            };
            let sibling = input.upsert(&mut tree, &msg);
            assert!(sibling.is_some());
            let sibling = sibling.unwrap();
            assert_eq!(sibling.get(b"foo"), Some(&b"abc"[..]));
            assert_eq!(sibling.get(b"bar"), None);
            assert_eq!(input.get(b"foo"), None);
            assert_eq!(input.get(b"bar"), Some(&b"xyz"[..]));
        }
    }
}