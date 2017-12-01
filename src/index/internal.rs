use super::buf::Buf;
use super::message::BufMessage;
use super::message::Message;
use super::node::NewSibling;
use super::node::Body;
use super::operation::Operation;
use super::store::Store;
use super::transaction::Transaction;
use super::tree::Tree;

use std::io;
use std::io::Cursor;
use std::io::Write;
use std::mem::size_of;
use std::slice::from_raw_parts_mut;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct Internal<'a> {
    pub level: u32,
    #[allow(dead_code)]
    pub data: Vec<u8>,
    pub keys: Vec<Buf<'a>>,
    pub buffer: Vec<BufMessage<'a>>,
    pub children: Vec<u64>,
    pub serde: bool,
}

impl<'a> Internal<'a> {
    pub fn serialize(&self, wtr: &mut Write) -> io::Result<()> {
        if self.serde {
            wtr.write_all(&self.data)?;
            return Ok(());
        }
        let key_size = self.keys.len();
        let buf_size = self.buffer.len();
        let child_size = self.children.len();
        wtr.write_u32::<LittleEndian>(self.level)?;
        wtr.write_u64::<LittleEndian>(key_size as u64)?;
        wtr.write_u64::<LittleEndian>(buf_size as u64)?;
        wtr.write_u64::<LittleEndian>(child_size as u64)?;

        for key in &self.keys {
            let len = key.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }

        for child in &self.children {
            wtr.write_u64::<LittleEndian>(*child)?;
        }

        for msg in &self.buffer {
            wtr.write_u32::<LittleEndian>(msg.op.serialize())?;
        }

        for msg in &self.buffer {
            let len = msg.key.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }

        for msg in &self.buffer {
            let len = msg.data.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }

        for key in &self.keys {
            wtr.write_all(key.bytes())?;
        }
        for msg in &self.buffer {
            wtr.write_all(msg.key.bytes())?;
        }
        for msg in &self.buffer {
            wtr.write_all(msg.data.bytes())?;
        }
        Ok(())
    }

    pub fn deserialize(mut input: Vec<u8>) -> io::Result<Internal<'a>> {
        let input_ptr = input.as_mut_ptr();
        let mut rdr = Cursor::new(input);
        let level = rdr.read_u32::<LittleEndian>()?;
        let key_size = rdr.read_u64::<LittleEndian>()? as usize;
        let buf_size = rdr.read_u64::<LittleEndian>()? as usize;
        let child_size = rdr.read_u64::<LittleEndian>()? as usize;

        let mut keys = Vec::with_capacity(key_size);
        let mut buffer = Vec::with_capacity(buf_size);
        let mut children = Vec::with_capacity(child_size);

        let mut offset = (size_of::<u32>() + 3 * size_of::<u64>()) as isize;
        offset += (key_size * size_of::<u64>()) as isize;
        offset += (child_size * size_of::<u64>()) as isize;
        offset += (2 * buf_size * size_of::<u64>()) as isize;
        offset += (buf_size * size_of::<u32>()) as isize;

        for _ in 0..key_size {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                keys.push(Buf::Shared(
                    from_raw_parts_mut(input_ptr.offset(offset), len),
                ));
            }
            offset += len as isize;
        }

        for _ in 0..child_size {
            children.push(rdr.read_u64::<LittleEndian>()?)
        }

        for _ in 0..buf_size {
            let msg = BufMessage {
                op: Operation::deserialize(rdr.read_u32::<LittleEndian>()?),
                key: Buf::Owned(vec![]),
                data: Buf::Owned(vec![]),
            };
            buffer.push(msg);
        }

        for buf in &mut buffer {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                buf.key = Buf::Shared(from_raw_parts_mut(input_ptr.offset(offset), len));
            }
            offset += len as isize;
        }

        for buf in &mut buffer {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                buf.data = Buf::Shared(from_raw_parts_mut(input_ptr.offset(offset), len));
            }
            offset += len as isize;
        }

        Ok(Internal {
            level: level,
            data: rdr.into_inner(),
            keys: keys,
            buffer: buffer,
            children: children,
            serde: false,
        })
    }

    fn upsert(&mut self, msg: Message) {
        self.buffer.push(msg.into_buf_message());
    }

    pub fn max_run(values: &[usize]) -> (usize, usize, usize) {
        // This implementation could be replaced with a prefix scan
        let state = (values[0], 0);
        let count = values.iter().scan(state, |state, &val| {
            let (prev, count) = *state;
            let next = if prev == val { count + 1 } else { 1 };
            *state = (val, next);
            Some(next)
        });
        let (idx, len) = count.enumerate().max_by_key(|x| x.1).unwrap();
        (idx - (len - 1), len, values[idx])
    }

    fn split(&mut self) -> NewSibling<'a> {
        let key_size = self.keys.len();
        let split = key_size / 2;

        let mut right_msgs = vec![];

        // TODO: replace with drain_filter when it lands in stable
        let mut i = 0;
        while i < self.buffer.len() {
            if self.buffer[i].key >= self.keys[split] {
                right_msgs.push(self.buffer.swap_remove(i));
            } else {
                i += 1;
            }
        }

        let mut total = size_of::<u32>();
        total += 3 * size_of::<u64>();
        total += (key_size - split - 1) * size_of::<u64>();
        total += (key_size - split) * size_of::<u64>();
        total += right_msgs.len() * size_of::<u32>();
        total += 2 * right_msgs.len() * size_of::<u64>();

        for i in (split + 1)..key_size {
            total += self.keys[i].bytes().len();
        }

        for msg in &right_msgs {
            total += msg.key.bytes().len();
        }

        for msg in &right_msgs {
            total += msg.data.bytes().len();
        }

        let mut sib_data = Vec::with_capacity(total);
        let sib_ptr = sib_data.as_mut_ptr();

        sib_data.write_u32::<LittleEndian>(self.level).unwrap();
        sib_data
            .write_u64::<LittleEndian>((key_size - split - 1) as u64)
            .unwrap();
        sib_data
            .write_u64::<LittleEndian>((key_size - split) as u64)
            .unwrap();
        sib_data
            .write_u64::<LittleEndian>(right_msgs.len() as u64)
            .unwrap();

        for i in (split + 1)..key_size {
            sib_data
                .write_u64::<LittleEndian>(self.keys[i].bytes().len() as u64)
                .unwrap();
        }

        for i in (split + 1)..(key_size + 1) {
            sib_data
                .write_u64::<LittleEndian>(self.children[i])
                .unwrap();
        }

        for msg in &right_msgs {
            sib_data
                .write_u32::<LittleEndian>(msg.op.serialize())
                .unwrap();
        }

        for msg in &right_msgs {
            sib_data
                .write_u64::<LittleEndian>(msg.key.bytes().len() as u64)
                .unwrap();
        }

        for msg in &right_msgs {
            sib_data
                .write_u64::<LittleEndian>(msg.data.bytes().len() as u64)
                .unwrap();
        }

        for i in (split + 1)..key_size {
            sib_data.write_all(self.keys[i].bytes()).unwrap();
        }
        for msg in &right_msgs {
            sib_data.write_all(msg.key.bytes()).unwrap();
        }
        for msg in &right_msgs {
            sib_data.write_all(msg.data.bytes()).unwrap();
        }

        let mut sib_keys = Vec::with_capacity(key_size - split - 1);
        let mut sib_children = Vec::with_capacity(key_size - split);
        let mut sib_buffer = Vec::with_capacity(right_msgs.len());

        for i in (split + 1)..(key_size + 1) {
            sib_children.push(self.children[i]);
        }

        let mut offset = (size_of::<u32>() + 3 * size_of::<u64>()) as isize;
        offset += ((key_size - split - 1) * size_of::<u64>()) as isize;
        offset += ((key_size - split) * size_of::<u64>()) as isize;
        offset += (2 * right_msgs.len() * size_of::<u64>()) as isize;
        offset += (right_msgs.len() * size_of::<u32>()) as isize;

        for i in (split + 1)..key_size {
            let len = self.keys[i].bytes().len();
            unsafe {
                let data = from_raw_parts_mut(sib_ptr.offset(offset), len);
                let key = Buf::Shared(data);
                sib_keys.push(key);
            }
            offset += len as isize;
        }

        for msg in &right_msgs {
            let msg = BufMessage {
                op: msg.op,
                key: Buf::Owned(vec![]),
                data: Buf::Owned(vec![]),
            };
            sib_buffer.push(msg);
        }

        for i in 0..right_msgs.len() {
            let len = right_msgs[i].key.bytes().len();
            sib_buffer[i].key = unsafe {
                let data = from_raw_parts_mut(sib_ptr.offset(offset), len);
                Buf::Shared(data)
            };
            offset += len as isize;
        }

        for i in 0..right_msgs.len() {
            let len = right_msgs[i].data.bytes().len();
            sib_buffer[i].data = unsafe {
                let data = from_raw_parts_mut(sib_ptr.offset(offset), len);
                Buf::Shared(data)
            };
            offset += len as isize;
        }

        let split_key = self.keys[split].to_vec();
        self.keys.truncate(split);
        self.children.truncate(split + 1);

        let body = Internal {
            level: self.level,
            data: sib_data,
            keys: sib_keys,
            buffer: sib_buffer,
            children: sib_children,
            serde: true,
        };
        NewSibling {
            key: split_key,
            body: Body::Internal(body),
        }
    }

    pub fn parent_to_child(
        &mut self,
        tree: &mut Tree,
        store: &mut Store,
        txn: &mut Transaction,
    ) -> io::Result<Option<NewSibling<'a>>> {
        self.buffer.sort_by(|a, b| a.key.bytes().cmp(b.key.bytes()));
        let mut indices = Vec::with_capacity(self.buffer.len());
        for msg in &self.buffer {
            let pos = self.keys.binary_search_by(
                |probe| msg.key.bytes().cmp(probe.bytes()),
            );
            let pos = match pos {
                Ok(val) | Err(val) => val,
            };
            indices.push(pos);
        }
        let (buff_idx, len, child_idx) = Internal::max_run(&indices);
        let mut msgs = self.buffer.split_off(buff_idx);
        let mut tail = msgs.split_off(len);
        self.buffer.append(&mut tail);
        let mut owned_msgs = Vec::with_capacity(len);
        for msg in msgs {
            owned_msgs.push(msg.into_message());
        }
        let child_id = self.children[child_idx];
        let mut child = store.read(child_id).unwrap();
        let newchild = child.upsert_msgs(tree, store, txn, owned_msgs)?;
        if child.header.epoch != tree.epoch {
            txn.delete.push(child.header.id);
            child.header.id = tree.next_id();
            child.header.epoch = tree.epoch;
        }
        let child_id = child.header.id;
        store.write(&child)?;
        self.children[child_idx] = child_id;

        if let Some(newchild) = newchild {
            self.keys.insert(child_idx, Buf::Owned(newchild.key));
            self.children.insert(child_idx + 1, newchild.id);
        }

        if self.keys.len() < tree.max_pivots {
            Ok(None)
        } else {
            Ok(Some(self.split()))
        }
    }

    pub fn upsert_msg(
        &mut self,
        tree: &mut Tree,
        store: &mut Store,
        txn: &mut Transaction,
        msg: Message,
    ) -> io::Result<Option<NewSibling<'a>>> {
        self.upsert(msg);
        if self.children.len() < tree.max_buffer {
            return Ok(None);
        }
        self.parent_to_child(tree, store, txn)
    }

    pub fn upsert_msgs(
        &mut self,
        tree: &mut Tree,
        store: &mut Store,
        txn: &mut Transaction,
        msgs: Vec<Message>,
    ) -> io::Result<Option<NewSibling<'a>>> {
        for msg in msgs {
            self.upsert(msg);
        }
        if self.children.len() < tree.max_buffer {
            return Ok(None);
        }
        self.parent_to_child(tree, store, txn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_internal() {
        let input = Internal {
            level: 1,
            data: vec![],
            keys: vec![],
            buffer: vec![],
            children: vec![],
            serde: false,
        };
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        let output = Internal::deserialize(wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(0, output.keys.len());
        assert_eq!(0, output.buffer.len());
        assert_eq!(0, output.children.len());
    }

    #[test]
    fn roundtrip_nonempty_internal() {
        let input = Internal {
            level: 1,
            data: vec![],
            keys: vec![Buf::Owned(b"hello".to_vec())],
            buffer: vec![
                BufMessage {
                    op: Operation::Assign,
                    key: Buf::Owned(b"foo".to_vec()),
                    data: Buf::Owned(b"bar".to_vec()),
                },
            ],
            children: vec![0, 1],
            serde: false,
        };
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        assert_eq!(
            8 * size_of::<u64>() + 2 * size_of::<u32>() + "hello".len() + "foo".len() + "bar".len(),
            wtr.len()
        );
        let output = Internal::deserialize(wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(b"hello", output.keys[0].bytes());
        assert_eq!(b"foo", output.buffer[0].key.bytes());
        assert_eq!(b"bar", output.buffer[0].data.bytes());
        assert_eq!(Operation::Assign, output.buffer[0].op);
        assert_eq!(vec![0, 1], output.children);
    }

    #[test]
    fn max_run() {
        let input = vec![1, 2, 3, 4];
        let (idx, len, val) = Internal::max_run(&input);
        assert_eq!(3, idx);
        assert_eq!(1, len);
        assert_eq!(4, val);
        let input = vec![1, 1, 3, 4];
        let (idx, len, val) = Internal::max_run(&input);
        assert_eq!(0, idx);
        assert_eq!(2, len);
        assert_eq!(1, val);
        let input = vec![1, 2, 2, 4];
        let (idx, len, val) = Internal::max_run(&input);
        assert_eq!(1, idx);
        assert_eq!(2, len);
        assert_eq!(2, val);
        let input = vec![1, 2, 3, 3];
        let (idx, len, val) = Internal::max_run(&input);
        assert_eq!(2, idx);
        assert_eq!(2, len);
        assert_eq!(3, val);
        let input = vec![1, 1, 1, 1];
        let (idx, len, val) = Internal::max_run(&input);
        assert_eq!(0, idx);
        assert_eq!(4, len);
        assert_eq!(1, val);
    }

    #[test]
    fn split_internal() {
        let mut input = Internal {
            level: 1,
            data: vec![],
            keys: vec![
                Buf::Owned(b"a".to_vec()),
                Buf::Owned(b"b".to_vec()),
                Buf::Owned(b"c".to_vec()),
                Buf::Owned(b"d".to_vec()),
            ],
            buffer: vec![
                BufMessage {
                    op: Operation::Assign,
                    key: Buf::Owned(b"a".to_vec()),
                    data: Buf::Owned(b"w".to_vec()),
                },
                BufMessage {
                    op: Operation::Assign,
                    key: Buf::Owned(b"b".to_vec()),
                    data: Buf::Owned(b"x".to_vec()),
                },
                BufMessage {
                    op: Operation::Assign,
                    key: Buf::Owned(b"c".to_vec()),
                    data: Buf::Owned(b"y".to_vec()),
                },
                BufMessage {
                    op: Operation::Assign,
                    key: Buf::Owned(b"d".to_vec()),
                    data: Buf::Owned(b"z".to_vec()),
                },
            ],
            children: vec![0, 1, 2, 3, 4],
            serde: false,
        };
        let sibling = input.split();

        assert_eq!(b"c".to_vec(), sibling.key);

        assert_eq!(2, input.keys.len());
        assert_eq!(2, input.buffer.len());
        assert_eq!(3, input.children.len());
        assert_eq!(b"a", input.keys[0].bytes());
        assert_eq!(b"b", input.keys[1].bytes());
        assert_eq!(b"a", input.buffer[0].key.bytes());
        assert_eq!(b"b", input.buffer[1].key.bytes());
        assert_eq!(b"w", input.buffer[0].data.bytes());
        assert_eq!(b"x", input.buffer[1].data.bytes());
        assert_eq!(vec![0, 1, 2], input.children);

        let sibling = match sibling.body {
            Body::Leaf(_) => panic!("expected internal node"),
            Body::Internal(node) => node,
        };
        assert_eq!(1, sibling.keys.len());
        assert_eq!(2, sibling.buffer.len());
        assert_eq!(2, sibling.children.len());
        assert_eq!(b"d", sibling.keys[0].bytes());
        assert_eq!(b"c", sibling.buffer[0].key.bytes());
        assert_eq!(b"d", sibling.buffer[1].key.bytes());
        assert_eq!(b"y", sibling.buffer[0].data.bytes());
        assert_eq!(b"z", sibling.buffer[1].data.bytes());
        assert_eq!(vec![3, 4], sibling.children);

        let mut wtr = vec![];
        let result = sibling.serialize(&mut wtr);
        assert!(result.is_ok());
        let output = Internal::deserialize(wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(1, output.keys.len());
        assert_eq!(2, output.buffer.len());
        assert_eq!(2, output.children.len());
        assert_eq!(b"d", output.keys[0].bytes());
        assert_eq!(b"c", output.buffer[0].key.bytes());
        assert_eq!(b"d", output.buffer[1].key.bytes());
        assert_eq!(b"y", output.buffer[0].data.bytes());
        assert_eq!(b"z", output.buffer[1].data.bytes());
        assert_eq!(vec![3, 4], output.children);
    }
}
