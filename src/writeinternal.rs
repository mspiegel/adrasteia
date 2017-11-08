use super::buf::Buf;
use super::message::Message;
use super::message::OwnedMessage;
use super::operation::Operation;
use super::store::WriteStore;
use super::tree::WriteTree;

use std::io::Cursor;
use std::io::Result;
use std::io::Write;
use std::mem::size_of;
use std::slice::from_raw_parts_mut;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct WriteInternal<'a> {
    #[allow(dead_code)]
    data: Buf<'a>,
    keys: Vec<Buf<'a>>,
    buffer: Vec<Message<'a>>,
    children: Vec<u64>,
}

impl<'a, 'b> WriteInternal<'a> {
    pub fn serialize(&self, wtr: &mut Write) -> Result<usize> {
        let mut total = 0;
        let key_size = self.keys.len();
        let buf_size = self.buffer.len();
        wtr.write_u64::<LittleEndian>(key_size as u64)?;
        wtr.write_u64::<LittleEndian>(buf_size as u64)?;
        total += 2 * size_of::<u64>();

        for key in &self.keys {
            let len = key.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
            total += len;
        }
        total += self.keys.len() * size_of::<u64>();

        for child in &self.children {
            wtr.write_u64::<LittleEndian>(*child)?;
        }
        total += self.children.len() * size_of::<u64>();

        for msg in &self.buffer {
            wtr.write_u32::<LittleEndian>(msg.op.serialize())?;
        }
        total += self.buffer.len() * size_of::<u32>();

        for msg in &self.buffer {
            let len = msg.key.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
            total += len;
        }
        total += self.buffer.len() * size_of::<u64>();

        for msg in &self.buffer {
            let len = msg.data.bytes().len();
            wtr.write_u64::<LittleEndian>(len as u64)?;
            total += len;
        }
        total += self.buffer.len() * size_of::<u64>();

        for key in &self.keys {
            wtr.write_all(key.bytes())?;
        }
        for msg in &self.buffer {
            wtr.write_all(msg.key.bytes())?;
        }
        for msg in &self.buffer {
            wtr.write_all(msg.data.bytes())?;
        }
        Ok(total)
    }

    pub fn deserialize(input: &mut [u8]) -> Result<WriteInternal> {
        let input_ptr = input.as_mut_ptr();
        let mut rdr = Cursor::new(input);
        let key_size = rdr.read_u64::<LittleEndian>()? as usize;
        let buf_size = rdr.read_u64::<LittleEndian>()? as usize;

        let mut keys = Vec::with_capacity(key_size);
        let mut children = Vec::with_capacity(key_size + 1);
        let mut buffer = Vec::with_capacity(buf_size);

        let mut offset = (2 * size_of::<u64>()) as isize;
        offset += (key_size * size_of::<u64>()) as isize;
        offset += ((key_size + 1) * size_of::<u64>()) as isize;
        offset += (3 * buf_size * size_of::<u64>()) as isize;

        for _ in 0..key_size {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                keys.push(Buf::Shared(
                    from_raw_parts_mut(input_ptr.offset(offset), len),
                ));
            }
            offset += len as isize;
        }

        for _ in 0..(key_size + 1) {
            children.push(rdr.read_u64::<LittleEndian>()?)
        }

        for _ in 0..buf_size {
            let msg = Message {
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

        Ok(WriteInternal {
            data: Buf::Shared(rdr.into_inner()),
            keys: keys,
            buffer: buffer,
            children: children,
        })
    }

    fn upsert(&mut self, msg: OwnedMessage) {
        self.buffer.push(msg.into_message());
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

    pub fn parent_to_child(&mut self, tree: &mut WriteTree, store: &mut WriteStore) {
        self.buffer.sort_by(|a, b| a.key.bytes().cmp(b.key.bytes()));
        let mut indices = vec![0; self.buffer.len()];
        for i in 0..self.buffer.len() {
            let pos = self.keys.binary_search_by(|probe| {
                self.buffer[i].key.bytes().cmp(probe.bytes())
            });
            indices[i] = match pos {
                Ok(val) | Err(val) => val,
            };
        }
        let (buff_idx, len, child_idx) = WriteInternal::max_run(&indices);
        let mut msgs = self.buffer.split_off(buff_idx);
        let mut tail = msgs.split_off(len);
        self.buffer.append(&mut tail);
        let mut owned_msgs = Vec::with_capacity(len);
        for msg in msgs {
            owned_msgs.push(msg.into_owned());
        }
        let child_id = self.children[child_idx];
        let mut child = store.read(child_id).unwrap();
        let sibling = child.upsert_msgs(tree, store, owned_msgs);
    }

    pub fn upsert_msg(
        &mut self,
        tree: &mut WriteTree,
        store: &mut WriteStore,
        msg: OwnedMessage,
    ) -> Option<WriteInternal<'b>> {
        self.upsert(msg);
        if self.children.len() < tree.max_buffer {
            return None;
        }
        self.parent_to_child(tree, store);
        None
    }

    pub fn upsert_msgs(
        &mut self,
        tree: &mut WriteTree,
        store: &mut WriteStore,
        msgs: Vec<OwnedMessage>,
    ) -> Option<WriteInternal<'b>> {
        for msg in msgs {
            self.upsert(msg);
        }
        if self.children.len() < tree.max_buffer {
            return None;
        }
        self.parent_to_child(tree, store);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_run() {
        let input = vec![1, 2, 3, 4];
        let (idx, len, val) = WriteInternal::max_run(&input);
        assert_eq!(3, idx);
        assert_eq!(1, len);
        assert_eq!(4, val);
        let input = vec![1, 1, 3, 4];
        let (idx, len, val) = WriteInternal::max_run(&input);
        assert_eq!(0, idx);
        assert_eq!(2, len);
        assert_eq!(1, val);
        let input = vec![1, 2, 2, 4];
        let (idx, len, val) = WriteInternal::max_run(&input);
        assert_eq!(1, idx);
        assert_eq!(2, len);
        assert_eq!(2, val);
        let input = vec![1, 2, 3, 3];
        let (idx, len, val) = WriteInternal::max_run(&input);
        assert_eq!(2, idx);
        assert_eq!(2, len);
        assert_eq!(3, val);
        let input = vec![1, 1, 1, 1];
        let (idx, len, val) = WriteInternal::max_run(&input);
        assert_eq!(0, idx);
        assert_eq!(4, len);
        assert_eq!(1, val);
    }
}
