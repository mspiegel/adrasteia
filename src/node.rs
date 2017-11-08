use super::message::Message;
use super::store::WriteStore;
use super::tree::WriteTree;
use super::writeinternal::WriteInternal;
use super::writeleaf::WriteLeaf;

use std::io::Cursor;
use std::io::Read;
use std::io::Result;
use std::io::Write;
use std::mem::size_of;


use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct WriteHeader {
    id: u64,
    epoch: u64,
}

pub enum WriteBody<'a> {
    Leaf(WriteLeaf<'a>),
    Internal(WriteInternal<'a>),
}

pub struct WriteNode<'a> {
    header: WriteHeader,
    body: WriteBody<'a>,
}

impl<'a, 'b> WriteNode<'a> {
    pub fn serialize(&self, wtr: &mut Write) -> Result<usize> {
        wtr.write_u64::<LittleEndian>(self.header.id)?;
        wtr.write_u64::<LittleEndian>(self.header.epoch)?;
        let head = 2 * size_of::<u64>() + size_of::<u8>();
        let count = match self.body {
            WriteBody::Leaf(ref node) => {
                wtr.write(&[0 as u8])?;
                node.serialize(wtr)
            }
            WriteBody::Internal(ref node) => {
                wtr.write(&[1 as u8])?;
                node.serialize(wtr)
            }
        };
        count.map(|x| x + head)
    }

    pub fn deserialize(input: &mut [u8]) -> Result<WriteNode> {
        let mut rdr = Cursor::new(input);
        let id = rdr.read_u64::<LittleEndian>()?;
        let epoch = rdr.read_u64::<LittleEndian>()?;
        let header = WriteHeader {
            id: id,
            epoch: epoch,
        };
        let mut byte = [0 as u8];
        rdr.read_exact(&mut byte)?;
        let body = match byte[0] {
            0 => WriteBody::Leaf(WriteLeaf::deserialize(rdr.into_inner())?),
            1 => WriteBody::Internal(WriteInternal::deserialize(rdr.into_inner())?),
            _ => panic!("unknown node type"),
        };
        Ok(WriteNode {
            header: header,
            body: body,
        })
    }

    pub fn upsert_msgs(
        &mut self,
        tree: &mut WriteTree,
        store: &mut WriteStore,
        msgs: Vec<Message>,
    ) -> Option<WriteNode<'b>> {
        let body = match self.body {
            WriteBody::Leaf(ref mut node) => node.upsert_msgs(tree, msgs).map(WriteBody::Leaf),
            WriteBody::Internal(ref mut node) => {
                node.upsert_msgs(tree, store, msgs).map(WriteBody::Internal)
            }
        };
        None
    }
}
