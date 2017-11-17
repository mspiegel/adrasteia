use super::message::Message;
use super::readleaf::ReadLeaf;
use super::store::WriteStore;
use super::transaction::Transaction;
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

pub struct Header {
    pub id: u64,
    pub epoch: u64,
}

pub enum ReadBody<'a> {
    Leaf(ReadLeaf<'a>),
}

pub enum WriteBody<'a> {
    Leaf(WriteLeaf<'a>),
    Internal(WriteInternal<'a>),
}

pub struct ReadNode<'a> {
    pub header: Header,
    pub body: ReadBody<'a>,
}

pub struct WriteNode<'a> {
    pub header: Header,
    pub body: WriteBody<'a>,
}

pub struct NewSibling<'a> {
    pub key: Vec<u8>,
    pub body: WriteBody<'a>,
}

pub struct NewChild {
    pub key: Vec<u8>,
    pub id: u64,
}

impl<'a> ReadBody<'a> {
    pub fn leaf(&self) -> &ReadLeaf<'a> {
        match *self {
            ReadBody::Leaf(ref leaf) => leaf,
        }
    }
}

impl<'a> WriteNode<'a> {
    pub fn serialize(&self, wtr: &mut Write) -> Result<usize> {
        wtr.write_u64::<LittleEndian>(self.header.id)?;
        wtr.write_u64::<LittleEndian>(self.header.epoch)?;
        let head = 2 * size_of::<u64>() + size_of::<u8>();
        let count = match self.body {
            WriteBody::Leaf(ref node) => {
                wtr.write_all(&[0 as u8])?;
                node.serialize(wtr)
            }
            WriteBody::Internal(ref node) => {
                wtr.write_all(&[1 as u8])?;
                node.serialize(wtr)
            }
        };
        count.map(|x| x + head)
    }

    pub fn deserialize(input: &mut [u8]) -> Result<WriteNode> {
        let mut rdr = Cursor::new(input);
        let id = rdr.read_u64::<LittleEndian>()?;
        let epoch = rdr.read_u64::<LittleEndian>()?;
        let header = Header {
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

    fn upsert(
        &mut self,
        body: Option<NewSibling<'a>>,
        tree: &mut WriteTree,
        store: &mut WriteStore<'a>,
    ) -> Option<NewChild> {
        if let Some(inner) = body {
            let (key, body) = (inner.key, inner.body);

            let id = tree.next_id();
            let header = Header {
                epoch: tree.epoch,
                id: id,
            };
            let sibling = WriteNode {
                header: header,
                body: body,
            };
            store.write(sibling);
            Some(NewChild { id: id, key: key })
        } else {
            None
        }
    }

    pub fn upsert_msg(
        &mut self,
        tree: &mut WriteTree,
        store: &mut WriteStore<'a>,
        txn: &mut Transaction,
        msg: Message,
    ) -> Option<NewChild> {
        let body = match self.body {
            WriteBody::Leaf(ref mut node) => node.upsert_msg(tree, msg),
            WriteBody::Internal(ref mut node) => node.upsert_msg(tree, store, txn, msg),
        };
        self.upsert(body, tree, store)
    }

    pub fn upsert_msgs(
        &mut self,
        tree: &mut WriteTree,
        store: &mut WriteStore<'a>,
        txn: &mut Transaction,
        msgs: Vec<Message>,
    ) -> Option<NewChild> {
        let body = match self.body {
            WriteBody::Leaf(ref mut node) => node.upsert_msgs(tree, msgs),
            WriteBody::Internal(ref mut node) => node.upsert_msgs(tree, store, txn, msgs),
        };
        self.upsert(body, tree, store)
    }
}
