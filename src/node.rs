use super::internal::Internal;
use super::leaf::Leaf;
use super::message::Message;
use super::store::Store;
use super::transaction::Transaction;
use super::tree::Tree;

use std::io::Cursor;
use std::io::Read;
use std::io::Result;
use std::io::Write;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct Header {
    pub id: u64,
    pub epoch: u64,
}

pub enum Body<'a> {
    Leaf(Leaf<'a>),
    Internal(Internal<'a>),
}

pub struct Node<'a> {
    pub header: Header,
    pub body: Body<'a>,
}

pub struct NewSibling<'a> {
    pub key: Vec<u8>,
    pub body: Body<'a>,
}

pub struct NewChild {
    pub key: Vec<u8>,
    pub id: u64,
}

impl<'a> Body<'a> {
    pub fn level(&self) -> u32 {
        match *self {
            Body::Leaf(_) => 0,
            Body::Internal(ref internal) => internal.level,
        }
    }

    pub fn leaf(&self) -> &Leaf<'a> {
        match *self {
            Body::Leaf(ref leaf) => leaf,
            Body::Internal(_) => panic!("attempt to convert internal to leaf"),
        }
    }
}

impl<'a> Node<'a> {
    pub fn id(&self) -> u64 {
        self.header.id
    }

    pub fn serialize(&self, wtr: &mut Write) -> Result<()> {
        wtr.write_u64::<LittleEndian>(self.header.id)?;
        wtr.write_u64::<LittleEndian>(self.header.epoch)?;
        match self.body {
            Body::Leaf(ref node) => {
                wtr.write_all(&[0 as u8])?;
                node.serialize(wtr)
            }
            Body::Internal(ref node) => {
                wtr.write_all(&[1 as u8])?;
                node.serialize(wtr)
            }
        }
    }

    pub fn deserialize(input: Vec<u8>) -> Result<Node<'a>> {
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
            0 => Body::Leaf(Leaf::deserialize(rdr.into_inner())?),
            1 => Body::Internal(Internal::deserialize(rdr.into_inner())?),
            _ => panic!("unknown node type"),
        };
        Ok(Node {
            header: header,
            body: body,
        })
    }

    fn upsert(
        &mut self,
        body: Option<NewSibling<'a>>,
        tree: &mut Tree,
        store: &mut Store<'a>,
    ) -> Result<Option<NewChild>> {
        if let Some(inner) = body {
            let (key, body) = (inner.key, inner.body);

            let id = tree.next_id();
            let header = Header {
                epoch: tree.epoch,
                id: id,
            };
            let sibling = Node {
                header: header,
                body: body,
            };
            store.write(&sibling)?;
            Ok(Some(NewChild { id: id, key: key }))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_msg(
        &mut self,
        tree: &mut Tree,
        store: &mut Store<'a>,
        txn: &mut Transaction,
        msg: Message,
    ) -> Result<Option<NewChild>> {
        let body = match self.body {
            Body::Leaf(ref mut node) => Ok(node.upsert_msg(tree, msg)),
            Body::Internal(ref mut node) => node.upsert_msg(tree, store, txn, msg),
        }?;
        self.upsert(body, tree, store)
    }

    pub fn upsert_msgs(
        &mut self,
        tree: &mut Tree,
        store: &mut Store<'a>,
        txn: &mut Transaction,
        msgs: Vec<Message>,
    ) -> Result<Option<NewChild>> {
        let body = match self.body {
            Body::Leaf(ref mut node) => Ok(node.upsert_msgs(tree, msgs)),
            Body::Internal(ref mut node) => node.upsert_msgs(tree, store, txn, msgs),
        }?;
        self.upsert(body, tree, store)
    }
}
