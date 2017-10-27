use byteorder::LittleEndian;
use byteorder::WriteBytesExt;

use super::buf::Buf;
use super::message::Message;
use super::ownmessage::OwnedMessage;
use super::tree::WriteTree;

use std::io::Result;
use std::io::Write;

pub struct WriteInternal<'a> {
    id: u64,
    epoch: u64,
    #[allow(dead_code)]
    data: Buf<'a>,
    keys: Vec<Buf<'a>>,
    buffer: Vec<Message<'a>>,
    children: Vec<u64>,
}

impl<'a,'b> WriteInternal<'a> {

    pub fn serialize(&self, wtr: &mut Write) -> Result<usize> {
        let key_size = self.keys.len();
        let buf_size = self.buffer.len();
        wtr.write_u64::<LittleEndian>(self.id)?;
        wtr.write_u64::<LittleEndian>(self.epoch)?;
        wtr.write_u64::<LittleEndian>(key_size as u64)?;
        wtr.write_u64::<LittleEndian>(buf_size as u64)?;
        for child in &self.children {
            wtr.write_u64::<LittleEndian>(*child)?;
        }
        for msg in &self.buffer {
            wtr.write_u32::<LittleEndian>(msg.op.serialize())?;
        }
        for msg in &self.buffer {
            wtr.write_u64::<LittleEndian>(msg.key.bytes().len() as u64)?;
        }
        for msg in &self.buffer {
            wtr.write_u64::<LittleEndian>(msg.data.bytes().len() as u64)?;
        }
        for msg in &self.buffer {
            wtr.write_all(msg.key.bytes())?;
        }
        for msg in &self.buffer {
            wtr.write_all(msg.data.bytes())?;
        }
        Ok(0 as usize)
    }

    pub fn upsert_owned(&mut self, tree : &mut WriteTree, msg : OwnedMessage) -> Option<WriteInternal<'b>> {
        self.buffer.push(msg.to_message());
        if self.children.len() < tree.max_buffer {
            return None
        }

        None
    }

}