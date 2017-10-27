use super::buf::Buf;

use super::message::Message;
use super::omessage::OwnedMessage;

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