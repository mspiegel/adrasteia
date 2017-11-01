use super::operation::Operation;
use super::buf::Buf;
use super::message::Message;

#[derive(Debug)]
pub struct OwnedMessage {
    pub op: Operation,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}

impl<'a> OwnedMessage {
    pub fn create(self) -> (Vec<u8>, Vec<u8>) {
        match self.op {
            Operation::Assign => (self.key, self.data),
        }
    }

    fn apply_assign(self, buf: &mut Buf) {
        if let Buf::Shared(ref mut val) = *buf {
            if val.len() == self.data.len() {
                val.copy_from_slice(&self.data);
                return;
            }
        }
        *buf = Buf::Owned(self.data);
    }

    pub fn apply(self, buf: &mut Buf) {
        match self.op {
            Operation::Assign => self.apply_assign(buf),
        };
    }

    pub fn to_message(self) -> Message<'a> {
        Message {
            op: self.op,
            key: Buf::Owned(self.key),
            data: Buf::Owned(self.data),
        }
    }
}
