use super::operation::Operation;
use super::buf::Buf;

#[derive(Debug)]
pub struct Message {
    pub op: Operation,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}

impl<'a> Message {
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

    pub fn into_buf_message(self) -> BufMessage<'a> {
        BufMessage {
            op: self.op,
            key: Buf::Owned(self.key),
            data: Buf::Owned(self.data),
        }
    }
}

#[derive(Debug)]
pub struct BufMessage<'a> {
    pub op: Operation,
    pub key: Buf<'a>,
    pub data: Buf<'a>,
}

impl<'a> BufMessage<'a> {
    fn apply_assign(&self, buf: &mut Buf) {
        if let Buf::Owned(ref mut val) = *buf {
            val.clear();
            val.extend_from_slice(self.data.bytes());
            return;
        }
        if let Buf::Shared(ref mut val) = *buf {
            if val.len() == self.data.len() {
                val.copy_from_slice(self.data.bytes());
                return;
            }
        }
        *buf = Buf::Owned(self.data.to_vec());
    }

    pub fn apply(&self, buf: &mut Buf) {
        match self.op {
            Operation::Assign => self.apply_assign(buf),
        };
    }

    pub fn into_message(self) -> Message {
        let key = match self.key {
            Buf::Shared(val) => val.to_vec(),
            Buf::Owned(val) => val,
        };
        let data = match self.data {
            Buf::Shared(val) => val.to_vec(),
            Buf::Owned(val) => val,
        };
        Message {
            op: self.op,
            key: key,
            data: data,
        }
    }
}
