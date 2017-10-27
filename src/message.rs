use super::operation::Operation;

use super::buf::Buf;

#[derive(Debug)]
pub struct Message<'a> {
    pub op: Operation,
    pub key: Buf<'a>,
    pub data: Buf<'a>,
}

impl<'a,'b> Message<'a> {

    pub fn create(&self) -> &[u8] {
        match self.op {
            Operation::Assign => self.data.bytes(),
        }
    }

    fn apply_assign(&self, buf : &mut Buf) {
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

    pub fn apply(&self, buf : &mut Buf) {
        match self.op {
            Operation::Assign => self.apply_assign(buf),
        };
    }
}