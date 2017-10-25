use super::buf::Buf;

pub enum Operation {
    Assign,
}

pub struct Message<'a> {
    pub op: Operation,
    pub key: &'a [u8],
    pub data: &'a [u8],
}

impl<'a> Message<'a> {
    pub fn create(&self) -> &[u8] {
        match self.op {
            Operation::Assign => self.data,
        }
    }

    fn apply_assign(&self, buf : &mut Buf) {
        if let Buf::Unique(ref mut val) = *buf {
            val.clear();
            val.extend_from_slice(self.data);
            return;
        }
        if let Buf::Shared(ref mut val) = *buf {
            if val.len() == self.data.len() {
                val.copy_from_slice(self.data);
                return;
            }
        }
        *buf = Buf::Unique(self.data.to_vec());
    }

    pub fn apply(&self, buf : &mut Buf) {
        match self.op {
            Operation::Assign => self.apply_assign(buf),
        };
    }
}