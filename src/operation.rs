
#[derive(Debug, Copy, Clone)]
pub enum Operation {
    Assign,
}

impl Operation {
    pub fn serialize(self) -> u32 {
        match self {
            Operation::Assign => 1,
        }
    }

    pub fn deserialize(val: u32) -> Operation {
        match val {
            1 => Operation::Assign,
            _ => panic!("unknown operation"),
        }
    }
}
