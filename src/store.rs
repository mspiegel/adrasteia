use super::node::WriteNode;

use std::collections::HashMap;

pub struct WriteStore {
    nodes: HashMap<u64, Box<WriteNode>>,
}

impl WriteStore {
    pub fn new() -> WriteStore {
        WriteStore { nodes: HashMap::new() }
    }

    pub fn read(&mut self, id: u64) -> Option<Box<WriteNode>> {
        self.nodes.remove(&id)
    }
}
