use super::node::WriteNode;

use std::collections::HashMap;

pub struct WriteStore<'a> {
    nodes: HashMap<u64, Box<WriteNode<'a>>>,
}

impl<'a> WriteStore<'a> {
    pub fn new() -> WriteStore<'a> {
        WriteStore { nodes: HashMap::new() }
    }

    pub fn read(&mut self, id: u64) -> Option<Box<WriteNode<'a>>> {
        self.nodes.remove(&id)
    }
}
