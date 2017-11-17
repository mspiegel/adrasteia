use super::node::ReadNode;
use super::node::WriteNode;

use std::collections::HashMap;

#[derive(Default)]
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

    pub fn write(&mut self, node: WriteNode<'a>) {
        self.nodes.insert(node.header.id, Box::new(node));
    }

    pub fn schedule_delete(&mut self, id: u64) {
        self.nodes.remove(&id);
    }
}

pub trait ReadStore {
    fn read(&self, id: u64) -> Option<ReadNode>;
}
