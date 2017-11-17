use super::node::ReadNode;
use super::node::WriteNode;

pub trait WriteStore<'a> {
    fn read(&mut self, id: u64) -> Option<Box<WriteNode<'a>>>;
    fn write(&mut self, node: WriteNode<'a>);
    fn schedule_delete(&mut self, id: u64);
}

pub trait ReadStore {
    fn read(&self, id: u64) -> Option<&ReadNode>;
}
