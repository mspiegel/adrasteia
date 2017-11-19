use super::node::Node;

pub trait Store<'a> {
    fn read(&self, id: u64) -> Option<Node<'a>>;
    fn write(&mut self, node: Node<'a>);
    fn schedule_delete(&mut self, id: u64);
}