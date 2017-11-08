use super::message::Message;
use super::store::WriteStore;
use super::tree::WriteTree;
use super::writeinternal::WriteInternal;
use super::writeleaf::WriteLeaf;

pub enum WriteNode<'a> {
    Leaf(WriteLeaf<'a>),
    Internal(WriteInternal<'a>),
}

impl<'a, 'b> WriteNode<'a> {
    pub fn upsert_msgs(
        &mut self,
        tree: &mut WriteTree,
        store: &mut WriteStore,
        msgs: Vec<Message>,
    ) -> Option<WriteNode<'b>> {
        match *self {
            WriteNode::Leaf(ref mut node) => node.upsert_msgs(tree, msgs).map(WriteNode::Leaf),
            WriteNode::Internal(ref mut node) => {
                node.upsert_msgs(tree, store, msgs).map(WriteNode::Internal)
            }
        }
    }
}
