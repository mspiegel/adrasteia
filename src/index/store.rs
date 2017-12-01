use super::node::Node;

use std::fs;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::Path;

pub trait Store<'a> {
    fn read(&self, id: u64) -> io::Result<Node<'a>>;
    fn write(&mut self, node: &Node<'a>) -> io::Result<()>;
    fn schedule_delete(&mut self, id: u64) -> io::Result<()>;
}

pub struct LocalStore {
    pub path: Path,
}

impl<'a> LocalStore {
    pub fn read(&self, id: u64) -> io::Result<Node<'a>> {
        let file_path = self.path.join(id.to_string());
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Node::deserialize(buffer)
    }

    pub fn write(&mut self, node: &Node<'a>) -> io::Result<()> {
        let file_path = self.path.join(node.id().to_string());
        let mut file = File::create(file_path)?;
        node.serialize(&mut file)
    }

    pub fn schedule_delete(&mut self, id: u64) -> io::Result<()> {
        let file_path = self.path.join(id.to_string());
        fs::remove_file(file_path)
    }
}
