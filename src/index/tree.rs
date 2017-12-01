use super::error::ErrorType;
use super::mode::Mode;
use super::store::Store;
use super::transaction::Transaction;

use std::io;

pub struct Tree {
    pub epoch: u64,
    pub id: u64,
    pub max_pivots: usize,
    pub max_buffer: usize,
    pub leafs: Vec<u64>,
    pub mode: Mode,
    pub txn: bool,
}

impl Tree {
    pub fn new(max_pivots: usize, max_buffer: usize, mode: Mode) -> Tree {
        Tree {
            epoch: 0,
            id: 0,
            max_pivots: max_pivots,
            max_buffer: max_buffer,
            leafs: vec![],
            mode: mode,
            txn: false,
        }
    }

    pub fn scan<F>(&self, store: &Store, scanner: F)
    where
        F: Fn(&[u8], &[u8]),
    {
        for id in &self.leafs {
            let node = store.read(*id).unwrap();
            let leaf = node.body.leaf();
            for i in 0..leaf.keys.len() {
                scanner(leaf.keys[i].bytes(), leaf.vals[i].bytes());
            }
        }
    }

    pub fn begin_txn(&mut self) -> Result<Transaction, ErrorType> {
        if self.txn {
            Result::Err(ErrorType::Msg(format!(
                "previous transaction {} must be closed",
                self.epoch
            )))
        } else {
            self.epoch += 1;
            self.txn = true;
            Ok(Transaction {
                epoch: self.epoch,
                delete: vec![],
            })
        }
    }

    fn close_txn(&mut self, store: &mut Store, txn: Transaction) -> io::Result<()> {
        for id in txn.delete {
            store.schedule_delete(id)?;
        }
        Ok(())
    }

    pub fn end_txn(&mut self, store: &mut Store, txn: Transaction) -> Result<(),ErrorType> {
        if !self.txn {
            return Err(ErrorType::Msg(
                "transaction has already been closed".to_string(),
            ));
        }
        if self.epoch != txn.epoch {
            return Err(ErrorType::Msg(format!(
                "tree epoch {} != transaction epoch {}",
                self.epoch,
                txn.epoch
            )));
        }
        self.close_txn(store, txn).map_err(ErrorType::IO)
    }

    pub fn next_id(&mut self) -> u64 {
        self.id += 1;
        self.id
    }
}
