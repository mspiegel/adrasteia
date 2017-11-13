use super::error::ErrorType;
use super::store::WriteStore;
use super::transaction::Transaction;

use std::io;

pub struct WriteTree {
    pub epoch: u64,
    pub id: u64,
    pub max_pivots: usize,
    pub max_buffer: usize,
    pub txn: bool,
}

impl WriteTree {
    pub fn new(max_pivots: usize, max_buffer: usize) -> WriteTree {
        WriteTree {
            epoch: 0,
            id: 0,
            max_pivots: max_pivots,
            max_buffer: max_buffer,
            txn: false,
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
            Result::Ok(Transaction {
                epoch: self.epoch,
                delete: vec![],
            })
        }
    }

    fn close_txn(&mut self, store: &mut WriteStore, txn: Transaction) -> Option<io::Error> {
        for id in txn.delete {
            store.schedule_delete(id);
        }
        None
    }

    pub fn end_txn(&mut self, store: &mut WriteStore, txn: Transaction) -> Option<ErrorType> {
        if !self.txn {
            return Some(ErrorType::Msg(
                "transaction has already been closed".to_string(),
            ));
        }
        if self.epoch != txn.epoch {
            return Some(ErrorType::Msg(format!(
                "tree epoch {} != transaction epoch {}",
                self.epoch,
                txn.epoch
            )));
        }
        self.close_txn(store, txn).map(ErrorType::IO)
    }

    pub fn next_id(&mut self) -> u64 {
        self.id += 1;
        self.id
    }
}
