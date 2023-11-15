use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TransactionId {
    pub client_id: u32,
    pub transaction_id: u32,
}

impl TransactionId {
    pub fn new(transaction_id: u32, client_id: u32) -> Self {
        Self {
            transaction_id,
            client_id
        }
    }
}

#[derive(Debug)]
pub struct TransactionHistory {
    /// just the update statements invoked with this transaction
    update_stmts: Vec<String>,
    /// the id for this transaction
    transaction_id: TransactionId,
}

impl TransactionHistory {
    pub fn new(client_id: u32, trans_id: u32) -> Self {
        Self {
            transaction_id: TransactionId::new(trans_id, client_id),
            update_stmts: Vec::new(),
        }
    }

    pub fn push<StmtT: Into<String>>(&mut self, stmt: StmtT) {
        self.update_stmts.push(stmt.into())
    }

    pub fn update_stmts(&self) -> &Vec<String> {
        &self.update_stmts
    }

    pub fn transaction_id(&self) -> u32 {
        self.transaction_id.transaction_id
    }
    
    pub fn client_id(&self) -> u32 {
        self.transaction_id.client_id
    }
}

impl Deref for TransactionHistory {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        self.update_stmts.as_slice()
    }
}

#[derive(Default, Debug)]
pub struct TransactionHistoryMap {
    transactions: HashMap<TransactionId, TransactionHistory>
}

impl TransactionHistoryMap {

    pub fn push_transaction(&mut self, client_id: u32, trans_id: u32) {
        let full_trans_id = TransactionId::new(trans_id, client_id);
        self.transactions.insert(full_trans_id, TransactionHistory::new(client_id, trans_id));
    }
    
    pub fn remove_transaction(&mut self, client_id: u32, trans_id: u32) -> Option<TransactionHistory> {
        let trans_id = TransactionId::new(trans_id, client_id);
        self.transactions.remove(&trans_id)
    }
    
    pub fn get_transaction_for_client(&self, client_id: u32, transaction_id: u32) -> Option<&TransactionHistory> {
        let trans_id = TransactionId::new(transaction_id, client_id);
        self.transactions.get(&trans_id)
    }
    
    pub fn get_transaction_for_client_mut(&mut self, client_id: u32, transaction_id: u32) -> Option<&mut TransactionHistory> {
        let trans_id = TransactionId::new(transaction_id, client_id);
        self.transactions.get_mut(&trans_id)
    }
}
