use std::collections::HashSet;
use std::sync::RwLock;
use sddms_shared::error::SddmsError;
use crate::transaction_id::TransactionId;

pub struct LiveTransactionSet {
    growing: RwLock<HashSet<TransactionId>>,
    shrinking: RwLock<HashSet<TransactionId>>,
}

impl LiveTransactionSet {
    pub fn new() -> Self {
        Self {
            growing: RwLock::default(),
            shrinking: RwLock::default(),
        }
    }

    pub fn register_transaction(&self, trans: TransactionId) -> Result<(), SddmsError> {
        if self.transaction_exists(&trans) {
            return Err(SddmsError::central(format!("Transaction {} already exists", trans)))
        }

        self.growing.write().unwrap().insert(trans);
        Ok(())
    }

    pub fn start_shrinking(&self, trans: &TransactionId) -> Result<(), SddmsError> {
        if !self.is_growing(trans) {
            return Err(SddmsError::central(format!("Transaction {} is not current growing, so it cannot start shrinking", trans)))
        }

        self.growing.write().unwrap().remove(trans);
        self.shrinking.write().unwrap().insert(*trans);
        Ok(())
    }

    pub fn remove(&self, trans: &TransactionId) -> Result<(), SddmsError> {
        if !self.is_shrinking(trans) {
            return Err(SddmsError::central(format!("Transaction {} must be shrinking before it can be removed", trans)))
        }

        self.shrinking.write().unwrap().remove(trans);
        Ok(())
    }

    pub fn is_growing(&self, trans: &TransactionId) -> bool {
        self.growing.read().unwrap().contains(trans)
    }

    pub fn is_shrinking(&self, trans: &TransactionId) -> bool {
        self.shrinking.read().unwrap().contains(trans)
    }

    pub fn transaction_exists(&self, id: &TransactionId) -> bool {
        let is_growing = self.growing.read().unwrap().contains(id);
        let is_shrinking = self.shrinking.read().unwrap().contains(id);
        is_growing || is_shrinking
    }
}
