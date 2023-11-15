use std::collections::HashSet;
use sddms_shared::error::SddmsError;
use crate::transaction_id::TransactionId;

pub struct LiveTransactionSet {
    growing: tokio::sync::RwLock<HashSet<TransactionId>>,
    shrinking: tokio::sync::RwLock<HashSet<TransactionId>>,
}

impl LiveTransactionSet {
    pub fn new() -> Self {
        Self {
            growing: tokio::sync::RwLock::default(),
            shrinking: tokio::sync::RwLock::default(),
        }
    }

    pub async fn register_transaction(&self, trans: TransactionId) -> Result<(), SddmsError> {
        if self.transaction_exists(&trans).await {
            return Err(SddmsError::central(format!("Transaction {} already exists", trans)))
        }

        self.growing.write().await.insert(trans);
        Ok(())
    }

    pub async fn start_shrinking(&self, trans: &TransactionId) -> Result<(), SddmsError> {
        if !self.is_growing(trans).await {
            return Err(SddmsError::central(format!("Transaction {} is not current growing, so it cannot start shrinking", trans)))
        }

        self.growing.write().await.remove(trans);
        self.shrinking.write().await.insert(*trans);
        Ok(())
    }

    pub async fn remove(&self, trans: &TransactionId) -> Result<(), SddmsError> {
        // just remove from the transaction set
        self.shrinking.write().await.remove(trans);
        self.growing.write().await.remove(trans);
        Ok(())
    }

    pub async fn is_growing(&self, trans: &TransactionId) -> bool {
        self.growing.read().await.contains(trans)
    }

    pub async fn is_shrinking(&self, trans: &TransactionId) -> bool {
        self.shrinking.read().await.contains(trans)
    }

    pub async fn transaction_exists(&self, id: &TransactionId) -> bool {
        let (grow, shrink) = tokio::join!(self.is_growing(id), self.is_shrinking(id));
        grow || shrink
    }
}
