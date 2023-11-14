use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
use sddms_shared::error::SddmsError;
use crate::live_transaction_set::LiveTransactionSet;
use crate::transaction_id::TransactionId;

pub struct LockTable {
    /// table of resources to be locked
    resources: Mutex<HashMap<String, VecDeque<TransactionId>>>,
    /// signals when a lock was released
    released_cond: Condvar,
    /// set of transactions that are currently live
    live_transactions: LiveTransactionSet,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            resources: Mutex::default(),
            released_cond: Condvar::new(),
            live_transactions: LiveTransactionSet::new(),
        }
    }

    fn add_new_resource(&self, resource_name: &str) {
        let mut resources = self.resources.lock().unwrap();
        if !resources.contains_key(resource_name) {
            resources.insert(resource_name.into(), VecDeque::default());
        }
    }

    pub fn register_transaction(&self, transaction_id: TransactionId) -> Result<(), SddmsError> {
        self.live_transactions.register_transaction(transaction_id)
    }

    pub fn finalize_transaction(&self, transaction_id: TransactionId) -> Result<(), SddmsError> {
        self.live_transactions.remove(&transaction_id)
    }

    pub fn transaction_exists(&self, transaction_id: &TransactionId) -> bool {
        self.live_transactions.transaction_exists(transaction_id)
    }
    
    pub fn lock_set(&self, transaction_id: &TransactionId) -> Result<HashSet<String>, SddmsError> {
        
        if !self.live_transactions.transaction_exists(&transaction_id) {
            return Err(SddmsError::central(format!("Transaction {} doesn't exist", transaction_id)))
        }
        
        let resources = self.resources.lock().unwrap();
        let acquired_resources = resources.iter()
            .filter(|(_, resource_queue)| {
                resource_queue.front().is_some_and(|front_val| front_val == transaction_id)
            })
            .map(|(acquired_resource, _)| acquired_resource.clone())
            .collect::<HashSet<String>>();
        
        Ok(acquired_resources)
    }

    pub fn acquire_lock(&self, transaction_id: TransactionId, resource: &str) -> Result<(), SddmsError> {

        if !self.live_transactions.is_growing(&transaction_id) {
            return Err(SddmsError::central(format!("Transaction {} is not growing, so it cannot acquire locks", transaction_id)))
        }

        // if resource doesn't exist, add it
        self.add_new_resource(resource);

        // get in the queue for the given resource
        self.enqueue_resource(transaction_id, resource)?;

        // wait until we are at the front of the queue for the given resource
        let resources_table_lock = self.resources.lock().unwrap();
        let _lock = self.released_cond.wait_while(resources_table_lock, |table| {
            let front = table.get(resource).unwrap().front().unwrap();
            *front != transaction_id
        }).unwrap();

        Ok(())
    }

    pub fn release_lock(&self, transaction_id: TransactionId, resource: &str) -> Result<(), SddmsError> {

        // Start shrinking if necessary
        if !self.live_transactions.is_shrinking(&transaction_id) {
            self.live_transactions.start_shrinking(&transaction_id)?;
        }

        let mut resources_table = self.resources.lock().unwrap();
        let resource_vec = resources_table.get_mut(resource).unwrap();
        if resource_vec.is_empty() || *resource_vec.front().unwrap() != transaction_id {
            return Err(SddmsError::central(format!("transaction {} does not own the lock for {}", transaction_id, resource)));
        }

        resource_vec.pop_front();
        self.released_cond.notify_all();
        Ok(())
    }

    fn enqueue_resource(&self, transaction_id: TransactionId, resource: &str) -> Result<(), SddmsError> {
        let mut resource_table = self.resources.lock().unwrap();
        let resource_queue = resource_table.get_mut(resource)
            .ok_or(SddmsError::central(format!("Resource '{}' doesn't exist", resource)))?;

        resource_queue.push_back(transaction_id);
        Ok(())
    }
}
