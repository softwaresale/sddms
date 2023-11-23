mod resource_lock;
mod lock_queue_opt;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use log::{debug, info, trace};
use tokio::task::yield_now;
use sddms_services::central_controller::LockMode;
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::live_transaction_set::LiveTransactionSet;
use crate::lock_table::lock_queue_opt::optimize_lock_queue;
use crate::lock_table::resource_lock::{ResourceLock};
use crate::transaction_id::TransactionId;

#[derive(Debug)]
pub enum LockRequestResult {
    HadLock,
    AcquiredLock,
    PromotedLock,
}

impl Display for LockRequestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LockRequestResult::HadLock => f.write_str("already had lock"),
            LockRequestResult::AcquiredLock => f.write_str("acquired lock"),
            LockRequestResult::PromotedLock => f.write_str("promoted lock to exclusive")
        }
    }
}

#[derive(Debug)]
pub struct LockTable {
    /// table of resources to be locked
    resources: tokio::sync::Mutex<HashMap<String, VecDeque<ResourceLock>>>,
    /// set of transactions that are currently live
    live_transactions: LiveTransactionSet,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            resources: tokio::sync::Mutex::default(),
            live_transactions: LiveTransactionSet::new(),
        }
    }

    async fn add_new_resource(&self, resource_name: &str) {
        let mut resources = self.resources.lock().await;
        if !resources.contains_key(resource_name) {
            resources.insert(resource_name.into(), VecDeque::default());
        }
    }

    pub async fn has_resource(&self, transaction_id: &TransactionId, resource: &str) -> Result<bool, SddmsError> {
        self.lock_set(transaction_id)
            .await
            .map(|lock_set| lock_set.contains(resource))
    }
    
    pub async fn register_transaction(&self, transaction_id: TransactionId) -> Result<(), SddmsError> {
        self.live_transactions.register_transaction(transaction_id).await
    }

    // removes any pending lock requests and remove the transaction from the live transaction set
    pub async fn finalize_transaction(&self, transaction_id: TransactionId) -> Result<(), SddmsError> {
        self.live_transactions.remove(&transaction_id).await
    }

    pub async fn transaction_exists(&self, transaction_id: &TransactionId) -> bool {
        self.live_transactions.transaction_exists(transaction_id).await
    }
    
    pub async fn lock_set(&self, transaction_id: &TransactionId) -> Result<HashSet<String>, SddmsError> {
        
        if !self.live_transactions.transaction_exists(&transaction_id).await {
            return Err(SddmsError::central(format!("Transaction {} doesn't exist", transaction_id)))
        }
        
        let resources = self.resources.lock().await;
        let acquired_resources = resources.iter()
            .filter(|(_, resource_queue)| {
                resource_queue.front().is_some_and(|front_val| front_val.is_locked_by(transaction_id))
            })
            .map(|(acquired_resource, _)| acquired_resource.clone())
            .collect::<HashSet<String>>();
        
        Ok(acquired_resources)
    }

    /// Determines if the given transaction already holds the lock for the given resources that's
    /// compatible with the given lock mode. If the resource wants an exclusive lock and it owns
    /// the resource exclusively, then the lock is owned. If the transaction wants a shared lock
    /// and it already owns the lock either exclusively or shared, then it's good.
    async fn has_lock_already(&self, transaction_id: &TransactionId, resource: &str, mode: LockMode) -> bool {
        let resources = self.resources.lock().await;
        let resource_queue = resources.get(resource).unwrap();
        let front_lock = resource_queue.front();

        match front_lock {
            None => {
                false
            }
            Some(front_lock) => {
                if mode == LockMode::Exclusive {
                    front_lock.is_locked_by_exclusive(transaction_id)
                } else if mode == LockMode::Shared {
                    front_lock.is_locked_by(transaction_id)
                } else {
                    unreachable!()
                }
            }
        }
    }

    /// tries to promote the lock. This case can only happen when the front lock is already locked
    /// in shared mode by the given transaction, and transaction wants to promote it to exclusive.
    /// If neither of these conditions is true, then it returns false. If the lock can be promoted,
    /// it'll promote the lock and return true. Otherwise, false will be returned
    async fn attempt_lock_promotion(&self, transaction_id: &TransactionId, resource: &str, mode: LockMode) -> bool {
        let mut resources = self.resources.lock().await;
        let resource_queue = resources.get_mut(resource).unwrap();
        debug!("{} queue before promotion: {:?}", resource, resource_queue);
        let front_lock = resource_queue.pop_front();

        match front_lock {
            None => {
                false
            }
            Some(front_lock) => {
                if mode == LockMode::Exclusive && front_lock.is_locked_by_shared(transaction_id) {
                    let (exclusive_lock, shared_lock) = front_lock.to_exclusive(transaction_id);
                    if shared_lock.is_some() {
                        resource_queue.push_front(shared_lock.unwrap());
                    }
                    resource_queue.push_front(exclusive_lock);
                    debug!("{} queue after promotion: {:?}", resource, resource_queue);
                    true
                } else {
                    resource_queue.push_front(front_lock);
                    false
                }
            }
        }
    }

    pub async fn acquire_lock(&self, transaction_id: TransactionId, resource: &str, mode: LockMode) -> Result<LockRequestResult, SddmsTermError> {
        if !self.live_transactions.is_growing(&transaction_id).await {
            return Err(SddmsError::central(format!("Transaction {} is not growing, so it cannot acquire locks", transaction_id)).into())
        }

        // if resource doesn't exist, add it
        self.add_new_resource(resource).await;

        // if this lock is already acquired, do nothing
        let has_lock = self.has_lock_already(&transaction_id, resource, mode).await;
        if has_lock {
            info!("{} already acquired lock {}", transaction_id, resource);
            return Ok(LockRequestResult::HadLock)
        }

        // attempt promoting the lock
        let lock_promoted = self.attempt_lock_promotion(&transaction_id, resource, mode).await;
        if lock_promoted {
            info!("{} promoted its shared lock on {} to exclusive", transaction_id, resource);
            return Ok(LockRequestResult::PromotedLock)
        }

        // if we don't own the lock or are unable to promote the lock, then we can draw a few
        // conclusions.
        // 1. It is possible that there are no locks currently in the request queue. That's fine.
        //    We need to enqueue our lock request
        // 2. If there are requests in the queue, then the first request does not contain our
        //    request is not compatible with the current lock. Either we don't have it or we
        //    can't promote it.
        //
        // In either of these cases, we need to enqueue our locking request.

        // check if this will cause deadlock
        self.detect_deadlock(transaction_id, resource).await?;

        // get in the queue for the given resource
        self.enqueue_resource(transaction_id, resource, mode).await?;
        info!("Transaction {} enqueued {:?} lock request for {}", transaction_id, mode, resource);

        // wait until we are at the front of the queue for the given resource
        let lock_result = loop {
            let resources = self.resources.lock().await;
            let resource_queue = resources.get(resource).unwrap();
            let front_lock = resource_queue.front().unwrap();

            if front_lock.is_locked_by(&transaction_id) {
                info!("After waiting, transaction {} acquired lock for {}", transaction_id, resource);
                break LockRequestResult::AcquiredLock;
            }

            yield_now().await;
        };

        // we got it finally
        Ok(lock_result)
    }

    pub async fn release_lock(&self, transaction_id: TransactionId, resource: &str) -> Result<(), SddmsError> {

        // Start shrinking if necessary
        if !self.live_transactions.is_shrinking(&transaction_id).await {
            self.live_transactions.start_shrinking(&transaction_id).await?;
        }

        let mut resources_table = self.resources.lock().await;
        let resource_vec = resources_table.get_mut(resource).unwrap();

        debug!("{} starting lock queue: {:?}", resource, resource_vec);

        let resource_lock = resource_vec.front_mut();

        let lock = match resource_lock {
            None => {
                return Err(SddmsError::central(format!("transaction {} does not own the lock for {}", transaction_id, resource)));
            }
            Some(resource_lock) => {
                if !resource_lock.is_locked_by(&transaction_id) {
                    return Err(SddmsError::central(format!("transaction {} does not own the lock for {}", transaction_id, resource)));
                } else {
                    resource_lock
                }
            }
        };

        let remove_lock = match lock {
            ResourceLock::Shared { owners, order } => {
                owners.remove(&transaction_id);
                let index = order.iter().position(|x| *x == transaction_id).unwrap();
                order.remove(index);
                owners.is_empty()
            }
            ResourceLock::Exclusive { .. } => {
                true
            }
        };

        if remove_lock {
            resource_vec.pop_front();
        }

        debug!("{} ending lock queue: {:?}", resource, resource_vec);

        Ok(())
    }

    async fn enqueue_resource(&self, transaction_id: TransactionId, resource: &str, mode: LockMode) -> Result<(), SddmsError> {
        let mut resource_table = self.resources.lock().await;
        let (resource_name, mut resource_queue) = resource_table.remove_entry(resource)
            .ok_or(SddmsError::central(format!("Resource '{}' doesn't exist", resource)))?;

        let lock = match mode {
            LockMode::Unspecified => { panic!("Can't handle unspecified lock mode") }
            LockMode::Exclusive => { ResourceLock::exclusive(transaction_id) }
            LockMode::Shared => { ResourceLock::shared(transaction_id) }
        };

        resource_queue.push_back(lock);
        debug!("{} lock queue after enqueueing: {:?}", resource, resource_queue);
        resource_queue = optimize_lock_queue(resource_queue);
        debug!("{} lock queue after optimizing: {:?}", resource, resource_queue);
        resource_table.insert(resource_name, resource_queue);

        Ok(())
    }

    pub async fn detect_deadlock(&self, transaction_id: TransactionId, resource: &str) -> Result<(), SddmsError> {
        // get the lock set of the given transaction
        let locked_resources = self.lock_set(&transaction_id).await?;

        // get the set of transactions that are before this transaction for the given resource
        let resources = self.resources.lock().await;
        let desired_resource_waiters = self.resource_waiters(&resources, resource, true).await;

        // for each locked resource...
        for resource in locked_resources {
            // ... see what resources are waiting on the resources we own...
            // side note here: we don't include the first because we know that this transaction is the first
            let owned_resource_waiters = self.resource_waiters(&resources, &resource, false).await;

            // ... and if any of them are in line before us for the resource we desire ...
            for waiter in &desired_resource_waiters {
                if owned_resource_waiters.contains(waiter) {
                    // ... then we will cause a deadlock
                    let err = SddmsError::central(format!("Transaction {} will deadlock system if it locks {}", transaction_id, resource));
                    return Err(err)
                }
            }
        }

        // ...otherwise we will not cause a deadlock
        Ok(())
    }

    async fn resource_waiters<'resource_map>(&self, resource_map: &'resource_map HashMap<String, VecDeque<ResourceLock>>, resource: &str, include_first: bool) -> HashSet<&'resource_map TransactionId> {
        let waiters = resource_map.get(resource).unwrap();
        let mut waiting_transactions: HashSet<&'resource_map TransactionId> = HashSet::new();

        let skip_amount = if include_first {
            0
        } else {
            1
        };

        for waiter in waiters.iter().skip(skip_amount) {
            match waiter {
                ResourceLock::Shared { owners, .. } => {
                    for owner in owners {
                        waiting_transactions.insert(owner);
                    }
                }
                ResourceLock::Exclusive { owner } => {
                    waiting_transactions.insert(owner);
                }
            }
        }

        waiting_transactions
    }
}
