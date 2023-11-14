use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{RwLock};
use log::{debug, info};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TransactionId {
    pub site_id: u32,
    pub transaction_id: u32,
}

impl TransactionId {
    pub fn new(site_id: u32, trans_id: u32) -> Self {
        Self {
            site_id,
            transaction_id: trans_id
        }
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.site_id, self.transaction_id))
    }
}

impl From<u64> for TransactionId {
    fn from(value: u64) -> Self {
        let trans_id = value as u32;
        let site_id = (value >> 32) as u32;

        Self {
            site_id,
            transaction_id: trans_id
        }
    }
}

impl Into<u64> for TransactionId {
    fn into(self) -> u64 {
        let site_id = self.site_id as u64;
        let trans_id = self.transaction_id as u64;
        (site_id << 32) | trans_id
    }
}

pub struct TransactionIdGenerator {
    sites: RwLock<HashMap<u32, AtomicU32>>
}

impl TransactionIdGenerator {
    pub fn new() -> Self {
        Self {
            sites: RwLock::new(HashMap::new())
        }
    }

    pub fn next_trans_id(&self, site_id: u32) -> TransactionId {
        debug!("Getting next transaction id for site {}", site_id);

        // potentially insert site if it doesn't exist yet
        self.add_new_site(site_id);

        // Acquire the site transaction counter
        let sites_read_lock = self.sites.read().unwrap();
        let existing_counter = sites_read_lock.get(&site_id).unwrap();
        debug!("Got transaction counter for site {}. Currently has value {}", site_id, existing_counter.load(Ordering::Acquire));

        // get next transaction
        let next_trans_id = existing_counter.fetch_add(1, Ordering::SeqCst);
        debug!("Allocated new transaction {} for site {}", next_trans_id, site_id);
        debug!("After allocating, counter has value {}", existing_counter.load(Ordering::Acquire));
        TransactionId::new(site_id, next_trans_id)
    }

    fn add_new_site(&self, site_id: u32) {
        let exists = self.sites.read().unwrap().contains_key(&site_id);
        if !exists {
            debug!("Site {} does not exist. Inserting...", site_id);
            self.sites.write().unwrap().insert(site_id, AtomicU32::new(0));
        }
    }
}
