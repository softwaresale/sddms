use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};
use sddms_shared::error::SddmsError;

pub struct ConnectionPool {
    /// map of connections
    connections: Mutex<HashMap<u32, String>>,
    /// keep track of site ids
    site_ids: Arc<AtomicU32>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            site_ids: Arc::new(AtomicU32::new(0)),
        }
    }

    pub async fn register_site(&self, host: &str, port: u16) -> Result<u32, SddmsError> {
        let conn_str = format!("http://{}:{}", host, port);

        let site_id = self.site_ids.fetch_add(1, Ordering::AcqRel);
        let mut conn_map = self.connections.lock()
            .map_err(|_err| SddmsError::general("Failed to acquire connection map lock"))?;
        
        conn_map.insert(site_id, conn_str);
        Ok(site_id)
    }
}
