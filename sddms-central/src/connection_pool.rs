use std::collections::HashMap;
use std::sync::{Arc};
use std::sync::atomic::{AtomicU32, Ordering};
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::site_client::SiteClient;

pub struct ConnectionPool {
    /// map of connections
    connections: tokio::sync::Mutex<HashMap<u32, String>>,
    /// keep track of site ids
    site_ids: Arc<AtomicU32>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connections: tokio::sync::Mutex::new(HashMap::new()),
            site_ids: Arc::new(AtomicU32::new(0)),
        }
    }

    pub async fn register_site(&self, host: &str, port: u16) -> Result<u32, SddmsError> {
        let conn_str = format!("http://{}:{}", host, port);

        let site_id = self.site_ids.fetch_add(1, Ordering::AcqRel);
        let mut conn_map = self.connections.lock().await;
        conn_map.insert(site_id, conn_str);
        Ok(site_id)
    }

    pub async fn replicate_sites(&self, update_history: &[String], originating_site: u32) -> Result<(), SddmsTermError> {
        let connection_pool = self.connections.lock().await;
        for (site_id, connection_string) in connection_pool.iter() {
            if site_id == &originating_site {
                continue;
            }

            let mut connection = SiteClient::connect(connection_string)
                .await?;

            connection.replicate_updates(update_history, originating_site).await?;
        }

        Ok(())
    }
}
