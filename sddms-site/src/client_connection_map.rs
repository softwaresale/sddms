use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use log::info;
use sqlite::{ConnectionThreadSafe};
use sddms_services::site_controller::InvokeQueryResults;
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::sqlite_row_serializer::serialize_row;

pub struct ClientConnection {
    connection: ConnectionThreadSafe,
    id: u32,
}

impl ClientConnection {
    fn new(connection: ConnectionThreadSafe, id: u32) -> Self {
        Self {
            connection,
            id,
        }
    }

    pub fn invoke_read_query(&self, query_text: &str) -> Result<InvokeQueryResults, SddmsError> {

        let sliced_query_text = if query_text.ends_with(";") {
            &query_text[0..query_text.len()-1]
        } else {
            query_text
        };

        let mut results = InvokeQueryResults::default();
        let statement = self.connection.prepare(sliced_query_text)
            .map_err(|err| SddmsError::general("Failed to prepare query").with_cause(err))?;

        let col_names = statement.column_names().to_vec();

        let serialized_rows = statement.into_iter()
            .map(|row| {
                let row = row.unwrap();
                serialize_row(&row, &col_names)
            })
            .collect::<Vec<_>>();

        info!("Read {} rows", serialized_rows.len());

        let payload_results = serde_json::to_vec(&serialized_rows)
            .map_err(|err| SddmsError::general("Failed to serialize record payload").with_cause(err))?;

        results.data_payload = Some(payload_results);
        Ok(results)
    }

    pub fn invoke_modify_query(&self, query_text: &str) -> Result<InvokeQueryResults, SddmsError> {
        let mut results = InvokeQueryResults::default();
        self.connection.execute(query_text)
            .map_err(|err| SddmsError::general("Failed to invoke SQL query").with_cause(err))?;

        let affected_rows = self.connection.change_count() as u32;
        results.affected_records = Some(affected_rows);
        info!("Updated {} rows", affected_rows);
        Ok(results)
    }

    pub fn invoke_one_off_stmt(&self, query_text: &str) -> Result<(), SddmsTermError> {
        self.connection.execute(query_text)
            .map_err(|err| SddmsError::general("Failed to execute one off SQL statement").with_cause(err))
            .map_err(|sddms_err| SddmsTermError::from(sddms_err))
    }
}

pub struct ClientConnectionMap {
    /// map of connections
    connections: HashMap<u32, ClientConnection>,
    /// how many clients are registered
    client_counter: AtomicU32,
}

impl ClientConnectionMap {
    pub fn new() -> Self {
        Self {
            connections: Default::default(),
            client_counter: AtomicU32::new(0),
        }
    }

    pub fn open_connection(&mut self, db_path: &Path) -> Result<u32, SddmsError> {
        // open connection to database
        let db_conn = sqlite::Connection::open_thread_safe(db_path)
            .map_err(|err| SddmsError::site("Could not open database").with_cause(err))?;

        let next_id = self.next_client_id();

        let connection = ClientConnection::new(db_conn, next_id);

        self.connections.insert(next_id, connection);
        Ok(next_id)
    }

    pub fn get_client_connection(&self, client_id: u32) -> Option<&ClientConnection> {
        self.connections.get(&client_id)
    }

    fn next_client_id(&self) -> u32 {
        self.client_counter.fetch_add(1, Ordering::SeqCst)
    }
}
