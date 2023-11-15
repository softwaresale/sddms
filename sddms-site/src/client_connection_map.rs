use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use log::info;
use rusqlite::{Connection, OpenFlags};
use sddms_services::site_controller::InvokeQueryResults;
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::sqlite_row_serializer::serialize_row;

pub struct ClientConnection {
    connection: tokio::sync::Mutex<Connection>,
    id: u32,
}

impl ClientConnection {
    fn new(connection: Connection, id: u32) -> Self {
        Self {
            connection: tokio::sync::Mutex::new(connection),
            id,
        }
    }

    pub async fn invoke_read_query(&self, query_text: &str) -> Result<InvokeQueryResults, SddmsError> {

        let sliced_query_text = if query_text.ends_with(";") {
            &query_text[0..query_text.len()-1]
        } else {
            query_text
        };

        let mut results = InvokeQueryResults::default();
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(sliced_query_text)
            .map_err(|err| SddmsError::general("Failed to prepare query").with_cause(err))?;

        let col_names = statement.column_names().iter()
            .map(|col_name| String::from(*col_name))
            .collect::<Vec<_>>();

        let serialized_rows = statement
            .query_map([], |row| {
                Ok(serialize_row(&row, &col_names))
            })
            .map_err(|err| SddmsError::site("Error while executing query").with_cause(err))
            ?.filter_map(|result| result.ok())
            .collect::<Vec<_>>();

        info!("Read {} rows", serialized_rows.len());

        let payload_results = serde_json::to_vec(&serialized_rows)
            .map_err(|err| SddmsError::general("Failed to serialize record payload").with_cause(err))?;

        results.data_payload = Some(payload_results);
        results.column_names = col_names.into_iter().map(|column| String::from(column)).collect();
        Ok(results)
    }

    pub async fn invoke_modify_query(&self, query_text: &str) -> Result<InvokeQueryResults, SddmsError> {
        let mut results = InvokeQueryResults::default();
        let connection = self.connection.lock().await;
        connection.execute(query_text, ())
            .map_err(|err| SddmsError::general("Failed to invoke SQL query").with_cause(err))?;

        let affected_rows = connection.changes() as u32;
        results.affected_records = Some(affected_rows);
        info!("Updated {} rows", affected_rows);
        Ok(results)
    }

    pub async fn invoke_one_off_stmt(&self, query_text: &str) -> Result<usize, SddmsTermError> {
        let connection = self.connection.lock().await;
        connection.execute(query_text, ())
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
        let db_conn = Connection::open(db_path)
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
