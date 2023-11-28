use serde_json::{Map, Value};
use tonic::transport::Channel;
use sddms_services::shared::{FinalizeMode, ReturnStatus};
use sddms_services::site_controller::invoke_query_response::InvokeQueryPayload;
use sddms_services::site_controller::{BeginTransactionRequest, FinalizeTransactionRequest, InvokeQueryRequest, RegisterClientRequest};
use sddms_services::site_controller::begin_transaction_response::BeginTransactionPayload;
use sddms_services::site_controller::finalize_transaction_response::FinalizeTransactionPayload;
use sddms_services::site_controller::register_client_response::RegisterClientPayload;
use sddms_services::site_controller::site_manager_service_client::SiteManagerServiceClient;
use sddms_shared::error::SddmsError;
use sddms_shared::sql_metadata::TransactionStmt;
use crate::query_results::{QueryResults, ResultsInfo};

pub enum FinalizeResult {
    Ok,
    Deadlock(SddmsError),
}

pub struct SddmsSiteClient {
    client: SiteManagerServiceClient<Channel>,
    client_id: Option<u32>,
}

impl SddmsSiteClient {
    fn new(inner: SiteManagerServiceClient<Channel>) -> Self {
        Self {
            client: inner,
            client_id: None
        }
    }

    pub fn set_client_id(&mut self, id: u32) {
        self.client_id = Some(id);
    }

    #[inline]
    fn client_id(&self) -> u32 {
        self.client_id.unwrap()
    }

    pub async fn connect<ConnStrT: Into<String>>(conn_str: ConnStrT) -> Result<Self, SddmsError> {
        let conn_str = conn_str.into();
        let client = SiteManagerServiceClient::connect(format!("http://{}", conn_str))
            .await
            .map_err(|err| SddmsError::client("Failed to connect to site controller").with_cause(err))?;

        Ok(Self::new(client))
    }

    pub async fn register_self(&mut self) -> Result<u32, SddmsError> {
        let request = RegisterClientRequest {
            host: "".to_string(),
            port: 0,
        };

        let response = self.client.register_client(request)
            .await
            .map_err(|err| SddmsError::client("Failed to connect to site controller").with_cause(err))
            ?.into_inner();

        match response.register_client_payload.unwrap() {
            RegisterClientPayload::Error(err) => {
                Err(err.into())
            }
            RegisterClientPayload::Results(results) => {
                Ok(results.client_id)
            }
        }
    }

    pub async fn begin_transaction(&mut self) -> Result<u32, SddmsError> {
        let request = BeginTransactionRequest {
            transaction_name: None,
            client_id: self.client_id()
        };
        let response = self.client.begin_transaction(request).await
            .map_err(|err| SddmsError::client("Failed to invoke begin transaction request").with_cause(err))?;

        let response = response.into_inner();
        match response.begin_transaction_payload.unwrap() {
            BeginTransactionPayload::Error(api_err) => {
                let cause: SddmsError = api_err.into();
                Err(SddmsError::client("Error occurred while beginning transaction").with_cause(cause))
            }
            BeginTransactionPayload::Value(results) => {
                Ok(results.transaction_id)
            }
        }
    }

    pub async fn invoke_query(&mut self, trans_id: Option<u32>, query: &str) -> Result<QueryResults, SddmsError> {
        let request = self.configure_request(trans_id, query)?;
        let response = self.client.invoke_query(request).await
            .map_err(|status| SddmsError::client(format!("Error while sending request: {} {}", status.code(), status.message())))?;

        let invoke_response = response.into_inner();
        let ret = invoke_response.ret().clone();
        let result = match invoke_response.invoke_query_payload.unwrap() {
            InvokeQueryPayload::Error(api_error) => {
                if let ReturnStatus::Deadlocked = ret {
                    Ok(QueryResults::DeadLock(api_error.into()))
                } else {
                    let sddms_err_cause: SddmsError = api_error.into();
                    Err(SddmsError::client("Failed to invoke query")
                        .with_cause(sddms_err_cause))
                }
            }
            InvokeQueryPayload::Results(query_results) => {
                let results = if let Some(affected_records) = query_results.affected_records {
                    QueryResults::AffectedRows(affected_records)
                } else if let Some(payload) = query_results.data_payload {
                    let objects: Vec<Map<String, Value>> = serde_json::from_slice(&payload)
                        .map_err(|err| SddmsError::general("Could not deserialize query result").with_cause(err))?;
                    QueryResults::Results(ResultsInfo {
                        results: objects,
                        columns: query_results.column_names
                    })
                } else {
                    panic!("Nothing was specified")
                };
                Ok(results)
            }
        };

        result
    }

    pub async fn finalize_transaction(&mut self, id: u32, mode: TransactionStmt) -> Result<(), SddmsError> {
        let finalize_mode = FinalizeMode::try_from(mode).unwrap();
        let mut request = FinalizeTransactionRequest {
            mode: 0,
            transaction_id: id,
            client_id: self.client_id()
        };
        request.set_mode(finalize_mode);
        
        let response = self.client.finalize_transaction(request).await
            .map_err(|status| SddmsError::client(format!("Error while sending request: {} {}", status.code(), status.message())))?;
        
        let results = response.into_inner();
        match results.finalize_transaction_payload.unwrap() {
            FinalizeTransactionPayload::Error(err) => {
                let sddms_err_cause: SddmsError = err.into();
                Err(SddmsError::client("Failed to finalize transaction")
                    .with_cause(sddms_err_cause))
            }
            FinalizeTransactionPayload::Results(_) => {
                Ok(())
            }
        }
    }

    fn configure_request(&self, trans_id: Option<u32>, query: &str) -> Result<InvokeQueryRequest, SddmsError> {
        let sql_statements = sddms_shared::sql_metadata::parse_statements(query)
            .map_err(|err| SddmsError::client("Failed to parse SQL query").with_cause(err))?;

        if sql_statements.len() != 1 {
            panic!("Got {} statements, which is too many", sql_statements.len())
        }

        let metadata = sql_statements.get(0).unwrap();
        let (read_set, write_set) = if metadata.modifiable() {
            (Vec::new(), Vec::from_iter(metadata.tables().iter().cloned()))
        } else {
            (Vec::from_iter(metadata.tables().iter().cloned()), Vec::new())
        };

        let single_stmt_trans = trans_id.is_none();

        Ok(InvokeQueryRequest {
            transaction_id: trans_id.unwrap_or_default(),
            query: String::from(query),
            has_results: metadata.has_results(),
            read_set,
            write_set,
            single_stmt_transaction: single_stmt_trans,
            client_id: self.client_id(),
        })
    }
}
