use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use log::{debug, info};
use sqlite::{Connection, ConnectionThreadSafe};
use tonic::{Request, Response, Status};
use sddms_services::shared::{ApiError, FinalizeMode, ReturnStatus};
use sddms_services::site_controller::{BeginTransactionRequest, BeginTransactionResponse, BeginTransactionResults, FinalizeTransactionRequest, FinalizeTransactionResponse, FinalizeTransactionResults, InvokeQueryRequest, InvokeQueryResponse, InvokeQueryResults, ReplicationUpdateRequest, ReplicationUpdateResponse};
use sddms_services::site_controller::begin_transaction_response::BeginTransactionPayload;
use sddms_services::site_controller::finalize_transaction_response::FinalizeTransactionPayload;
use sddms_services::site_controller::invoke_query_response::InvokeQueryPayload;
use sddms_services::site_controller::site_manager_service_server::SiteManagerService;
use sddms_shared::error::SddmsError;
use crate::sqlite_row_serializer::serialize_row;

pub struct SddmsSiteManagerService {
    db_path: PathBuf,
    connection: ConnectionThreadSafe,
    trans_id: Arc<AtomicU32>,
}

impl SddmsSiteManagerService {
    pub fn new(path: &Path, db_conn: ConnectionThreadSafe) -> Self {
        Self {
            db_path: PathBuf::from(path),
            connection: db_conn,
            trans_id: Arc::new(AtomicU32::new(0))
        }
    }

    fn invoke_read_query(&self, query_text: &str) -> Result<InvokeQueryResults, SddmsError> {

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

    fn invoke_modify_query(&self, query_text: &str) -> Result<InvokeQueryResults, SddmsError> {
        let mut results = InvokeQueryResults::default();
        self.connection.execute(query_text)
            .map_err(|err| SddmsError::general("Failed to invoke SQL query").with_cause(err))?;

        let affected_rows = self.connection.change_count() as u32;
        results.affected_records = Some(affected_rows);
        info!("Updated {} rows", affected_rows);
        Ok(results)
    }

    fn invoke_one_off_stmt(&self, query_text: &str) -> Result<(), SddmsError> {
        self.connection.execute(query_text)
            .map_err(|err| SddmsError::general("Failed to execute one off SQL statement").with_cause(err))
    }
}

impl Clone for SddmsSiteManagerService {
    fn clone(&self) -> Self {
        let new_connection = Connection::open_thread_safe(&self.db_path).unwrap();
        Self {
            db_path: self.db_path.clone(),
            connection: new_connection,
            trans_id: self.trans_id.clone(),
        }
    }
}

impl Debug for SddmsSiteManagerService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("SddmsSiteServer {{ {}, #connection }}", self.db_path.to_str().unwrap()))
    }
}

#[tonic::async_trait]
impl SiteManagerService for SddmsSiteManagerService {
    async fn begin_transaction(&self, request: Request<BeginTransactionRequest>) -> Result<Response<BeginTransactionResponse>, Status> {
        info!("Got begin transaction request: {:?}", request.remote_addr());
        let begin_trans_result = self.invoke_one_off_stmt("BEGIN TRANSACTION");
        if begin_trans_result.is_err() {
            let err = begin_trans_result.unwrap_err();
            let api_error: ApiError = SddmsError::site("Failed to begin transaction")
                .with_cause(err)
                .into();

            let mut response = BeginTransactionResponse::default();
            response.set_ret(ReturnStatus::Error);
            response.begin_transaction_payload = Some(BeginTransactionPayload::Error(api_error));
            return Ok(Response::new(response));
        }

        let trans_id = self.trans_id.fetch_add(1, Ordering::AcqRel);
        let mut response = BeginTransactionResponse::default();
        response.set_ret(ReturnStatus::Ok);
        response.begin_transaction_payload = Some(BeginTransactionPayload::Value(BeginTransactionResults { transaction_id: trans_id }));

        Ok(Response::new(response))
    }

    async fn invoke_query(&self, request: Request<InvokeQueryRequest>) -> Result<Response<InvokeQueryResponse>, Status> {
        info!("Got invoke query request: {:?}", request.remote_addr());
        let invoke_request = request.into_inner();
        debug!("Got query: {}", invoke_request.query);

        let invoke_results = if invoke_request.has_results {
            self.invoke_read_query(&invoke_request.query)
        } else {
            self.invoke_modify_query(&invoke_request.query)
        }
            .map_err(|err| ApiError::from(err));

        let (ret, payload) = match invoke_results {
            Ok(results) => {

                let record_count = if results.affected_records.is_some() {
                    *results.affected_records.as_ref().unwrap() as usize
                } else if results.data_payload.is_some() {
                    results.data_payload.as_ref().unwrap().len()
                } else {
                    0usize
                };
                info!("{} records affected", record_count);
                (ReturnStatus::Ok, InvokeQueryPayload::Results(results))
            }
            Err(err) => {
                (ReturnStatus::Error, InvokeQueryPayload::Error(err))
            }
        };
        let mut response = InvokeQueryResponse::default();
        response.set_ret(ret);
        response.invoke_query_payload = Some(payload);

        Ok(Response::new(response))
    }

    async fn finalize_transaction(&self, request: Request<FinalizeTransactionRequest>) -> Result<Response<FinalizeTransactionResponse>, Status> {
        info!("Got finalize transaction: {:?}", request.remote_addr());
        let finalize_request = request.into_inner();
        info!("Finalized transaction {} with mode {:?}", finalize_request.transaction_id, finalize_request.mode());
        let finalize_query = match finalize_request.mode() {
            FinalizeMode::Unspecified => panic!("Unspecified commit method"),
            FinalizeMode::Commit => {
                "COMMIT"
            }
            FinalizeMode::Abort => {
                "ROLLBACK"
            }
        };
        let result = self.invoke_one_off_stmt(finalize_query);
        if result.is_err() {
            let err = result.unwrap_err();
            let api_error: ApiError = SddmsError::site("Failed to finalize transaction")
                .with_cause(err)
                .into();

            let mut response = FinalizeTransactionResponse::default();
            response.set_ret(ReturnStatus::Error);
            response.finalize_transaction_payload = Some(FinalizeTransactionPayload::Error(api_error));
            return Ok(Response::new(response));
        }
        let mut response = FinalizeTransactionResponse::default();
        response.finalize_transaction_payload = Some(FinalizeTransactionPayload::Results(FinalizeTransactionResults::default()));
        Ok(Response::new(response))
    }

    async fn replication_update(&self, request: Request<ReplicationUpdateRequest>) -> Result<Response<ReplicationUpdateResponse>, Status> {
        todo!()
    }
}
