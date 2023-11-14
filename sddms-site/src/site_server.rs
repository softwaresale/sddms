use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use log::{debug, error, info};
use sqlite::{Connection, ConnectionThreadSafe};
use tokio::sync::MutexGuard;
use tonic::{Request, Response, Status};
use sddms_services::shared::{ApiError, FinalizeMode, ReturnStatus};
use sddms_services::site_controller::{BeginTransactionRequest, BeginTransactionResponse, BeginTransactionResults, FinalizeTransactionRequest, FinalizeTransactionResponse, FinalizeTransactionResults, InvokeQueryRequest, InvokeQueryResponse, InvokeQueryResults, RegisterClientRequest, RegisterClientResponse, RegisterClientResults, ReplicationUpdateRequest, ReplicationUpdateResponse};
use sddms_services::site_controller::begin_transaction_response::BeginTransactionPayload;
use sddms_services::site_controller::finalize_transaction_response::FinalizeTransactionPayload;
use sddms_services::site_controller::invoke_query_response::InvokeQueryPayload;
use sddms_services::site_controller::register_client_response::RegisterClientPayload;
use sddms_services::site_controller::site_manager_service_server::SiteManagerService;
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::central_client::CentralClient;
use crate::client_connection_map::ClientConnectionMap;
use crate::sqlite_row_serializer::serialize_row;

pub struct SddmsSiteManagerService {
    db_path: PathBuf,
    client_connections: tokio::sync::Mutex<ClientConnectionMap>,
    cc_client: tokio::sync::Mutex<CentralClient>,
    site_id: u32,
}

impl SddmsSiteManagerService {
    pub fn new(path: &Path, cc_client: CentralClient, site_id: u32) -> Self {
        Self {
            db_path: PathBuf::from(path),
            client_connections: tokio::sync::Mutex::new(ClientConnectionMap::new()),
            cc_client: tokio::sync::Mutex::new(cc_client),
            site_id
        }
    }

    async fn register_transaction_with_cc(&self) -> Result<u32, BeginTransactionResponse> {

        let result = {
            let mut cc_client = self.cc_client.lock().await;
            cc_client.register_transaction(self.site_id)
                .await
        };

        match result {
            Ok(trans_id) => { Ok(trans_id) }
            Err(err) => {
                let payload = BeginTransactionPayload::Error(err.into());
                let mut response = BeginTransactionResponse::default();
                response.set_ret(ReturnStatus::Error);
                response.begin_transaction_payload = Some(payload);
                Err(response)
            }
        }
    }

    async fn acquire_table_lock(&self, trans_id: u32, table: &str) -> Result<(), InvokeQueryResponse> {
        let mut cc_client = self.cc_client.lock().await;
        cc_client.acquire_table_lock(self.site_id, trans_id, table)
            .await
            .map_err(|err| {
                error!("Error while trying to acquire lock: {}", err);
                let payload = InvokeQueryPayload::Error(err.into());
                let mut response = InvokeQueryResponse {
                    invoke_query_payload: Some(payload),
                    ret: 0
                };
                response.set_ret(ReturnStatus::Error);
                response
            })
    }

    async fn finalize_transaction(&self, trans_id: u32, mode: FinalizeMode) -> Result<(), FinalizeTransactionResponse> {
        let mut cc_client = self.cc_client.lock().await;
        cc_client.finalize_transaction(self.site_id, trans_id, mode)
            .await
            .map_err(|err| {
                error!("Error while finalizing transaction: {}", err);
                let payload = FinalizeTransactionPayload::Error(err.into());
                let mut response = FinalizeTransactionResponse {
                    ret: 0,
                    finalize_transaction_payload: Some(payload),
                };
                response.set_ret(ReturnStatus::Error);

                response
            })
    }
}

impl Debug for SddmsSiteManagerService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("SddmsSiteServer {{ {}, #connection }}", self.db_path.to_str().unwrap()))
    }
}

#[tonic::async_trait]
impl SiteManagerService for SddmsSiteManagerService {
    async fn register_client(&self, _request: Request<RegisterClientRequest>) -> Result<Response<RegisterClientResponse>, Status> {
        info!("Registering new client");

        let mut connection_map = self.client_connections.lock().await;
        let result = connection_map.open_connection(&self.db_path);

        let (ret, payload) = match result {
            Ok(client_id) => {
                info!("Successfully registered new client with id {}", client_id);
                let payload = RegisterClientPayload::Results(RegisterClientResults{ client_id });
                (ReturnStatus::Ok, payload)
            }
            Err(err) => {
                error!("Error while registering client: {}", err);
                let payload = RegisterClientPayload::Error(err.into());
                    (ReturnStatus::Error, payload)
            }
        };

        let mut response = RegisterClientResponse {
            ret: 0,
            register_client_payload: Some(payload),
        };
        response.set_ret(ret);

        Ok(Response::new(response))
    }

    async fn begin_transaction(&self, request: Request<BeginTransactionRequest>) -> Result<Response<BeginTransactionResponse>, Status> {
        info!("Got begin transaction request: {:?}", request.remote_addr());
        let begin_trans_request = request.into_inner();
        let client_id = begin_trans_request.client_id;
        let register_trans_result = self.register_transaction_with_cc().await;
        let Ok(trans_id) = register_trans_result else {
            return Ok(Response::new(register_trans_result.unwrap_err()))
        };

        // get the connection for the given client
        let connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();

        let begin_trans_result = client_connection.invoke_one_off_stmt("BEGIN TRANSACTION");
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
        let mut response = BeginTransactionResponse::default();
        response.set_ret(ReturnStatus::Ok);
        response.begin_transaction_payload = Some(BeginTransactionPayload::Value(BeginTransactionResults { transaction_id: trans_id }));
        info!("Successfully registered transaction {}", trans_id);

        Ok(Response::new(response))
    }

    async fn invoke_query(&self, request: Request<InvokeQueryRequest>) -> Result<Response<InvokeQueryResponse>, Status> {
        info!("Got invoke query request: {:?}", request.remote_addr());
        let invoke_request = request.into_inner();
        debug!("Got query: {}", invoke_request.query);
        let client_id = invoke_request.client_id;

        // only acquire locks if in a transaction
        if !invoke_request.single_stmt_transaction {
            // first, try acquiring the lock
            debug!("Acquiring lock for {:?}...", invoke_request.write_set);
            for tab in &invoke_request.write_set {
                let lock_result = self.acquire_table_lock(invoke_request.transaction_id, tab).await;
                if let Err(lock_err) = lock_result {
                    return Ok(Response::new(lock_err))
                }
            }
            debug!("Successfully acquired lock");
        } else {
            debug!("Single transaction is running, skipping lock acquiring phase")
        }

        // get the connection for the given client
        let connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();

        let invoke_results = if invoke_request.has_results {
            client_connection.invoke_read_query(&invoke_request.query)
        } else {
            client_connection.invoke_modify_query(&invoke_request.query)
        }
            .map_err(|err| ApiError::from(err));

        let (ret, payload) = match invoke_results {
            Ok(results) => {
                (ReturnStatus::Ok, InvokeQueryPayload::Results(results))
            }
            Err(err) => {
                (ReturnStatus::Error, InvokeQueryPayload::Error(err))
            }
        };
        let mut response = InvokeQueryResponse::default();
        response.set_ret(ret);
        response.invoke_query_payload = Some(payload);
        info!("Successfully invoked query");

        Ok(Response::new(response))
    }

    async fn finalize_transaction(&self, request: Request<FinalizeTransactionRequest>) -> Result<Response<FinalizeTransactionResponse>, Status> {
        info!("Got finalize transaction: {:?}", request.remote_addr());
        let finalize_request = request.into_inner();
        let client_id = finalize_request.client_id;
        info!("Finalizing transaction {} with mode {:?}", finalize_request.transaction_id, finalize_request.mode());
        let finalize_query = match finalize_request.mode() {
            FinalizeMode::Unspecified => panic!("Unspecified commit method"),
            FinalizeMode::Commit => {
                "COMMIT"
            }
            FinalizeMode::Abort => {
                "ROLLBACK"
            }
        };

        // get the connection for the given client
        let connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();

        let result = client_connection.invoke_one_off_stmt(finalize_query);
        if let Err(err) = result {
            let api_error: ApiError = SddmsError::site("Failed to finalize transaction")
                .with_cause(err)
                .into();

            let mut response = FinalizeTransactionResponse::default();
            response.set_ret(ReturnStatus::Error);
            response.finalize_transaction_payload = Some(FinalizeTransactionPayload::Error(api_error));
            return Ok(Response::new(response));
        }

        // finalize the transaction in the CC
        let finalize_result = self.finalize_transaction(finalize_request.transaction_id, finalize_request.mode()).await;
        let response = match finalize_result {
            Ok(_) => {
                let mut response = FinalizeTransactionResponse::default();
                response.finalize_transaction_payload = Some(FinalizeTransactionPayload::Results(FinalizeTransactionResults::default()));
                info!("Successfully finalized transaction");
                response
            }
            Err(err_response) => {
                err_response
            }
        };

        Ok(Response::new(response))
    }

    async fn replication_update(&self, request: Request<ReplicationUpdateRequest>) -> Result<Response<ReplicationUpdateResponse>, Status> {
        todo!()
    }
}
