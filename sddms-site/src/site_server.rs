use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use log::{debug, error, info};
use rusqlite::Connection;
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
use crate::client_connection::{ClientConnectionMap};
use crate::transaction_history::{TransactionHistoryMap};

pub struct SddmsSiteManagerService {
    db_path: PathBuf,
    // TODO make this a RW lock -- 80% of time we're reading, and underlying connections
    // are managed by mutexes as well
    client_connections: tokio::sync::Mutex<ClientConnectionMap>,
    cc_client: CentralClient,
    transaction_history: tokio::sync::Mutex<TransactionHistoryMap>,
    site_id: u32,
}

impl SddmsSiteManagerService {
    pub fn new(path: &Path, cc_client: CentralClient, site_id: u32) -> Self {
        Self {
            db_path: PathBuf::from(path),
            client_connections: tokio::sync::Mutex::new(ClientConnectionMap::new()),
            cc_client,
            transaction_history: tokio::sync::Mutex::default(),
            site_id
        }
    }

    async fn register_transaction_with_cc(&self) -> Result<u32, BeginTransactionResponse> {

        let result = self.cc_client.register_transaction(self.site_id).await;

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
        self.cc_client.acquire_table_lock(self.site_id, trans_id, table)
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
        // TODO can't decide this
        self.cc_client.finalize_transaction(self.site_id, trans_id, mode)
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

    async fn push_transaction_for_client(&self, client_id: u32, trans_id: u32) {
        let mut transaction_history = self.transaction_history.lock().await;
        transaction_history.push_transaction(client_id, trans_id)
    }

    async fn push_update_command(&self, client_id: u32, trans_id: u32, cmd: &str) {
        let mut transaction_history = self.transaction_history.lock().await;
        transaction_history.get_transaction_for_client_mut(client_id, trans_id).unwrap()
            .push(cmd)
    }

    async fn execute_query_on_db(&self, client_id: u32, invoke_request: &InvokeQueryRequest) -> Result<InvokeQueryResults, ApiError> {
        // get the connection for the given client
        let connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();

        if invoke_request.has_results {
            client_connection.invoke_read_query(&invoke_request.query).await
        } else {
            debug!("Saving update command from client_id={}, trans_id={}: {}", client_id, invoke_request.transaction_id, &invoke_request.query);
            self.push_update_command(client_id, invoke_request.transaction_id, &invoke_request.query).await;
            client_connection.invoke_modify_query(&invoke_request.query).await
        }
            .map_err(|err| ApiError::from(err))
    }

    async fn replicate_local_transaction(&self, client_connection_map: &mut ClientConnectionMap, client_id: u32, transaction_id: u32) -> Result<(), SddmsTermError> {
        // get the transaction history
        let transaction_history = {
            self.transaction_history.lock().await.remove_transaction(client_id, transaction_id)
                .ok_or(SddmsError::site(format!("No transaction for client_id={}, transaction_id={}", client_id, transaction_id)).into())
                .map_err(|err: SddmsError| SddmsTermError::from(err))?
        };

        // apply it to the local database
        self.replicate_on_disk(&transaction_history).await?;

        // apply it to the connection map
        self.replicate_remote_transaction(client_connection_map, &transaction_history, Some(client_id)).await
    }

    async fn replicate_on_disk(&self, stmts: &[String]) -> Result<(), SddmsTermError> {
        let mut disk_connection = Connection::open(&self.db_path)
            .map_err(|err| SddmsError::site("Failed to open disk database").with_cause(err))?;

        let transaction = disk_connection.transaction()
            .map_err(|err| SddmsError::site("Failed to open replication txn on disk").with_cause(err))?;

        for stmt in stmts {
            transaction.execute(stmt, [])
                .map_err(|err| SddmsError::site("Failed to execute update stmt").with_cause(err))?;
        }

        transaction.commit()
            .map_err(|err| SddmsError::site("Failed to commit replication transaction on disk").with_cause(err))
            .map_err(|err| SddmsTermError::from(err))
    }

    async fn replicate_remote_transaction(&self, connection_map: &mut ClientConnectionMap, stmts: &[String], skip: Option<u32>) -> Result<(), SddmsTermError> {
        connection_map.replicate_messages(stmts, skip).await
            .map_err(|err| SddmsTermError::from(err))
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

        // register that we are starting a new transaction
        self.push_transaction_for_client(client_id, trans_id).await;

        // get the connection for the given client
        let connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();

        let begin_trans_result = client_connection.invoke_one_off_stmt("BEGIN TRANSACTION").await;
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

        // actually execute the results
        let invoke_results = self.execute_query_on_db(client_id, &invoke_request).await;

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
        debug!("Acquiring connection pool lock...");
        let mut connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();
        debug!("Acquired");

        debug!("Invoking query finalization statement...");
        let result = client_connection.invoke_one_off_stmt(finalize_query).await;
        if let Err(err) = result {
            error!("Error while finalizing transaction query: {}", err);
            let api_error: ApiError = SddmsError::site("Failed to finalize transaction")
                .with_cause(err)
                .into();

            let mut response = FinalizeTransactionResponse::default();
            response.set_ret(ReturnStatus::Error);
            response.finalize_transaction_payload = Some(FinalizeTransactionPayload::Error(api_error));
            return Ok(Response::new(response));
        }
        debug!("Invoked");

        // replicate the transaction locally if commit, do nothing if abort
        if let FinalizeMode::Commit = finalize_request.mode() {
            let replicate_result = self.replicate_local_transaction(&mut connection_map_lock, client_id, finalize_request.transaction_id).await;
            if let Err(err) = replicate_result {
                error!("Error while replicating query: {}", err);
                let api_error: ApiError = SddmsError::site("Failed to finalize transaction")
                    .with_cause(err)
                    .into();

                let mut response = FinalizeTransactionResponse::default();
                response.set_ret(ReturnStatus::Error);
                response.finalize_transaction_payload = Some(FinalizeTransactionPayload::Error(api_error));
                return Ok(Response::new(response));
            }
        }

        // finalize the transaction in the CC
        debug!("Finalizing transaction with CC...");
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
        info!("Got finalize transaction: {:?}", request.remote_addr());
        let replicate_update_request = request.into_inner();
        todo!()
    }
}
