use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use log::{debug, error, info};
use rusqlite::Connection;
use tonic::{Request, Response, Status};
use sddms_services::shared::{ApiError, FinalizeMode, LockMode, LockRequest, ReturnStatus};
use sddms_services::site_controller::{BeginTransactionRequest, BeginTransactionResponse, BeginTransactionResults, FinalizeTransactionRequest, FinalizeTransactionResponse, FinalizeTransactionResults, InvokeQueryRequest, InvokeQueryResponse, InvokeQueryResults, RegisterClientRequest, RegisterClientResponse, RegisterClientResults, ReplicationUpdateRequest, ReplicationUpdateResponse};
use sddms_services::site_controller::begin_transaction_response::BeginTransactionPayload;
use sddms_services::site_controller::finalize_transaction_response::FinalizeTransactionPayload;
use sddms_services::site_controller::invoke_query_response::InvokeQueryPayload;
use sddms_services::site_controller::register_client_response::RegisterClientPayload;
use sddms_services::site_controller::site_manager_service_server::SiteManagerService;
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::central_client::{AcquireLockRet, CentralClient};
use crate::client_connection::{ClientConnectionMap};
use crate::history_logger::HistoryLogger;
use crate::transaction_history::{TransactionHistoryMap};

pub struct SddmsSiteManagerService {
    db_path: PathBuf,
    // TODO make this a RW lock -- 80% of time we're reading, and underlying connections
    // are managed by mutexes as well
    client_connections: tokio::sync::Mutex<ClientConnectionMap>,
    cc_client: CentralClient,
    transaction_history: tokio::sync::Mutex<TransactionHistoryMap>,
    site_id: u32,
    history_logger: tokio::sync::Mutex<Box<dyn HistoryLogger>>,
}

impl SddmsSiteManagerService {
    pub fn new<LoggerT: Into<Box<dyn HistoryLogger>>>(path: &Path, cc_client: CentralClient, site_id: u32, logger: LoggerT) -> Self {
        Self {
            db_path: PathBuf::from(path),
            client_connections: tokio::sync::Mutex::new(ClientConnectionMap::new()),
            cc_client,
            transaction_history: tokio::sync::Mutex::default(),
            site_id,
            history_logger: tokio::sync::Mutex::new(logger.into()),
        }
    }

    async fn register_transaction_with_cc(&self) -> Result<u32, BeginTransactionResponse> {

        self.cc_client.register_transaction(self.site_id).await
            .map_err(|err| err.into())
    }

    async fn acquire_locks_for_txn(&self, trans_id: u32, read_set: &[String], write_set: &[String]) -> Result<(), InvokeQueryResponse> {
        let lock_requests = {
            let mut lock_requests = read_set.into_iter()
                .map(|table| LockRequest::new(table, LockMode::Shared))
                .collect::<Vec<_>>();

            write_set.into_iter()
                .map(|table| LockRequest::new(table, LockMode::Exclusive))
                .for_each(|request| lock_requests.push(request));

            lock_requests
        };

        info!("Acquiring locks: {:?}", lock_requests);

        let lock_result = self.cc_client.acquire_table_lock(self.site_id, trans_id, lock_requests.clone())
            .await
            .map_err(|err| {
                error!("Error while trying to acquire lock: {}", err);
                InvokeQueryResponse::from(err)
            })?;

        match lock_result {
            AcquireLockRet::Ok => {
                info!("Successfully acquired locks: {:?}", lock_requests);
                Ok(())
            }
            AcquireLockRet::Deadlock(deadlock_err) => {
                let mut response = InvokeQueryResponse::from(deadlock_err);
                response.set_ret(ReturnStatus::Deadlocked);
                Err(response)
            }
        }
    }

    async fn acquire_table_lock(&self, trans_id: u32, lock_requests: Vec<LockRequest>) -> Result<AcquireLockRet, InvokeQueryResponse> {
        self.cc_client.acquire_table_lock(self.site_id, trans_id, lock_requests)
            .await
            .map_err(|err| {
                error!("Error while trying to acquire lock: {}", err);
                err.into()
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

    async fn execute_query_on_db(&self, client_id: u32, transaction_id: u32, invoke_request: &InvokeQueryRequest) -> Result<InvokeQueryResults, SddmsTermError> {
        // get the connection for the given client
        let connection_map_lock = self.client_connections.lock().await;
        let client_connection = connection_map_lock
            .get_client_connection(client_id)
            .unwrap();

        if invoke_request.has_results {
            client_connection.invoke_read_query(&invoke_request.query).await
                .map_err(|err| SddmsTermError::from(err))
        } else {
            debug!("Saving update command from client_id={}, trans_id={}: {}", client_id, transaction_id, &invoke_request.query);
            let invoke_result = client_connection.invoke_modify_query(&invoke_request.query).await;
            match invoke_result {
                Ok(query_result) => {
                    self.push_update_command(client_id, transaction_id, &invoke_request.query).await;
                    Ok(query_result)
                }
                Err(sddms_error) => {
                    Err(sddms_error)
                }
            }
        }
    }

    async fn replicate_local_transaction(&self, client_connection_map: &mut ClientConnectionMap, client_id: u32, stmts: &[String]) -> Result<(), SddmsTermError> {
        // apply it to the local database
        self.replicate_on_disk(stmts).await?;
        // apply it to the connection map
        self.replicate_to_clients(client_connection_map, stmts, Some(client_id)).await
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

    async fn replicate_to_clients(&self, connection_map: &mut ClientConnectionMap, stmts: &[String], skip: Option<u32>) -> Result<(), SddmsTermError> {
        connection_map.replicate_messages(stmts, skip).await
            .map_err(|err| SddmsTermError::from(err))
    }

    async fn provision_single_stmt_transaction(&self) -> Result<u32, InvokeQueryResponse> {
        self.cc_client.register_transaction(self.site_id)
            .await
            .map_err(|err| {
                error!("Failed to register temporary transaction: {}", err);
                err.into()
            })
    }

    async fn replicate_and_finalize(&self, client_id: u32, trans_id: u32, mode: FinalizeMode) -> Result<(), SddmsTermError> {
        // Get the history of what to replicate
        let mut history = self.transaction_history.lock().await;
        let transaction_history = history.remove_transaction(client_id, trans_id).unwrap();

        // replicate locally if commit
        if let FinalizeMode::Commit = mode {
            debug!("Replicating to local transactions...");
            let mut client_connections = self.client_connections.lock().await;
            self.replicate_local_transaction(&mut client_connections, client_id, &transaction_history).await?;
            debug!("Replicated local transaction");
        }

        // finalize with concurrency controller
        debug!("Finalizing transaction with CC...");
        self.cc_client.finalize_transaction(self.site_id, trans_id, mode, &transaction_history).await?;
        debug!("Transaction finalized with CC");

        Ok(())
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
            return Ok(Response::new(BeginTransactionResponse::from(err)));
        }
        let mut response = BeginTransactionResponse::default();
        response.set_ret(ReturnStatus::Ok);
        response.begin_transaction_payload = Some(BeginTransactionPayload::Value(BeginTransactionResults { transaction_id: trans_id }));
        info!("Successfully registered transaction {}", trans_id);

        // TODO make this not fail by default?
        self.history_logger.lock().await.log(client_id, self.site_id, trans_id, "Begin Txn")
            .unwrap();

        Ok(Response::new(response))
    }

    async fn invoke_query(&self, request: Request<InvokeQueryRequest>) -> Result<Response<InvokeQueryResponse>, Status> {
        info!("Got invoke query request: {:?}", request.remote_addr());
        let invoke_request = request.into_inner();
        debug!("Got query: {}", invoke_request.query);
        let client_id = invoke_request.client_id;

        // only acquire locks if in a transaction
        let transaction_id = if invoke_request.single_stmt_transaction {
            info!("Provisioning transaction for single stmt");
            let result = self.provision_single_stmt_transaction().await;
            match result {
                Ok(id) => {
                    info!("Provisioned temporary transaction with id {}", id);
                    self.push_transaction_for_client(client_id, id).await;
                    id
                }
                Err(response) => {
                    return Ok(Response::new(response))
                }
            }
        } else {
            invoke_request.transaction_id
        };

        // try acquiring the lock
        debug!("Acquiring lock(s) for {:?}...", invoke_request.write_set);

        // attempt acquiring all locks necessary
        let lock_requests_result = self.acquire_locks_for_txn(transaction_id, &invoke_request.read_set, &invoke_request.write_set).await;
        match lock_requests_result {
            Ok(_) => {
                debug!("Successfully acquired lock");
            }
            Err(err_response) => {
                return Ok(Response::new(err_response))
            }
        }

        // actually execute the results
        let invoke_results = self.execute_query_on_db(client_id, transaction_id, &invoke_request).await;
        // check for failure and return if it did
        if let Err(err) = invoke_results {
            let response = InvokeQueryResponse::from(err);
            return Ok(Response::new(response));
        }

        let results = invoke_results.unwrap();

        // finalize the transaction as well
        let (ret, payload) = if invoke_request.single_stmt_transaction {
            let replication_result = self.replicate_and_finalize(client_id, transaction_id, FinalizeMode::Commit)
                .await;

            match replication_result {
                Ok(_) => {
                    (ReturnStatus::Ok, InvokeQueryPayload::Results(results))
                }
                Err(err) => {
                    (ReturnStatus::Error, InvokeQueryPayload::Error(ApiError::from(err)))
                }
            }

        } else {
            (ReturnStatus::Ok, InvokeQueryPayload::Results(results))
        };

        let mut response = InvokeQueryResponse::default();
        response.set_ret(ret);
        response.invoke_query_payload = Some(payload);
        info!("Successfully invoked query");

        self.history_logger.lock().await.log_query(client_id, self.site_id, transaction_id, &invoke_request.write_set, &invoke_request.read_set)
            .unwrap();

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
        {
            let connection_map_lock = self.client_connections.lock().await;
            let client_connection = connection_map_lock
                .get_client_connection(client_id)
                .unwrap();
            debug!("Acquired");

            debug!("Invoking query finalization statement...");
            let result = client_connection.invoke_one_off_stmt(finalize_query).await;
            if let Err(err) = result {
                error!("Error while finalizing transaction query: {}", err);
                let response = FinalizeTransactionResponse::from(err);
                return Ok(Response::new(response));
            }
            debug!("Invoked");
        }

        self.history_logger.lock().await.log(client_id, self.site_id, finalize_request.transaction_id, finalize_query)
            .unwrap();

        debug!("Starting to replicate and finalize...");
        let result = self.replicate_and_finalize(client_id, finalize_request.transaction_id, finalize_request.mode()).await;
        let (ret, payload) = match result {
            Ok(_) => {
                info!("Transaction successfully replicated and finalized");
                (ReturnStatus::Ok, FinalizeTransactionPayload::Results(FinalizeTransactionResults {}))
            }
            Err(err) => {
                error!("Error while finalizing and replicating transaction: {}", err);
                (ReturnStatus::Error, FinalizeTransactionPayload::Error(ApiError::from(err)))
            }
        };

        let mut response = FinalizeTransactionResponse::default();
        response.set_ret(ret);
        response.finalize_transaction_payload = Some(payload);

        Ok(Response::new(response))
    }

    async fn replication_update(&self, request: Request<ReplicationUpdateRequest>) -> Result<Response<ReplicationUpdateResponse>, Status> {
        info!("Got replication request");
        let replicate_update_request = request.into_inner();
        let mut connections = self.client_connections.lock().await;
        let replication_error = self.replicate_to_clients(&mut connections, &replicate_update_request.update_statements, None)
            .await
            .err();

        if let Some(error) = replication_error {
            error!("Error occurred while replicating transaction to clients: {}", error);
            let response = ReplicationUpdateResponse::from(error);
            return Ok(Response::new(response));
        }

        let disk_replication_err = self.replicate_on_disk(&replicate_update_request.update_statements)
            .await
            .err();


        let response = if disk_replication_err.is_some() {
            let err = disk_replication_err.unwrap();
            error!("Error while performing replication request: {}", err);
            ReplicationUpdateResponse::from(err)
        } else {
            info!("Successfully replicated database on site");
            let mut response = ReplicationUpdateResponse::default();
            response.set_ret(ReturnStatus::Ok);
            response.error = None;

            self.history_logger.lock().await.log_replication(replicate_update_request.originating_site, &replicate_update_request.update_statements)
                .unwrap();

            response
        };

        Ok(Response::new(response))
    }
}
