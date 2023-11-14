use log::{error, info};
use tonic::{Request, Response, Status};
use sddms_services::central_controller::concurrency_controller_service_server::ConcurrencyControllerService;
use sddms_services::central_controller::{AcquireLockRequest, AcquireLockResponse, AcquireLockResults, FinalizeTransactionRequest, FinalizeTransactionResponse, RegisterSiteRequest, RegisterSiteResponse, RegisterSiteResults, RegisterTransactionRequest, RegisterTransactionResponse, RegisterTransactionResults, ReleaseLockRequest, ReleaseLockResponse, ReleaseLockResults};
use sddms_services::central_controller::acquire_lock_response::AcquireLockPayload;
use sddms_services::central_controller::register_site_response::RegisterSitePayload;
use sddms_services::central_controller::register_transaction_response::RegisterTransactionPayload;
use sddms_services::central_controller::release_lock_response::ReleaseLockPayload;
use sddms_services::shared::{ApiError, ReturnStatus};
use crate::connection_pool::ConnectionPool;
use crate::lock_table::LockTable;
use crate::transaction_id::{TransactionId, TransactionIdGenerator};

pub struct CentralService {
    lock_tab: LockTable,
    connections: ConnectionPool,
    trans_id_gen: TransactionIdGenerator,
}

impl CentralService {
    pub fn new() -> Self {
        Self {
            lock_tab: LockTable::new(),
            connections: ConnectionPool::new(),
            trans_id_gen: TransactionIdGenerator::new(),
        }
    }

    fn release_all_locks(&self, trans_id: TransactionId) -> Result<(), FinalizeTransactionResponse> {
        let held_resources = self.lock_tab.lock_set(&trans_id)
            .map_err(|err| {
                let mut response = FinalizeTransactionResponse::default();
                response.set_ret(ReturnStatus::Error);
                response.error = Some(err.into());
                response
            })?;

        for resource in &held_resources {
            self.lock_tab.release_lock(trans_id, resource)
                .map_err(|err| {
                    let mut response = FinalizeTransactionResponse::default();
                    response.set_ret(ReturnStatus::Error);
                    response.error = Some(err.into());
                    response
                })?;
        }

        Ok(())
    }

    fn check_has_lock(&self, trans_id: &TransactionId, record: &str) -> Option<AcquireLockResponse> {
        let result = self.lock_tab.has_resource(&trans_id, record)
            .map(|has| {
                if has {
                    let mut response = AcquireLockResponse {
                        ret: 0,
                        acquire_lock_payload: Some(AcquireLockPayload::Results(AcquireLockResults{ acquired: true })),
                    };
                    response.set_ret(ReturnStatus::Ok);
                    Some(response)
                } else {
                    None
                }
            })
            .map_err(|err| {
                let mut response = AcquireLockResponse {
                    ret: 0,
                    acquire_lock_payload: Some(AcquireLockPayload::Error(err.into())),
                };
                response.set_ret(ReturnStatus::Error);
                response
            });

        match result {
            Ok(res) => res,
            Err(err) => Some(err)
        }
    }
}

#[tonic::async_trait]
impl ConcurrencyControllerService for CentralService {
    async fn register_site(&self, request: Request<RegisterSiteRequest>) -> Result<Response<RegisterSiteResponse>, Status> {
        let register_site_request = request.into_inner();
        info!("Registering site on {}:{}", register_site_request.host, register_site_request.port);
        let site_registration = self.connections
            .register_site(&register_site_request.host, register_site_request.port as u16)
            .await
            .map_err(|err| {
                ApiError::from(err)
            });

        let response = match site_registration {
            Ok(site_id) => {
                let mut response = RegisterSiteResponse::default();
                let results = RegisterSitePayload::Results(RegisterSiteResults {
                    site_id
                });
                response.set_ret(ReturnStatus::Ok);
                response.register_site_payload = Some(results);
                info!("Successfully registered site {}:{} with id {}", register_site_request.host, register_site_request.port, site_id);
                response
            }
            Err(api_err) => {
                error!("Failed to register site: {} - {}", api_err.message, api_err.description);
                let mut response = RegisterSiteResponse::default();
                let results = RegisterSitePayload::Error(api_err);
                response.set_ret(ReturnStatus::Ok);
                response.register_site_payload = Some(results);
                response
            }
        };

        Ok(Response::new(response))
    }

    async fn register_transaction(&self, request: Request<RegisterTransactionRequest>) -> Result<Response<RegisterTransactionResponse>, Status> {
        let register_transaction_request = request.into_inner();
        info!("Registering transaction for site {}", register_transaction_request.site_id);
        let trans_id = self.trans_id_gen.next_trans_id(register_transaction_request.site_id);

        let register_transaction_result = self.lock_tab.register_transaction(trans_id)
            .map_err(|err| {
                error!("Error while registering transaction: {}", err); // TODO prob not the place for this
                let mut response = RegisterTransactionResponse::default();
                response.set_ret(ReturnStatus::Error);
                response.register_transaction_payload = Some(RegisterTransactionPayload::Error(err.into()));
                response
            });

        let Ok(()) = register_transaction_result else {
            return Ok(Response::new(register_transaction_result.unwrap_err()))
        };

        let results = RegisterTransactionResults {
            trans_id: trans_id.transaction_id,
        };
        let mut response = RegisterTransactionResponse::default();
        response.set_ret(ReturnStatus::Ok);
        response.register_transaction_payload = Some(RegisterTransactionPayload::Results(results));
        info!("Successfully registered transaction for site {} with id {}", register_transaction_request.site_id, trans_id);
        Ok(Response::new(response))
    }

    async fn acquire_lock(&self, request: Request<AcquireLockRequest>) -> Result<Response<AcquireLockResponse>, Status> {
        let acquire_lock_request = request.into_inner();
        let trans_id = TransactionId::new(acquire_lock_request.site_id, acquire_lock_request.transaction_id);
        info!("Transaction {} is trying to acquire lock for {}", trans_id, acquire_lock_request.record_name);

        if let Some(existing_lock_response) = self.check_has_lock(&trans_id, &acquire_lock_request.record_name) {
            info!("Transaction {} already has lock for {}", trans_id, acquire_lock_request.record_name);
            return Ok(Response::new(existing_lock_response))
        }

        let mut acquire_lock_response = AcquireLockResponse::default();
        let lock_result = self.lock_tab.acquire_lock(trans_id, &acquire_lock_request.record_name);
        if lock_result.is_err() {
            let err = lock_result.unwrap_err();
            error!("Error while trying to acquire lock: {}", err);
            acquire_lock_response.set_ret(ReturnStatus::Error);
            let payload = AcquireLockPayload::Error(err.into());
            acquire_lock_response.acquire_lock_payload = Some(payload);
            return Ok(Response::new(acquire_lock_response));
        }

        acquire_lock_response.set_ret(ReturnStatus::Ok);
        acquire_lock_response.acquire_lock_payload = Some(AcquireLockPayload::Results(AcquireLockResults { acquired: true }));
        info!("{} successfully locked {}", trans_id, acquire_lock_request.record_name);
        Ok(Response::new(acquire_lock_response))
    }

    async fn release_lock(&self, request: Request<ReleaseLockRequest>) -> Result<Response<ReleaseLockResponse>, Status> {
        let release_lock_request = request.into_inner();
        let trans_id = TransactionId::new(release_lock_request.site_id, release_lock_request.transaction_id);
        info!("Transaction {} is releasing lock for {}", trans_id, release_lock_request.record_name);

        let mut release_lock_response = ReleaseLockResponse::default();
        let lock_result = self.lock_tab.release_lock(trans_id, &release_lock_request.record_name);
        if lock_result.is_err() {
            let err = lock_result.unwrap_err();
            error!("Error while trying to release lock: {}", err);
            release_lock_response.set_ret(ReturnStatus::Error);
            let payload = ReleaseLockPayload::Error(err.into());
            release_lock_response.release_lock_payload = Some(payload);
            return Ok(Response::new(release_lock_response));
        }

        release_lock_response.set_ret(ReturnStatus::Ok);
        release_lock_response.release_lock_payload = Some(ReleaseLockPayload::Results(ReleaseLockResults { released: true }));
        info!("{} released lock for {}", trans_id, release_lock_request.record_name);
        Ok(Response::new(release_lock_response))
    }

    async fn finalize_transaction(&self, request: Request<FinalizeTransactionRequest>) -> Result<Response<FinalizeTransactionResponse>, Status> {
        let finalize_request = request.into_inner();
        let trans_id = TransactionId::new(finalize_request.site_id, finalize_request.transaction_id);
        info!("Transaction {} is finalizing itself", trans_id);

        // Release all locks that this transaction currently holds
        if let Err(unlock_err) = self.release_all_locks(trans_id) {
            return Ok(Response::new(unlock_err))
        }

        // finalize the transaction
        let finalize_result = self.lock_tab.finalize_transaction(trans_id);
        match finalize_result {
            Ok(_) => {
                let mut response = FinalizeTransactionResponse::default();
                response.set_ret(ReturnStatus::Ok);
                info!("Successfully finalized transaction {}", trans_id);
                Ok(Response::new(response))
            }
            Err(err) => {
                error!("Error while finalizing transaction: {}", err);
                let mut response = FinalizeTransactionResponse::default();
                response.set_ret(ReturnStatus::Error);
                response.error = Some(err.into());
                Ok(Response::new(response))
            }
        }
    }
}
