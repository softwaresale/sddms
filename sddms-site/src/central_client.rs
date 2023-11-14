use tonic::transport::Channel;
use sddms_services::central_controller::concurrency_controller_service_client::ConcurrencyControllerServiceClient;
use sddms_services::central_controller::register_site_response::RegisterSitePayload;
use sddms_services::central_controller::{AcquireLockRequest, FinalizeTransactionRequest, LockMode, RegisterSiteRequest, RegisterTransactionRequest};
use sddms_services::central_controller::acquire_lock_response::AcquireLockPayload;
use sddms_services::central_controller::register_transaction_response::RegisterTransactionPayload;
use sddms_services::shared::FinalizeMode;
use sddms_shared::error::SddmsError;

pub struct CentralClient {
    client: ConcurrencyControllerServiceClient<Channel>,
}

impl CentralClient {
    pub async fn new(conn_str: &str) -> Result<Self, SddmsError> {
        let conn_str = format!("http://{}", conn_str);
        let client = ConcurrencyControllerServiceClient::connect(conn_str)
            .await
            .map_err(|err| SddmsError::site("Failed to connect to central site").with_cause(err))?;

        Ok(Self {
            client
        })
    }

    pub async fn register_self(&mut self, ip: &str, port: u16) -> Result<u32, SddmsError> {
        let register_request = RegisterSiteRequest {
            host: ip.to_string(),
            port: port as u32,
        };

        let response = self.client.register_site(register_request)
            .await
            .map_err(|err| SddmsError::site("Failed to transport register site request").with_cause(err))
            ?.into_inner();

        match response.register_site_payload.unwrap() {
            RegisterSitePayload::Error(api_err) => {
                Err(api_err.into())
            }
            RegisterSitePayload::Results(results) => {
                Ok(results.site_id)
            }
        }
    }

    pub async fn register_transaction(&mut self, site_id: u32) -> Result<u32, SddmsError> {
        let request = RegisterTransactionRequest {
            site_id,
            name: None,
        };

        let response = self.client.register_transaction(request)
            .await
            .map_err(|err| SddmsError::site("Failed to transport register site request").with_cause(err))
            ?.into_inner();

        match response.register_transaction_payload.unwrap() {
            RegisterTransactionPayload::Error(api_err) => {
                Err(api_err.into())
            }
            RegisterTransactionPayload::Results(results) => {
                Ok(results.trans_id)
            }
        }
    }

    pub async fn acquire_table_lock(&mut self, site_id: u32, transaction_id: u32, table: &str) -> Result<(), SddmsError> {
        let mut request = AcquireLockRequest {
            site_id,
            transaction_id,
            record_name: table.to_string(),
            lock_mode: 0,
        };
        request.set_lock_mode(LockMode::Exclusive);

        let response = self.client.acquire_lock(request)
            .await
            .map_err(|err| SddmsError::site("Failed to transport acquire lock request").with_cause(err))
            ?.into_inner();

        match response.acquire_lock_payload.unwrap() {
            AcquireLockPayload::Error(api_err) => {
                let err: SddmsError = api_err.into();
                Err(SddmsError::site(format!("Failed to acquire lock for {}", table))
                    .with_cause(err))
            }
            AcquireLockPayload::Results(_) => {
                Ok(())
            }
        }
    }

    pub async fn finalize_transaction(&mut self, site_id: u32, trans_id: u32, mode: FinalizeMode) -> Result<(), SddmsError> {
        let mut request = FinalizeTransactionRequest {
            site_id,
            transaction_id: trans_id,
            finalize_mode: 0,
        };
        request.set_finalize_mode(mode);

        let response = self.client.finalize_transaction(request)
            .await
            .map_err(|err| SddmsError::site("Failed to transport finalize transaction request").with_cause(err))
            ?.into_inner();

        match response.error {
            Some(api_err) => {
                let err: SddmsError = api_err.into();
                Err(SddmsError::site(format!("Failed to finalize transaction {}", trans_id))
                    .with_cause(err))
            }
            None => {
                Ok(())
            }
        }
    }
}
