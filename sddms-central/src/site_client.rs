use tonic::transport::Channel;
use sddms_services::site_controller::ReplicationUpdateRequest;
use sddms_services::site_controller::site_manager_service_client::SiteManagerServiceClient;
use sddms_shared::error::SddmsError;

pub struct SiteClient {
    client: SiteManagerServiceClient<Channel>
}

impl SiteClient {
    pub async fn connect<ConnStrT: Into<String>>(connection_str: ConnStrT) -> Result<Self, SddmsError> {
        let client = SiteManagerServiceClient::connect(connection_str.into())
            .await
            .map_err(|err| SddmsError::site("Failed to connect to central site").with_cause(err))?;

        Ok(Self {
            client
        })
    }

    pub async fn replicate_updates(&mut self, updates: &[String], originating_site: u32) -> Result<(), SddmsError> {
        let replication_update_request = ReplicationUpdateRequest {
            update_statements: updates.clone().to_vec(),
            originating_site,
        };

        let response = self.client.replication_update(replication_update_request)
            .await
            .map_err(|err| SddmsError::central(format!("Failed to transport replication update request: {} {}", err.code(), err.message())))
            ?.into_inner();

        if let Some(replication_error) = response.error {
            Err(replication_error.into())
        } else {
            Ok(())
        }
    }
}
