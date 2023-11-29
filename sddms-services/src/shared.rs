pub mod lock_request;

use tonic::include_proto;
use sddms_shared::error::{SddmsError, SddmsTermError};
use sddms_shared::sql_metadata::TransactionStmt;

include_proto!("sddms.shared");

impl From<SddmsError> for ApiError {
    fn from(value: SddmsError) -> Self {
        let mut api_error = ApiError::default();
        let message = format!("{} - {}", value.category(), value.message());
        let description = value.inner_cause().as_ref()
            .map(|inner_cause| inner_cause.to_string())
            .unwrap_or_default();

        api_error.message = message;
        api_error.description = description;
        api_error
    }
}

impl From<SddmsTermError> for ApiError {
    fn from(value: SddmsTermError) -> Self {
        let mut err = ApiError::default();
        err.message = value.message().to_string();
        err.description = format!("{}", value);
        err
    }
}

impl Into<SddmsError> for ApiError {
    fn into(self) -> SddmsError {
        SddmsError::general(format!("ApiError: {} - {}", self.message, self.description))
    }
}

impl TryFrom<TransactionStmt> for FinalizeMode {
    type Error = SddmsError;

    fn try_from(value: TransactionStmt) -> Result<Self, Self::Error> {
        match value {
            TransactionStmt::Begin => {
                Err(SddmsError::general("Begin is not a finalization mode"))
            }
            TransactionStmt::Commit => {
                Ok(FinalizeMode::Commit)
            }
            TransactionStmt::Rollback => {
                Ok(FinalizeMode::Abort)
            }
        }
    }
}
