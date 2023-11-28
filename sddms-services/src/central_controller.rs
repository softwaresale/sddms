use std::fmt::{Display, Formatter};
use tonic::include_proto;
use crate::central_controller::register_site_response::RegisterSitePayload;
use crate::response_from_error_for;
use crate::shared::{ApiError, ReturnStatus};
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::central_controller::acquire_lock_response::AcquireLockPayload;
use crate::central_controller::register_transaction_response::RegisterTransactionPayload;
use crate::central_controller::release_lock_response::ReleaseLockPayload;

include_proto!("sddms.cc");

response_from_error_for!(RegisterSiteResponse, RegisterSitePayload, register_site_payload);
response_from_error_for!(RegisterTransactionResponse, RegisterTransactionPayload, register_transaction_payload);
response_from_error_for!(AcquireLockResponse, AcquireLockPayload, acquire_lock_payload);
response_from_error_for!(ReleaseLockResponse, ReleaseLockPayload, release_lock_payload);
response_from_error_for!(FinalizeTransactionResponse, error);

impl Display for LockMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LockMode::Unspecified => write!(f, "unspecified"),
            LockMode::Exclusive => write!(f, "exclusive"),
            LockMode::Shared => write!(f, "shared")
        }
    }
}
