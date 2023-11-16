use tonic::include_proto;
use sddms_shared::error::{SddmsError, SddmsTermError};
use crate::{response_from_error_for};
use crate::shared::{ApiError, ReturnStatus};
use crate::site_controller::begin_transaction_response::BeginTransactionPayload;
use crate::site_controller::finalize_transaction_response::FinalizeTransactionPayload;
use crate::site_controller::invoke_query_response::InvokeQueryPayload;
use crate::site_controller::register_client_response::RegisterClientPayload;

include_proto!("sddms.site_manager");

response_from_error_for!(RegisterClientResponse, RegisterClientPayload, register_client_payload);
response_from_error_for!(BeginTransactionResponse, BeginTransactionPayload, begin_transaction_payload);
response_from_error_for!(InvokeQueryResponse, InvokeQueryPayload, invoke_query_payload);
response_from_error_for!(FinalizeTransactionResponse, FinalizeTransactionPayload, finalize_transaction_payload);
response_from_error_for!(ReplicationUpdateResponse, error);
