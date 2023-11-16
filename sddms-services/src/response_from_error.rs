
#[macro_export]
macro_rules! response_from_error_for {
    ($response_type:ty, $payload_type:ty, $payload_property_name:ident) => {
        impl From<ApiError> for $response_type {
            fn from(value: ApiError) -> Self {
                let mut response = <$response_type>::default();
                response.set_ret(ReturnStatus::Error);
                response.$payload_property_name = Some(<$payload_type>::Error(value));
                response
            }
        }

        impl From<SddmsError> for $response_type {
            fn from(value: SddmsError) -> Self {
                let api_err: ApiError = value.into();
                <$response_type>::from(api_err)
            }
        }

        impl From<SddmsTermError> for $response_type {
            fn from(value: SddmsTermError) -> Self {
                let err: SddmsError = value.into();
                <$response_type>::from(err)
            }
        }
    };

    ($response_type:ty, $error_property_name:ident) => {

        impl From<ApiError> for $response_type {
            fn from(value: ApiError) -> Self {
                let mut response = <$response_type>::default();
                response.set_ret(ReturnStatus::Error);
                response.$error_property_name = Some(value);
                response
            }
        }

        impl From<SddmsError> for $response_type {
            fn from(value: SddmsError) -> Self {
                let api_err: ApiError = value.into();
                <$response_type>::from(api_err)
            }
        }

        impl From<SddmsTermError> for $response_type {
            fn from(value: SddmsTermError) -> Self {
                let err: SddmsError = value.into();
                <$response_type>::from(err)
            }
        }
    };
}
