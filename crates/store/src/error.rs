/// An error from a storage backend.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// The request is malformed; retrying it cannot succeed.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The backend could not complete the request; a retry may succeed.
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    /// Stored data is missing or does not have the expected shape.
    #[error("stored data is corrupt: {0}")]
    DataLoss(String),
}

impl StoreError {
    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }
}
