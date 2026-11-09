/// An error from a storage backend.
///
/// Callers match every variant: this crate is internal, so a new variant
/// must be handled everywhere rather than fall into a default.
#[derive(Debug, thiserror::Error)]
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
    /// The backend is overloaded; retry after `retry_after`.
    #[error("storage overloaded; retry after {retry_after:?}")]
    Overloaded {
        /// How long to wait before retrying.
        retry_after: std::time::Duration,
    },
    /// The backend failed in a way a retry cannot fix: it is misconfigured,
    /// rejected the request permanently, or broke an internal invariant.
    #[error("storage failed permanently: {0}")]
    Internal(String),
}

impl StoreError {
    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }
}
