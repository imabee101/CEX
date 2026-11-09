//! Operation errors, each with a `scion.v1` reason code.

use derived::{DeriveError, InvalidKind};
use store::StoreError;

/// A `scion.v1` reason code an operation can fail with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Code {
    /// `INVALID_ARGUMENT`.
    InvalidArgument,
    /// `NOT_CANONICAL`.
    NotCanonical,
    /// `LIMIT_EXCEEDED`.
    LimitExceeded,
    /// `NO_OP_CHANGE`.
    NoOpChange,
    /// `PATH_CONFLICT`.
    PathConflict,
    /// `CASE_CONFLICT`.
    CaseConflict,
    /// `NOT_FOUND`.
    NotFound,
    /// `DATA_LOSS`: stored bytes are missing or fail verification.
    DataLoss,
    /// `INTERNAL`: a server defect, such as a violated invariant, a failed
    /// task, or a misconfigured backend.
    Internal,
    /// `RESOURCE_EXHAUSTED`: the server is overloaded; retry after a delay.
    ResourceExhausted,
    /// `UNAVAILABLE`.
    Unavailable,
    /// `ACL_PENDING`: the requested ACL version has not been backfilled.
    AclPending,
}

impl Code {
    /// Whether a retry of the same request may succeed.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::ResourceExhausted | Self::Unavailable | Self::AclPending
        )
    }
}

/// An operation's failure.
#[derive(Debug, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct OpError {
    /// The reason code.
    pub code: Code,
    /// A human-readable explanation; never branched on.
    pub message: String,
    /// The paths the failure is about, if any.
    pub paths: Vec<Vec<u8>>,
}

impl OpError {
    /// An error with no paths.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            paths: Vec::new(),
        }
    }

    /// An error about one path.
    pub fn at(code: Code, message: impl Into<String>, path: &[u8]) -> Self {
        Self {
            code,
            message: message.into(),
            paths: vec![path.to_vec()],
        }
    }
}

impl From<StoreError> for OpError {
    fn from(e: StoreError) -> Self {
        let code = match e {
            StoreError::InvalidArgument(_) => Code::InvalidArgument,
            StoreError::DataLoss(_) => Code::DataLoss,
            StoreError::Unavailable(_) => Code::Unavailable,
            StoreError::Overloaded { .. } => Code::ResourceExhausted,
            StoreError::Internal(_) => Code::Internal,
        };
        Self::new(code, e.to_string())
    }
}

impl From<DeriveError> for OpError {
    fn from(e: DeriveError) -> Self {
        match e {
            DeriveError::Store(s) => s.into(),
            DeriveError::Invalid { kind, path } => {
                let (code, what) = match kind {
                    InvalidKind::NoOp => (Code::NoOpChange, "change leaves the path as it was"),
                    InvalidKind::PathConflict => {
                        (Code::PathConflict, "a file and a directory at one path")
                    }
                    InvalidKind::SizeMismatch => {
                        (Code::InvalidArgument, "size differs from the blob's size")
                    }
                };
                Self::at(
                    code,
                    format!("{what}: {}", String::from_utf8_lossy(&path)),
                    &path,
                )
            }
            DeriveError::NotFound(m) => Self::new(Code::NotFound, m),
            DeriveError::DataLoss(m) => Self::new(Code::DataLoss, m),
            DeriveError::CaseConflict { path, other } => Self::at(
                Code::CaseConflict,
                format!(
                    "{} conflicts with {} in the same directory",
                    String::from_utf8_lossy(&path),
                    String::from_utf8_lossy(&other)
                ),
                &path,
            ),
            DeriveError::Pending(m) => Self::new(Code::Unavailable, m),
            DeriveError::Internal(m) => Self::new(Code::Internal, m),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Of the codes ops returns, `UNAVAILABLE` and `ACL_PENDING` are retryable (AGENTS.md
    /// lists `RESOURCE_EXHAUSTED`, `UNAVAILABLE`, `LAND_RETRIES_EXHAUSTED`,
    /// and `ACL_PENDING`).
    #[test]
    fn only_overload_unavailable_and_acl_pending_are_retryable() {
        use Code::*;
        for c in [
            InvalidArgument,
            NotCanonical,
            LimitExceeded,
            NoOpChange,
            PathConflict,
            CaseConflict,
            NotFound,
            DataLoss,
            Internal,
        ] {
            assert!(!c.retryable(), "{c:?}");
        }
        assert!(Unavailable.retryable());
        assert!(ResourceExhausted.retryable());
        assert!(AclPending.retryable());
    }

    #[test]
    fn store_errors_keep_their_meaning() {
        let code = |e: StoreError| OpError::from(e).code;
        assert_eq!(
            code(StoreError::InvalidArgument("x".into())),
            Code::InvalidArgument
        );
        assert_eq!(code(StoreError::DataLoss("x".into())), Code::DataLoss);
        assert_eq!(code(StoreError::Unavailable("x".into())), Code::Unavailable);
        assert_eq!(code(StoreError::Internal("x".into())), Code::Internal);
        assert_eq!(
            code(StoreError::Overloaded {
                retry_after: std::time::Duration::from_secs(1)
            }),
            Code::ResourceExhausted
        );
    }
}
