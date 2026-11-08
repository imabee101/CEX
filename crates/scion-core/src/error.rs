//! The error type for decoding and constructing core objects.

/// Why bytes or values were rejected as a changeset, tree, frame, path, or id.
///
/// Every decoder rejects anything that is not the single canonical encoding
/// of a valid value, so a decode error is never retryable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FormatError {
    /// The input ended before the value was complete.
    #[error("input ended unexpectedly")]
    Truncated,
    /// Bytes remain after a complete value.
    #[error("{0} trailing bytes after a complete value")]
    TrailingBytes(usize),
    /// A varint used more bytes than its minimal encoding.
    #[error("varint is not minimally encoded")]
    NonMinimalVarint,
    /// A varint does not fit in 64 bits.
    #[error("varint overflows 64 bits")]
    VarintOverflow,
    /// The format version byte is not one this decoder knows.
    #[error("unsupported format version {0:#04x}")]
    UnsupportedVersion(u8),
    /// An `opt<T>` tag byte is neither `0x00` nor `0x01`.
    #[error("invalid option tag {0:#04x}")]
    InvalidOptionTag(u8),
    /// A kind or codec byte is not defined for its field.
    #[error("invalid {field} code {code}")]
    InvalidCode {
        /// The field the byte belongs to.
        field: &'static str,
        /// The rejected byte.
        code: u8,
    },
    /// A `bytes` field or name is longer than `MAX_FIELD_BYTES`.
    #[error("field of {len} bytes exceeds MAX_FIELD_BYTES")]
    FieldTooLong {
        /// The declared length.
        len: u64,
    },
    /// A timestamp's UTC offset is not within ±86,399 seconds.
    #[error("UTC offset {0} s is out of range")]
    OffsetOutOfRange(i64),
    /// `committer` and `committer_date` are not both present or both absent.
    #[error("committer and committer_date must be both present or both absent")]
    CommitterDateMismatch,
    /// Keys are not in strictly increasing byte order (unsorted or duplicate).
    #[error("keys are not strictly increasing in byte order")]
    NotSorted,
    /// A path is empty, has an empty, `.` or `..` component, or contains NUL.
    #[error("invalid path")]
    InvalidPath,
    /// A tree entry name is empty, contains `/` or NUL, or is `.` or `..`
    /// (the last two are allowed only for shard entries).
    #[error("invalid tree entry name")]
    InvalidName,
    /// A tree node has more than `MAX_TREE_ENTRIES` entries.
    #[error("tree node has {0} entries, more than MAX_TREE_ENTRIES")]
    TooManyEntries(u64),
    /// A frame does not start with the magic `SCF1`.
    #[error("frame magic is not SCF1")]
    BadMagic,
    /// A frame's trailing digest does not match its bytes.
    #[error("frame digest mismatch")]
    DigestMismatch,
    /// A frame's lengths are inconsistent with its kind and codec.
    #[error("frame lengths are invalid: {0}")]
    BadFrameLength(&'static str),
    /// A chunk list's count does not match the blob length it describes.
    #[error("chunk list has {actual} chunks, expected {expected}")]
    ChunkCountMismatch {
        /// The count the blob length requires.
        expected: u64,
        /// The count found.
        actual: u64,
    },
    /// A hex id is not exactly 64 lowercase hex digits.
    #[error("invalid hex id")]
    InvalidHex,
}
