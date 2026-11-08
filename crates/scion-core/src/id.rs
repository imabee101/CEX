//! Typed object ids: BLAKE3 `derive_key` over an object's canonical bytes,
//! with a separate context string per kind.

use core::fmt;
use core::str::FromStr;

use crate::FormatError;

/// The BLAKE3 `derive_key` context for blob ids.
pub const BLOB_CONTEXT: &str = "scion v1 blob";
/// The BLAKE3 `derive_key` context for chunk ids.
pub const CHUNK_CONTEXT: &str = "scion v1 chunk";
/// The BLAKE3 `derive_key` context for tree ids.
pub const TREE_CONTEXT: &str = "scion v1 tree";
/// The BLAKE3 `derive_key` context for changeset ids.
pub const CHANGESET_CONTEXT: &str = "scion v1 changeset";

macro_rules! define_id {
    ($(#[$doc:meta])* $name:ident, $context:ident, $hashed:literal) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Wraps 32 id bytes without checking that any object has them.
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// The id's 32 bytes.
            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            #[doc = concat!("Computes the id of ", $hashed, ".")]
            pub fn hash(bytes: &[u8]) -> Self {
                Self(blake3::derive_key($context, bytes))
            }

            #[doc = concat!("Starts an incremental computation of the id of ", $hashed, ".")]
            pub fn hasher() -> IdHasher<Self> {
                IdHasher {
                    inner: blake3::Hasher::new_derive_key($context),
                    wrap: Self,
                }
            }
        }

        impl fmt::Display for $name {
            /// Formats the id as 64 lowercase hex digits.
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(f, &self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl FromStr for $name {
            type Err = FormatError;

            /// Parses exactly 64 lowercase hex digits.
            fn from_str(s: &str) -> Result<Self, FormatError> {
                parse_hex(s).map(Self)
            }
        }
    };
}

define_id!(
    /// The id of a blob: `derive_key("scion v1 blob", raw bytes)` over the
    /// file's full raw, uncompressed bytes, even when it is stored as chunks.
    BlobId, BLOB_CONTEXT, "a blob from its full raw bytes"
);
define_id!(
    /// The id of one chunk of a chunked blob: `derive_key("scion v1 chunk",
    /// chunk bytes)`.
    ChunkId, CHUNK_CONTEXT, "a chunk from its raw bytes"
);
define_id!(
    /// The id of a tree node: `derive_key("scion v1 tree", canonical bytes)`.
    TreeId, TREE_CONTEXT, "a tree node from its canonical bytes"
);
define_id!(
    /// The id of a changeset: `derive_key("scion v1 changeset", canonical
    /// bytes)`.
    ChangesetId, CHANGESET_CONTEXT, "a changeset from its canonical bytes"
);

/// Incrementally computes an id over bytes supplied in pieces, for objects
/// too large to hold in memory at once.
pub struct IdHasher<T> {
    inner: blake3::Hasher,
    wrap: fn([u8; 32]) -> T,
}

impl<T> IdHasher<T> {
    /// Feeds the next bytes of the object.
    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.inner.update(bytes);
        self
    }

    /// Returns the id of all bytes fed so far.
    pub fn finalize(&self) -> T {
        (self.wrap)(*self.inner.finalize().as_bytes())
    }
}

impl<T> fmt::Debug for IdHasher<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdHasher").finish_non_exhaustive()
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8; 32]) -> fmt::Result {
    let mut buf = [0u8; 64];
    for (i, b) in bytes.iter().enumerate() {
        buf[2 * i] = HEX[usize::from(b >> 4)];
        buf[2 * i + 1] = HEX[usize::from(b & 0xf)];
    }
    // Every byte written is an ASCII hex digit.
    f.write_str(core::str::from_utf8(&buf).map_err(|_| fmt::Error)?)
}

fn parse_hex(s: &str) -> Result<[u8; 32], FormatError> {
    let s = s.as_bytes();
    if s.len() != 64 {
        return Err(FormatError::InvalidHex);
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err(FormatError::InvalidHex),
    };
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = (digit(s[2 * i])? << 4) | digit(s[2 * i + 1])?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_use_distinct_contexts() {
        let b = BlobId::hash(b"x");
        assert_ne!(b.as_bytes(), ChunkId::hash(b"x").as_bytes());
        assert_ne!(b.as_bytes(), TreeId::hash(b"x").as_bytes());
        assert_ne!(b.as_bytes(), ChangesetId::hash(b"x").as_bytes());
        assert_ne!(b.as_bytes(), blake3::hash(b"x").as_bytes());
        assert_eq!(*b.as_bytes(), blake3::derive_key("scion v1 blob", b"x"));
    }

    #[test]
    fn hasher_equals_one_shot() {
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 7) as u8).collect();
        let mut h = BlobId::hasher();
        for piece in data.chunks(333) {
            h.update(piece);
        }
        assert_eq!(h.finalize(), BlobId::hash(&data));
    }

    #[test]
    fn hex_round_trip_and_rejects() {
        let id = TreeId::hash(b"t");
        let s = id.to_string();
        assert_eq!(s.len(), 64);
        assert_eq!(s.parse::<TreeId>(), Ok(id));
        assert_eq!(
            s.to_uppercase().parse::<TreeId>(),
            Err(FormatError::InvalidHex)
        );
        assert_eq!(s[..63].parse::<TreeId>(), Err(FormatError::InvalidHex));
        assert_eq!(
            format!("{s}0").parse::<TreeId>(),
            Err(FormatError::InvalidHex)
        );
        assert_eq!(
            format!("{}g", &s[..63]).parse::<TreeId>(),
            Err(FormatError::InvalidHex)
        );
        assert_eq!(format!("{id:?}"), format!("TreeId({s})"));
    }
}
