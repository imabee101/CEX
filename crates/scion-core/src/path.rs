//! Repository paths and tree entry names.

use core::fmt;

use crate::{FormatError, MAX_FIELD_BYTES};

/// A file path in a repository: a byte string of `/`-separated components,
/// none of them empty, `.`, or `..`, with no NUL byte anywhere.
///
/// Paths order by their bytes. Length limits that apply to newly created
/// changesets are enforced when the changeset is created, not here; only
/// `MAX_FIELD_BYTES` is enforced here.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoPath(Vec<u8>);

impl RepoPath {
    /// Validates `bytes` as a path.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, FormatError> {
        let bytes = bytes.into();
        check_path(&bytes)?;
        Ok(Self(bytes))
    }

    /// The path's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the path's bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// The path's components, in order.
    pub fn components(&self) -> impl Iterator<Item = &[u8]> {
        self.0.split(|&b| b == b'/')
    }
}

impl fmt::Debug for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RepoPath({:?})", String::from_utf8_lossy(&self.0))
    }
}

impl AsRef<[u8]> for RepoPath {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

fn check_path(bytes: &[u8]) -> Result<(), FormatError> {
    if bytes.len() > MAX_FIELD_BYTES {
        return Err(FormatError::FieldTooLong {
            len: bytes.len() as u64,
        });
    }
    if bytes.contains(&0) {
        return Err(FormatError::InvalidPath);
    }
    for component in bytes.split(|&b| b == b'/') {
        if component.is_empty() || component == b"." || component == b".." {
            return Err(FormatError::InvalidPath);
        }
    }
    Ok(())
}

/// Checks a tree entry name. Names of ordinary entries are path components:
/// non-empty, with no `/` or NUL, and not `.` or `..`. A shard entry's name is
/// a prefix of the names below it, so it may also be `.` or `..`.
pub(crate) fn check_name(name: &[u8], shard: bool) -> Result<(), FormatError> {
    if name.len() > MAX_FIELD_BYTES {
        return Err(FormatError::FieldTooLong {
            len: name.len() as u64,
        });
    }
    if name.is_empty() || name.iter().any(|&b| b == b'/' || b == 0) {
        return Err(FormatError::InvalidName);
    }
    if !shard && (name == b"." || name == b"..") {
        return Err(FormatError::InvalidName);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_paths() {
        for p in [&b"a"[..], b"a/b", b"a/.b/c..", b"...", b"\xff\xfe/x", b" "] {
            assert!(RepoPath::new(p).is_ok(), "{p:?}");
        }
    }

    #[test]
    fn rejects_invalid_paths() {
        for p in [
            &b""[..],
            b"/",
            b"/a",
            b"a/",
            b"a//b",
            b".",
            b"..",
            b"a/./b",
            b"a/../b",
            b"a/.",
            b"../a",
            b"a\0b",
        ] {
            assert_eq!(RepoPath::new(p), Err(FormatError::InvalidPath), "{p:?}");
        }
    }

    #[test]
    fn names() {
        assert!(check_name(b"a", false).is_ok());
        assert!(check_name(b".", true).is_ok());
        assert!(check_name(b"..", true).is_ok());
        for (n, shard) in [
            (&b""[..], false),
            (b"", true),
            (b".", false),
            (b"..", false),
            (b"a/b", false),
            (b"a/b", true),
            (b"a\0", true),
        ] {
            assert_eq!(
                check_name(n, shard),
                Err(FormatError::InvalidName),
                "{n:?} {shard}"
            );
        }
    }
}
