//! Derived values and blob records are built from scion-core's canonical
//! primitives ([`scion_core::wire`]). Derived formats are internal and never
//! reach clients; each is versioned by its type's version. Stored bytes that
//! do not decode are data loss.

use scion_core::FormatError;
use scion_core::wire::Reader;

use crate::DeriveError;

/// Decodes all of `bytes` with `f`, which must consume every byte; any
/// failure is data loss in the stored `what`.
pub(crate) fn decode<T>(
    what: &str,
    bytes: &[u8],
    f: impl FnOnce(&mut Reader<'_>) -> Result<T, FormatError>,
) -> Result<T, DeriveError> {
    let mut r = Reader::new(bytes);
    f(&mut r)
        .and_then(|v| r.finish().map(|()| v))
        .map_err(|e| DeriveError::DataLoss(format!("malformed {what}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_failure_and_trailing_bytes_are_data_loss() {
        let read = |b: &[u8]| decode("test", b, |r| r.uvarint());
        assert_eq!(read(&[5]).unwrap(), 5);
        for bad in [&[5, 0][..], &[0x80, 0x00], &[]] {
            let err = read(bad).unwrap_err();
            assert!(
                matches!(&err, DeriveError::DataLoss(m) if m.starts_with("malformed test: ")),
                "{err}"
            );
        }
    }
}
