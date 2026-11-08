//! A blob's metadata record, stored under `MetaKey::Blob` once the blob's
//! frames are in blob storage.

use crate::DeriveError;
use crate::codec::{Reader, put_uvarint};

const RECORD_VERSION: u8 = 2;

/// What the metadata database knows about a stored blob.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BlobRecord {
    /// The blob's length in bytes.
    pub size: u64,
    /// The SHA-256 of the blob's bytes.
    pub sha256: [u8; 32],
    /// The git SHA-1 of the blob, when it was imported.
    pub git_sha1: Option<[u8; 20]>,
    /// Text classification computed from uploaded bytes. `None` for legacy
    /// records whose contents must be sniffed before a textual diff.
    pub text: Option<bool>,
}

impl BlobRecord {
    /// The record's stored bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![RECORD_VERSION];
        put_uvarint(&mut out, self.size);
        out.extend_from_slice(&self.sha256);
        match &self.git_sha1 {
            None => out.push(0),
            Some(s) => {
                out.push(1);
                out.extend_from_slice(s);
            }
        }
        out.push(match self.text {
            None => 0,
            Some(true) => 1,
            Some(false) => 2,
        });
        out
    }

    /// Decodes stored bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DeriveError> {
        let mut r = Reader::new(bytes, "blob record");
        let version = r.u8()?;
        if version != 1 && version != RECORD_VERSION {
            return Err(DeriveError::DataLoss("unknown blob record version".into()));
        }
        let size = r.uvarint()?;
        let sha256 = r.array32()?;
        let git_sha1 = match r.u8()? {
            0 => None,
            1 => {
                let mut s = [0; 20];
                s.copy_from_slice(r.take(20)?);
                Some(s)
            }
            _ => return Err(DeriveError::DataLoss("bad blob record tag".into())),
        };
        let text = if version == 1 {
            None
        } else {
            match r.u8()? {
                0 => None,
                1 => Some(true),
                2 => Some(false),
                _ => return Err(DeriveError::DataLoss("bad text classification tag".into())),
            }
        };
        r.finish()?;
        Ok(Self {
            size,
            sha256,
            git_sha1,
            text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for git_sha1 in [None, Some([7; 20])] {
            let r = BlobRecord {
                size: 1 << 40,
                sha256: [9; 32],
                git_sha1,
                text: Some(false),
            };
            assert_eq!(BlobRecord::decode(&r.encode()).unwrap(), r);
        }
        let mut bad = BlobRecord {
            size: 1,
            sha256: [0; 32],
            git_sha1: None,
            text: None,
        }
        .encode();
        bad.push(0);
        assert!(BlobRecord::decode(&bad).is_err());
    }

    #[test]
    fn classification_versions_and_tags() {
        // v1: version, size, SHA-256, absent git SHA-1; no classification.
        let mut legacy = vec![1, 7];
        legacy.extend_from_slice(&[9; 32]);
        legacy.push(0);
        let record = BlobRecord::decode(&legacy).unwrap();
        assert_eq!(record.text, None);
        for text in [None, Some(true), Some(false)] {
            let record = BlobRecord { text, ..record };
            assert_eq!(BlobRecord::decode(&record.encode()).unwrap(), record);
        }
        let mut bad = record.encode();
        *bad.last_mut().unwrap() = 3;
        assert!(BlobRecord::decode(&bad).is_err());
        legacy.push(0);
        assert!(BlobRecord::decode(&legacy).is_err());
    }
}
