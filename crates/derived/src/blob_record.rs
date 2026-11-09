//! A blob's metadata record, stored under `MetaKey::Blob` once the blob's
//! frames are in blob storage.

use scion_core::FormatError;
use scion_core::wire::put_uvarint;

use crate::DeriveError;
use crate::codec::decode;

/// What the metadata database knows about a stored blob.
///
/// Encoded as: `u8` version, `uvarint` size, the 32-byte SHA-256,
/// `opt<20-byte git SHA-1>`, and `u8` text (0 binary, 1 text). The decoder
/// accepts only that canonical form.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BlobRecord {
    /// The blob's length in bytes.
    pub size: u64,
    /// The SHA-256 of the blob's bytes.
    pub sha256: [u8; 32],
    /// The git SHA-1 of the blob, when it was imported.
    pub git_sha1: Option<[u8; 20]>,
    /// Whether the blob is text, classified from its bytes when uploaded.
    pub text: bool,
}

impl BlobRecord {
    /// The version byte that starts every record's encoding.
    pub const VERSION: u8 = 3;

    /// The record's stored bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![Self::VERSION];
        put_uvarint(&mut out, self.size);
        out.extend_from_slice(&self.sha256);
        match &self.git_sha1 {
            None => out.push(0),
            Some(s) => {
                out.push(1);
                out.extend_from_slice(s);
            }
        }
        out.push(u8::from(self.text));
        out
    }

    /// Decodes stored bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DeriveError> {
        decode("blob record", bytes, |r| {
            let version = r.u8()?;
            if version != Self::VERSION {
                return Err(FormatError::UnsupportedVersion(version));
            }
            let size = r.uvarint()?;
            let sha256 = r.array32()?;
            let git_sha1 = match r.opt_tag()? {
                false => None,
                true => {
                    let mut s = [0; 20];
                    s.copy_from_slice(r.take(20)?);
                    Some(s)
                }
            };
            let text = match r.u8()? {
                0 => false,
                1 => true,
                code => {
                    return Err(FormatError::InvalidCode {
                        field: "text classification",
                        code,
                    });
                }
            };
            Ok(Self {
                size,
                sha256,
                git_sha1,
                text,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip() {
        for git_sha1 in [None, Some([7; 20])] {
            for text in [false, true] {
                let r = BlobRecord {
                    size: 1 << 40,
                    sha256: [9; 32],
                    git_sha1,
                    text,
                };
                assert_eq!(BlobRecord::decode(&r.encode()).unwrap(), r);
            }
        }
    }

    #[test]
    fn only_the_canonical_encoding_is_accepted() {
        let good = BlobRecord {
            size: 7,
            sha256: [9; 32],
            git_sha1: None,
            text: true,
        }
        .encode();
        let mut trailing = good.clone();
        trailing.push(0);
        let mut text_2 = good.clone();
        *text_2.last_mut().unwrap() = 2;
        let mut tag_2 = good.clone();
        tag_2[1 + 1 + 32] = 2;
        let mut version_2 = good.clone();
        version_2[0] = 2;
        let mut long_size = good.clone();
        long_size.splice(1..2, [0x87, 0x00]);
        for bad in [
            trailing,
            text_2,
            tag_2,
            version_2,
            long_size,
            good[..good.len() - 1].to_vec(),
        ] {
            assert!(BlobRecord::decode(&bad).is_err(), "{bad:?}");
        }
    }
}
