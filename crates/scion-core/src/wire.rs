//! The canonical primitives of the encoding (see [Primitives](crate#primitives)),
//! for formats built from them: minimal LEB128 `uvarint`, zigzag `svarint`,
//! length-prefixed `bytes`, `opt<T>` tags, and 32-byte ids.
//!
//! The writers produce only the canonical form, and [`Reader`] rejects every
//! other form with a [`FormatError`].
//!
//! ```
//! use scion_core::wire::{Reader, put_bytes, put_svarint, put_uvarint};
//!
//! let mut out = Vec::new();
//! put_uvarint(&mut out, 300);
//! put_svarint(&mut out, -2);
//! put_bytes(&mut out, b"name");
//! assert_eq!(out, [0xac, 0x02, 0x03, 0x04, b'n', b'a', b'm', b'e']);
//!
//! let mut r = Reader::new(&out);
//! assert_eq!(r.uvarint()?, 300);
//! assert_eq!(r.svarint()?, -2);
//! assert_eq!(r.bytes()?, b"name");
//! r.finish()?;
//!
//! // A non-minimal varint is rejected.
//! assert!(Reader::new(&[0x80, 0x00]).uvarint().is_err());
//! # Ok::<(), scion_core::FormatError>(())
//! ```

use crate::{FormatError, MAX_FIELD_BYTES};

/// Appends the minimal unsigned LEB128 encoding of `v`.
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Appends the zigzag-then-`uvarint` encoding of `v`.
pub fn put_svarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

/// Appends `bytes`: a `uvarint` length, then the raw bytes.
pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// Number of bytes `put_uvarint` writes for `v`.
pub(crate) fn uvarint_len(v: u64) -> usize {
    let bits = 64 - (v | 1).leading_zeros() as usize;
    bits.div_ceil(7)
}

/// A cursor over untrusted input that decodes canonical primitives. Every
/// method fails, consuming nothing it can use, on input that is truncated or
/// not canonical.
#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    /// A reader at the start of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// How many bytes are left.
    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    /// Fails with [`FormatError::TrailingBytes`] unless every byte was
    /// consumed.
    pub fn finish(self) -> Result<(), FormatError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(FormatError::TrailingBytes(self.buf.len()))
        }
    }

    /// The next `n` raw bytes.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], FormatError> {
        if n > self.buf.len() {
            return Err(FormatError::Truncated);
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    /// The next byte.
    pub fn u8(&mut self) -> Result<u8, FormatError> {
        Ok(self.take(1)?[0])
    }

    /// The next 32 bytes, such as an id.
    pub fn array32(&mut self) -> Result<[u8; 32], FormatError> {
        let mut a = [0u8; 32];
        a.copy_from_slice(self.take(32)?);
        Ok(a)
    }

    /// A minimal `uvarint`.
    pub fn uvarint(&mut self) -> Result<u64, FormatError> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = self.u8()?;
            if i == 9 && b > 0x01 {
                return Err(FormatError::VarintOverflow);
            }
            v |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                if i > 0 && b == 0 {
                    return Err(FormatError::NonMinimalVarint);
                }
                return Ok(v);
            }
        }
        Err(FormatError::VarintOverflow)
    }

    /// A zigzag `svarint`.
    pub fn svarint(&mut self) -> Result<i64, FormatError> {
        let u = self.uvarint()?;
        Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
    }

    /// A `bytes` field, rejecting a length over [`MAX_FIELD_BYTES`] before
    /// looking at (or allocating for) its contents.
    pub fn bytes(&mut self) -> Result<&'a [u8], FormatError> {
        let len = self.uvarint()?;
        if len > MAX_FIELD_BYTES as u64 {
            return Err(FormatError::FieldTooLong { len });
        }
        self.take(len as usize)
    }

    /// An `opt<T>` tag: `false` for `0x00`, `true` for `0x01`.
    pub fn opt_tag(&mut self) -> Result<bool, FormatError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            t => Err(FormatError::InvalidOptionTag(t)),
        }
    }

    /// A `uvarint` count of items that each occupy at least `min_item_bytes`
    /// (taken as 1 when 0), rejecting counts the remaining input cannot hold, so a
    /// caller may allocate for the count.
    pub fn count(&mut self, min_item_bytes: usize) -> Result<usize, FormatError> {
        let n = self.uvarint()?;
        self.fits(n, min_item_bytes)
    }

    /// Checks that `n` items of at least `min_item_bytes` each fit in the
    /// remaining input, so a count read from it can size an allocation.
    pub(crate) fn fits(&self, n: u64, min_item_bytes: usize) -> Result<usize, FormatError> {
        if n > (self.remaining() / min_item_bytes.max(1)) as u64 {
            return Err(FormatError::Truncated);
        }
        Ok(n as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, v);
        out
    }

    #[test]
    fn uvarint_known_encodings() {
        assert_eq!(enc(0), [0x00]);
        assert_eq!(enc(1), [0x01]);
        assert_eq!(enc(127), [0x7f]);
        assert_eq!(enc(128), [0x80, 0x01]);
        assert_eq!(enc(300), [0xac, 0x02]);
        assert_eq!(enc(16_384), [0x80, 0x80, 0x01]);
        assert_eq!(
            enc(u64::MAX),
            [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
        );
    }

    #[test]
    fn uvarint_len_matches_encoding() {
        for v in [0, 1, 127, 128, 16_383, 16_384, u64::MAX >> 1, u64::MAX] {
            assert_eq!(uvarint_len(v), enc(v).len(), "{v}");
        }
    }

    #[test]
    fn uvarint_rejects_non_minimal_and_overflow() {
        let read = |b: &[u8]| Reader::new(b).uvarint();
        assert_eq!(read(&[0x80, 0x00]), Err(FormatError::NonMinimalVarint));
        assert_eq!(
            read(&[0x81, 0x80, 0x00]),
            Err(FormatError::NonMinimalVarint)
        );
        assert_eq!(
            read(&[0xff; 9].iter().copied().chain([0x02]).collect::<Vec<_>>()),
            Err(FormatError::VarintOverflow)
        );
        assert_eq!(read(&[0xff; 10]), Err(FormatError::VarintOverflow));
        assert_eq!(read(&[0x80]), Err(FormatError::Truncated));
        assert_eq!(read(&[]), Err(FormatError::Truncated));
    }

    #[test]
    fn svarint_zigzag() {
        for (v, want) in [
            (0i64, 0u64),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (i64::MAX, u64::MAX - 1),
            (i64::MIN, u64::MAX),
        ] {
            let mut out = Vec::new();
            put_svarint(&mut out, v);
            assert_eq!(out, enc(want), "{v}");
            assert_eq!(Reader::new(&out).svarint(), Ok(v));
        }
    }

    #[test]
    fn count_rejects_counts_the_remaining_input_cannot_hold() {
        // Three 2-byte items fit in 6 remaining bytes; four do not, and must be
        // rejected before any caller allocates for them.
        let rest = [0u8; 6];
        let input = |n: u8| [&[n][..], &rest].concat();
        assert_eq!(Reader::new(&input(3)).count(2), Ok(3));
        assert_eq!(Reader::new(&input(4)).count(2), Err(FormatError::Truncated));
        assert_eq!(Reader::new(&input(7)).count(1), Err(FormatError::Truncated));
        assert_eq!(Reader::new(&input(6)).count(1), Ok(6));
        // A zero item size counts as one byte, so it never divides by zero.
        assert_eq!(Reader::new(&input(6)).count(0), Ok(6));
        assert_eq!(Reader::new(&input(7)).count(0), Err(FormatError::Truncated));
    }

    #[test]
    fn bytes_rejects_oversized_length_before_reading() {
        let mut b = Vec::new();
        put_uvarint(&mut b, MAX_FIELD_BYTES as u64 + 1);
        assert_eq!(
            Reader::new(&b).bytes(),
            Err(FormatError::FieldTooLong {
                len: MAX_FIELD_BYTES as u64 + 1
            })
        );
        let mut b = Vec::new();
        put_uvarint(&mut b, MAX_FIELD_BYTES as u64);
        assert_eq!(Reader::new(&b).bytes(), Err(FormatError::Truncated));
    }
}
