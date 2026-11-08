//! The byte encodings of derived values and blob records: minimal `uvarint`
//! integers, length-prefixed byte strings, and 32-byte ids. Derived formats
//! are internal and never reach clients; each is versioned by its type's
//! version.

use crate::DeriveError;

pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// A cursor over stored bytes; any malformed input is data loss.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8], what: &'static str) -> Self {
        Self { buf, what }
    }

    fn bad(&self, why: &str) -> DeriveError {
        DeriveError::DataLoss(format!("malformed {}: {why}", self.what))
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], DeriveError> {
        if n > self.buf.len() {
            return Err(self.bad("truncated"));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DeriveError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn array32(&mut self) -> Result<[u8; 32], DeriveError> {
        let mut a = [0; 32];
        a.copy_from_slice(self.take(32)?);
        Ok(a)
    }

    pub(crate) fn uvarint(&mut self) -> Result<u64, DeriveError> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = self.u8()?;
            let low = u64::from(b & 0x7f);
            if i == 9 && low > 1 {
                return Err(self.bad("varint overflow"));
            }
            v |= low << (7 * i);
            if b & 0x80 == 0 {
                if i > 0 && b == 0 {
                    return Err(self.bad("non-minimal varint"));
                }
                return Ok(v);
            }
        }
        Err(self.bad("varint overflow"))
    }

    /// A count of items each at least `min_item` bytes long, checked against
    /// the remaining input before anything is allocated.
    pub(crate) fn count(&mut self, min_item: usize) -> Result<usize, DeriveError> {
        let n = self.uvarint()?;
        if n.saturating_mul(min_item as u64) > self.buf.len() as u64 {
            return Err(self.bad("count exceeds input"));
        }
        Ok(n as usize)
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], DeriveError> {
        let n = self.uvarint()?;
        if n > self.buf.len() as u64 {
            return Err(self.bad("truncated"));
        }
        self.take(n as usize)
    }

    pub(crate) fn finish(self) -> Result<(), DeriveError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(self.bad("trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_round_trips_and_rejects_non_minimal() {
        for v in [0, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let mut b = Vec::new();
            put_uvarint(&mut b, v);
            let mut r = Reader::new(&b, "test");
            assert_eq!(r.uvarint().unwrap(), v);
            r.finish().unwrap();
        }
        assert!(Reader::new(&[0x80, 0x00], "test").uvarint().is_err());
        assert!(Reader::new(&[0xff; 10], "test").uvarint().is_err());
        assert!(Reader::new(&[0x80], "test").uvarint().is_err());
    }

    #[test]
    fn counts_are_checked_against_the_input() {
        assert!(Reader::new(&[5, 0, 0], "test").count(1).is_err());
        assert_eq!(Reader::new(&[2, 0, 0], "test").count(1).unwrap(), 2);
    }
}
