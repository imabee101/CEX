//! Case-conflict detection for names in one directory (INV-19).
//!
//! Two names conflict when they are equal after NFC normalization followed
//! by Unicode simple case folding, both per Unicode 18.0 as implemented by
//! `icu_normalizer` and `icu_casemap` 2.4. Names that are not valid UTF-8
//! compare by bytes.

use std::collections::HashMap;

/// The Unicode version case-conflict checks implement.
pub const UNICODE_VERSION: &str = "18.0.0";

/// A name's comparison key: two names conflict exactly when their keys are
/// equal.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FoldKey {
    /// A UTF-8 name, NFC-normalized and simple-case-folded.
    Text(String),
    /// A name that is not valid UTF-8, as is.
    Bytes(Vec<u8>),
}

/// The comparison key of a name.
pub fn fold_key(name: &[u8]) -> FoldKey {
    match std::str::from_utf8(name) {
        Ok(s) => {
            let nfc = icu_normalizer::ComposingNormalizer::new_nfc().normalize(s);
            let cm = icu_casemap::CaseMapper::new();
            FoldKey::Text(nfc.chars().map(|c| cm.simple_fold(c)).collect())
        }
        Err(_) => FoldKey::Bytes(name.to_vec()),
    }
}

/// The first of `added` that conflicts with another name in `all` (the
/// directory's resulting names, which include `added`), with the name it
/// conflicts with.
pub fn find_conflict<'a>(
    all: impl IntoIterator<Item = &'a [u8]>,
    added: &'a [Vec<u8>],
) -> Option<(&'a [u8], &'a [u8])> {
    let mut by_key: HashMap<FoldKey, Vec<&[u8]>> = HashMap::new();
    for name in all {
        by_key.entry(fold_key(name)).or_default().push(name);
    }
    for name in added {
        if let Some(other) = by_key[&fold_key(name)]
            .iter()
            .find(|n| **n != name.as_slice())
        {
            return Some((name, other));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conflict(a: &str, b: &str) -> bool {
        fold_key(a.as_bytes()) == fold_key(b.as_bytes())
    }

    #[test]
    fn case_and_normalization_variants_conflict() {
        assert!(conflict("README", "readme"));
        assert!(conflict("caf\u{e9}", "cafe\u{301}"));
        assert!(conflict("CAF\u{c9}", "cafe\u{301}"));
        assert!(conflict("\u{212a}elvin", "kelvin"));
        assert!(conflict("stra\u{df}e", "STRA\u{1e9e}E"));
        assert!(conflict("\u{3a3}", "\u{3c3}"));
    }

    #[test]
    fn full_folding_and_other_names_do_not_conflict() {
        // Simple folding never expands: sharp s is not "ss".
        assert!(!conflict("stra\u{df}e", "strasse"));
        assert!(!conflict("a", "b"));
        assert!(!conflict("a.txt", "a.TXT "));
        // Simple folding maps final sigma to sigma.
        assert!(conflict("\u{3c2}", "\u{3c3}"));
    }

    #[test]
    fn non_utf8_names_compare_by_bytes() {
        assert_eq!(fold_key(b"\xff"), FoldKey::Bytes(vec![0xff]));
        assert_ne!(fold_key(b"A\xff"), fold_key(b"a\xff"));
        assert_ne!(fold_key(b"\xff"), fold_key(b"\xfe"));
    }

    #[test]
    fn finds_a_conflict_with_an_existing_name_only() {
        let all: Vec<&[u8]> = vec![b"Foo", b"bar", b"foo"];
        let added = vec![b"foo".to_vec()];
        assert_eq!(
            find_conflict(all.iter().copied(), &added),
            Some((&b"foo"[..], &b"Foo"[..]))
        );
        let all: Vec<&[u8]> = vec![b"Foo", b"bar"];
        let added = vec![b"bar".to_vec()];
        assert_eq!(find_conflict(all.iter().copied(), &added), None);
    }
}
