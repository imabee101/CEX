//! Support shared by scion-core's integration tests.
// Each test crate includes this module and uses only part of it.
#![allow(dead_code)]

pub(crate) mod convert;
pub(crate) mod reference;
pub(crate) mod vectors;

/// Decodes hex written with optional whitespace and `{XX*N}` repeats, so
/// literal vectors stay readable.
pub(crate) fn hx(s: &str) -> Vec<u8> {
    let mut expanded = String::new();
    let mut rest = s;
    while let Some(open) = rest.find('{') {
        expanded.push_str(&rest[..open]);
        let close = open + rest[open..].find('}').expect("unclosed repeat");
        let (unit, count) = rest[open + 1..close]
            .split_once('*')
            .expect("repeat is {XX*N}");
        expanded.push_str(&unit.repeat(count.parse().expect("repeat count")));
        rest = &rest[close + 1..];
    }
    expanded.push_str(rest);
    let digits: Vec<u8> = expanded
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    assert!(digits.len().is_multiple_of(2), "odd hex length in {s:?}");
    digits
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).expect("hex digit"))
        .collect()
}

pub(crate) fn id(s: &str) -> [u8; 32] {
    hx(s).try_into().expect("an id is 32 bytes")
}

pub(crate) fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
