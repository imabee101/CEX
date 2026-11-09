//! Fuzzes the hex id parser; see the crate docs for what is checked.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = fuzz::check_id_hex(data);
});
