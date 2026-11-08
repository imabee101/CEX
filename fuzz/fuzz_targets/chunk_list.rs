//! Fuzzes the chunk list decoder; see the crate docs for what is checked.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| fuzz::check_chunk_list(data));
