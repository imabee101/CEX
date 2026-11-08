//! Runs every fuzz check over the golden vectors, the seeds `cargo xtask`
//! gives the fuzzers, and their truncations.

const VECTORS: &str = include_str!("../../crates/scion-core/vectors/v1.txt");

#[test]
fn checks_pass_on_golden_vectors_and_their_prefixes() {
    let mut inputs = 0;
    for line in VECTORS.lines() {
        let Some(hex) = line.strip_prefix("bytes:") else {
            continue;
        };
        let hex = hex.trim();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        // Every prefix of the small vectors; a few of the large ones.
        let step = (bytes.len() / 64).max(1);
        for end in (0..=bytes.len()).step_by(step).chain([bytes.len()]) {
            let b = &bytes[..end];
            fuzz::check_changeset(b);
            fuzz::check_tree(b);
            fuzz::check_frame(b);
            fuzz::check_chunk_list(b);
        }
        inputs += 1;
    }
    let records = VECTORS.lines().filter(|l| l.starts_with("name:")).count();
    assert!(records > 0);
    assert_eq!(inputs, records, "every record has bytes");
}
