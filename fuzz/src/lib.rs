//! Fuzz targets for every Scion decoder.
//!
//! Each target feeds arbitrary bytes to one decoder. A decoder must never
//! panic, and whatever it accepts must be the canonical encoding of the value
//! it returns (INV-10): re-encoding gives back exactly the input. The checks
//! live here so stable tests can run them on the golden vectors too.

use scion_core::{
    BlobId, Changeset, ChangesetId, ChunkId, ChunkList, FRAME_DIGEST_LEN, Frame, FrameHeader, Tree,
    TreeId,
};

/// Checks the changeset decoder on `data`.
pub fn check_changeset(data: &[u8]) {
    if let Ok(c) = Changeset::decode(data) {
        assert_eq!(c.encode(), data, "accepted a non-canonical changeset");
        assert_eq!(c.id(), ChangesetId::hash(data));
        assert_eq!(Changeset::new(c.clone().into_parts()).as_ref(), Ok(&c));
    }
}

/// Checks the tree decoder on `data`.
pub fn check_tree(data: &[u8]) {
    if let Ok(t) = Tree::decode(data) {
        assert_eq!(t.encode(), data, "accepted a non-canonical tree");
        assert_eq!(t.id(), TreeId::hash(data));
        assert_eq!(Tree::new(t.clone().into_entries()).as_ref(), Ok(&t));
    }
}

/// Checks the frame decoder, and the header parser, on `data`, then on
/// `data` with its trailing digest replaced by the correct one. A fuzzer
/// almost never guesses a BLAKE3 digest, so without the second check the
/// decoder's accept path would run only on unmutated seeds.
pub fn check_frame(data: &[u8]) {
    check_frame_bytes(data);
    if data.len() >= FRAME_DIGEST_LEN {
        let at = data.len() - FRAME_DIGEST_LEN;
        let mut signed = data.to_vec();
        signed[at..].copy_from_slice(blake3::hash(&data[..at]).as_bytes());
        check_frame_bytes(&signed);
    }
}

fn check_frame_bytes(data: &[u8]) {
    if let Ok((h, len)) = FrameHeader::parse_prefix(data) {
        assert_eq!(
            h.encode(),
            &data[..len],
            "accepted a non-canonical frame header"
        );
        assert_eq!(h.encoded_len(), len);
    }
    if let Ok(f) = Frame::decode(data) {
        let h = f.header();
        let again = Frame::encode(h.id(), h.codec(), h.raw_len(), f.payload())
            .expect("a decoded frame re-encodes");
        assert_eq!(again, data, "accepted a non-canonical frame");
        assert_eq!(h.frame_len(), data.len() as u64);
    }
}

/// Checks the chunk-list decoder on `data`.
pub fn check_chunk_list(data: &[u8]) {
    if let Ok(l) = ChunkList::decode(data) {
        assert_eq!(l.encode(), data, "accepted a non-canonical chunk list");
    }
}

/// Checks the hex id parser on `data`: whatever it accepts is exactly the
/// id's lowercase hex, for every id type.
pub fn check_id_hex(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let parsed = [
        s.parse::<BlobId>().map(|id| *id.as_bytes()),
        s.parse::<ChunkId>().map(|id| *id.as_bytes()),
        s.parse::<TreeId>().map(|id| *id.as_bytes()),
        s.parse::<ChangesetId>().map(|id| *id.as_bytes()),
    ];
    for p in &parsed {
        assert_eq!(p.is_ok(), parsed[0].is_ok(), "id types disagree on {s:?}");
    }
    if let Ok(bytes) = parsed[0] {
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, s, "accepted a non-canonical hex id");
        assert_eq!(BlobId::from_bytes(bytes).to_string(), s);
    }
}
