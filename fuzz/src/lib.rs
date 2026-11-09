//! Fuzz targets for every Scion decoder.
//!
//! Each target feeds arbitrary bytes to one decoder. A decoder must never
//! panic, and whatever it accepts must be the canonical encoding of the value
//! it returns (INV-10): re-encoding gives back exactly the input. The checks
//! live here so stable tests can run them on the golden vectors too; each
//! returns whether the decoder accepted its input, so those tests can tell a
//! check that runs its decoder from one that does nothing.

use scion_core::{
    BlobId, Changeset, ChangesetId, ChunkId, ChunkList, FRAME_DIGEST_LEN, Frame, FrameHeader, Tree,
    TreeId,
};

/// What a decoder did with an input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum Outcome {
    /// It decoded the input, and the input is the value's canonical encoding.
    Accepted,
    /// It rejected the input.
    Rejected,
}

/// The outcome of a decode, after asserting that an accepted input is the
/// canonical encoding of the decoded value: `encode` gives back exactly
/// `data`. `what` names the format in the panic message.
pub fn canonical<T, E>(
    data: &[u8],
    decoded: Result<T, E>,
    encode: impl FnOnce(&T) -> Vec<u8>,
    what: &str,
) -> (Outcome, Option<T>) {
    match decoded {
        Ok(v) => {
            assert_eq!(encode(&v), data, "accepted a non-canonical {what}");
            (Outcome::Accepted, Some(v))
        }
        Err(_) => (Outcome::Rejected, None),
    }
}

/// Checks the changeset decoder on `data`.
pub fn check_changeset(data: &[u8]) -> Outcome {
    let (outcome, c) = canonical(
        data,
        Changeset::decode(data),
        Changeset::encode,
        "changeset",
    );
    if let Some(c) = c {
        assert_eq!(c.id(), ChangesetId::hash(data));
        assert_eq!(Changeset::new(c.clone().into_parts()).as_ref(), Ok(&c));
    }
    outcome
}

/// Checks the tree decoder on `data`.
pub fn check_tree(data: &[u8]) -> Outcome {
    let (outcome, t) = canonical(data, Tree::decode(data), Tree::encode, "tree");
    if let Some(t) = t {
        assert_eq!(t.id(), TreeId::hash(data));
        assert_eq!(Tree::new(t.clone().into_entries()).as_ref(), Ok(&t));
    }
    outcome
}

/// What the frame check found: the decoder's outcome on the input as given,
/// and on the input with its trailing digest replaced by the correct one
/// (`Rejected` when the input is shorter than a digest).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub struct FrameOutcome {
    /// The outcome on the input as given.
    pub given: Outcome,
    /// The outcome on the re-signed input.
    pub resigned: Outcome,
}

/// Checks the frame decoder, and the header parser, on `data`, then on
/// `data` with its trailing digest replaced by the correct one. A fuzzer
/// almost never guesses a BLAKE3 digest, so without the second check the
/// decoder's accept path would run only on unmutated seeds.
pub fn check_frame(data: &[u8]) -> FrameOutcome {
    let given = check_frame_bytes(data);
    let resigned = match data.len().checked_sub(FRAME_DIGEST_LEN) {
        Some(at) => {
            let mut signed = data.to_vec();
            signed[at..].copy_from_slice(blake3::hash(&data[..at]).as_bytes());
            check_frame_bytes(&signed)
        }
        None => Outcome::Rejected,
    };
    FrameOutcome { given, resigned }
}

fn check_frame_bytes(data: &[u8]) -> Outcome {
    if let Ok((h, len)) = FrameHeader::parse_prefix(data) {
        assert_eq!(
            h.encode(),
            &data[..len],
            "accepted a non-canonical frame header"
        );
        assert_eq!(h.encoded_len(), len);
    }
    let encode = |f: &Frame<'_>| {
        let h = f.header();
        Frame::encode(h.id(), h.codec(), h.raw_len(), f.payload())
            .expect("a decoded frame re-encodes")
    };
    let (outcome, f) = canonical(data, Frame::decode(data), encode, "frame");
    if let Some(f) = f {
        assert_eq!(f.header().frame_len(), data.len() as u64);
    }
    outcome
}

/// Checks the chunk-list decoder on `data`.
pub fn check_chunk_list(data: &[u8]) -> Outcome {
    canonical(
        data,
        ChunkList::decode(data),
        ChunkList::encode,
        "chunk list",
    )
    .0
}

/// Checks the hex id parser on `data`: whatever it accepts is exactly the
/// id's lowercase hex, for every id type.
pub fn check_id_hex(data: &[u8]) -> Outcome {
    let Ok(s) = std::str::from_utf8(data) else {
        return Outcome::Rejected;
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
    let hex = |bytes: &[u8; 32]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let (outcome, bytes) = canonical(data, parsed[0].clone(), |b| hex(b).into_bytes(), "hex id");
    if let Some(bytes) = bytes {
        assert_eq!(BlobId::from_bytes(bytes).to_string(), s);
    }
    outcome
}

/// Checks the blob-record decoder on `data`.
pub fn check_blob_record(data: &[u8]) -> Outcome {
    canonical(
        data,
        derived::BlobRecord::decode(data),
        derived::BlobRecord::encode,
        "blob record",
    )
    .0
}

/// Checks the CommitGraph value decoder on `data`.
pub fn check_graph_node(data: &[u8]) -> Outcome {
    canonical(
        data,
        derived::GraphNode::decode(data),
        derived::GraphNode::encode,
        "CommitGraph value",
    )
    .0
}

/// Checks the RestrictedRoots value decoder on `data`.
pub fn check_restricted_roots(data: &[u8]) -> Outcome {
    canonical(
        data,
        derived::RestrictedRoots::decode(data),
        derived::RestrictedRoots::encode,
        "RestrictedRoots value",
    )
    .0
}
