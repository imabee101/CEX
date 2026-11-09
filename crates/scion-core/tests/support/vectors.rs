//! The golden vectors. Small cases are literal hex worked out by hand from
//! the byte layout; their ids and frame digests were computed with BLAKE3
//! alone. Large cases are built by the reference encoder. Once committed,
//! these vectors are permanent: changing one requires a format version bump.

use super::reference::{self as r, RefChange, RefChangeset, RefEntry, SplitMix};
use super::{hx, id, to_hex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ty {
    Blob,
    Chunk,
    Tree,
    Changeset,
    Frame,
    ChunkList,
}

impl Ty {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Ty::Blob => "blob",
            Ty::Chunk => "chunk",
            Ty::Tree => "tree",
            Ty::Changeset => "changeset",
            Ty::Frame => "frame",
            Ty::ChunkList => "chunk_list",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<Self> {
        [
            Ty::Blob,
            Ty::Chunk,
            Ty::Tree,
            Ty::Changeset,
            Ty::Frame,
            Ty::ChunkList,
        ]
        .into_iter()
        .find(|t| t.name() == s)
    }

    pub(crate) fn context(self) -> Option<&'static str> {
        match self {
            Ty::Blob => Some("scion v1 blob"),
            Ty::Chunk => Some("scion v1 chunk"),
            Ty::Tree => Some("scion v1 tree"),
            Ty::Changeset => Some("scion v1 changeset"),
            Ty::Frame | Ty::ChunkList => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Vector {
    pub(crate) name: String,
    pub(crate) ty: Ty,
    pub(crate) accept: bool,
    pub(crate) id: Option<[u8; 32]>,
    pub(crate) bytes: Vec<u8>,
}

fn accept(name: &str, ty: Ty, id_hex: &str, bytes: &str) -> Vector {
    Vector {
        name: name.into(),
        ty,
        accept: true,
        id: Some(id(id_hex)),
        bytes: hx(bytes),
    }
}

fn reject(name: &str, ty: Ty, bytes: &str) -> Vector {
    Vector {
        name: name.into(),
        ty,
        accept: false,
        id: None,
        bytes: hx(bytes),
    }
}

/// Everything in a minimal changeset up to its change count.
const CS_HEAD: &str = "01 00 01 78 00 00 00 00 00 00 00";

const HI_FRAME_HEAD: &str =
    "53434631 01 ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27";
const BIG_BLOB: &str = "6a0b5c0a7ea6a142a9cddda39fb8fcdaa63e3b4238ce1f669cd26e44748eed09";
const CHUNK0: &str = "d6328e383be99a1be1977473af3d76d2ac58f9f9f31a61f5707a3fec0b24505c";
const CHUNK1: &str = "b5cebad219f9c79a2517ff900e9b957c8f18d2af7820f86c848e9b9994eeeddf";

pub(crate) fn small() -> Vec<Vector> {
    use Ty::*;
    let cs_rej = |name: &str, tail: &str| reject(name, Changeset, &format!("{CS_HEAD} {tail}"));
    let hi_frame = |name: &str, rest: &str| reject(name, Frame, &format!("{HI_FRAME_HEAD} {rest}"));
    let big =
        |name: &str, rest: &str| reject(name, Frame, &format!("53434631 04 {BIG_BLOB} {rest}"));
    vec![
        // ---- blobs and chunks: the id of raw bytes.
        accept("blob-empty", Blob, "395e5ff2c5ebbdece5384a16c163976f3037d28f4b4b889bdf0c103fcd199bdc", ""),
        accept("blob-hi", Blob, "ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27", "6869"),
        accept("blob-newline", Blob, "07b970791414a6e84b2ba5dc0435503dbb2b6e93c66ff70ead342166639d34ab", "0a"),
        accept("blob-a100", Blob, "1ba20b36aadf00543883404cbf85ba653ebd3b107a676a15caf1d916ad9eb8a8", "{61*100}"),
        accept("chunk-hi", Chunk, "3d9f8f8dc0503ca198e409171a5a0365b48fd5097d1ef94dea71a420e2c1cffd", "6869"),
        accept("chunk-a100", Chunk, "8c17519b35ab651973a544b881cb0ec8288a14ec8cdfdc0f98126fac3d8d7e21", "{61*100}"),
        // ---- changesets
        accept("cs-minimal", Changeset, "8ee7780c09d89038eb04b7351595f3b5b3315865cb115a3d34246120263b3958",
            "01 00 01 78 00 00 00 00 00 00 00 00"),
        accept("cs-full", Changeset, "0c5d1e4faa201be8cffd6d6997499edbb6fc49abe63094efbea493bd0dc20351",
            "01 02 {11*32} {12*32} 07 41203c6140783e d8 04 a0 38 01 01 63 01 01 9f 38 02 6d 0a
             01 03 736967 00 03 01 61 01 {22*32} 05 03 622f63 00 03 622f64 03 {23*32} 80 01"),
        accept("cs-imported", Changeset, "8e01fa148f246cb968d426573a01a4cfc7ca5493e4f9b33b6272568725e8805f",
            "01 00 00 ff ff ff ff ff ff ff ff ff 01 fe c5 0a 01 01 63
             01 fe ff ff ff ff ff ff ff ff 01 fd c5 0a 00 00 01 01 68 01 01 78 02 {22*32} 00"),
        accept("cs-offset-max", Changeset, "8a239b7c46f0f26a3a78bf51eecf45fbb0ede416df9c5e0401b21cd849bb2e45",
            "01 00 01 78 00 fe c5 0a 00 00 00 00 00 00"),
        reject("cs-empty-input", Changeset, ""),
        reject("cs-version-0", Changeset, "00 00 01 78 00 00 00 00 00 00 00 00"),
        reject("cs-version-2", Changeset, "02 00 01 78 00 00 00 00 00 00 00 00"),
        reject("cs-truncated", Changeset, "01 00 01 78 00 00 00 00 00 00 00"),
        cs_rej("cs-trailing-byte", "00 00"),
        reject("cs-nonminimal-parent-count", Changeset, "01 80 00 01 78 00 00 00 00 00 00 00 00"),
        reject("cs-nonminimal-author-len", Changeset, "01 00 81 00 78 00 00 00 00 00 00 00 00"),
        reject("cs-nonminimal-seconds", Changeset, "01 00 01 78 80 00 00 00 00 00 00 00 00"),
        cs_rej("cs-nonminimal-change-count", "80 00"),
        cs_rej("cs-nonminimal-size", "01 01 61 01 {22*32} 85 00"),
        reject("cs-varint-overflow", Changeset, "01 00 01 78 ff ff ff ff ff ff ff ff ff 02 00 00 00 00 00 00 00"),
        reject("cs-varint-11-bytes", Changeset, "01 00 01 78 ff ff ff ff ff ff ff ff ff ff 01 00 00 00 00 00 00 00"),
        reject("cs-option-tag-2", Changeset, "01 00 01 78 00 00 02 00 00 00 00 00"),
        reject("cs-committer-without-date", Changeset, "01 00 01 78 00 00 01 01 63 00 00 00 00 00"),
        reject("cs-date-without-committer", Changeset, "01 00 01 78 00 00 00 01 00 00 00 00 00 00"),
        reject("cs-offset-86400", Changeset, "01 00 01 78 00 80 c6 0a 00 00 00 00 00 00"),
        reject("cs-offset-minus-86400", Changeset, "01 00 01 78 00 ff c5 0a 00 00 00 00 00 00"),
        reject("cs-author-over-max-field", Changeset, "01 00 81 80 80 20"),
        reject("cs-parents-truncated", Changeset, "01 02 {11*32}"),
        cs_rej("cs-change-kind-4", "01 01 61 04 {22*32} 01"),
        cs_rej("cs-unsorted-paths", "02 01 62 00 01 61 00"),
        cs_rej("cs-duplicate-path", "02 01 61 00 01 61 00"),
        cs_rej("cs-longer-path-first", "02 02 6161 00 01 61 00"),
        cs_rej("cs-deleted-with-blob", "01 01 61 00 {22*32} 05"),
        cs_rej("cs-path-empty", "01 00 00"),
        cs_rej("cs-path-leading-slash", "01 02 2f61 00"),
        cs_rej("cs-path-trailing-slash", "01 02 612f 00"),
        cs_rej("cs-path-double-slash", "01 04 612f2f62 00"),
        cs_rej("cs-path-dot", "01 01 2e 00"),
        cs_rej("cs-path-dotdot", "01 02 2e2e 00"),
        cs_rej("cs-path-inner-dotdot", "01 06 612f2e2e2f62 00"),
        cs_rej("cs-path-nul", "01 03 610062 00"),
        // ---- trees
        accept("tree-empty", Tree, "7c007800ec20b9f58d356d2136b30da8a2194d7e0bdc697c4b74d8b592f354e4", "01 00"),
        accept("tree-mixed", Tree, "5915aedbae15bf2d74c44d4eb1ecc110de6e56213d7bc2918aceef85603db159",
            "01 04 01 61 02 {22*32} 03 {33*32} 01 62 03 {23*32} 00 {34*32}
             01 64 00 {44*32} 01 6c 04 {24*32} 01 {35*32}"),
        accept("tree-shard-dot", Tree, "b67bcb8e0e72dff6a5fd7dd49ca2c979b40bc7a5d82a11a593505e067e950aac",
            "01 02 01 2e 01 {45*32} 01 61 01 {46*32}"),
        accept("tree-shard-dotdot", Tree, "7df71bebc60790abbc3e0bd57435d85de57152f80e1cd7a1396312f96e4aadd1",
            "01 02 02 2e2e 01 {45*32} 02 2e61 01 {46*32}"),
        accept("tree-shard-prefix-entry", Tree, "eb1b3e575cbe65fb4b56556ad287806a1a2ea994a1302d3f76317fb6370c27d5",
            "01 02 01 61 02 {22*32} 00 {33*32} 02 6162 01 {45*32}"),
        accept("tree-dotted-names", Tree, "3b58af338748811ece482f51810756eb0dd25a9a257ae637b58dcf8869a8e513",
            "01 02 03 2e2e2e 00 {44*32} 02 2e61 02 {22*32} 00 {33*32}"),
        accept("tree-size-multibyte", Tree, "be7f3a78de2686331b0274d79bbf1a3d4241fe760d4b6f3f2be2f34ac10151c7",
            "01 01 01 66 02 {22*32} ff ff 03 {33*32}"),
        reject("tree-empty-input", Tree, ""),
        reject("tree-version-2", Tree, "02 00"),
        reject("tree-trailing-byte", Tree, "01 00 00"),
        reject("tree-truncated", Tree, "01 01"),
        reject("tree-nonminimal-count", Tree, "01 80 00"),
        reject("tree-count-exceeds-input", Tree, "01 02 01 61 00 {44*32}"),
        reject("tree-unsorted", Tree, "01 02 01 62 00 {44*32} 01 61 00 {44*32}"),
        reject("tree-duplicate-name", Tree, "01 02 01 61 00 {44*32} 01 61 00 {45*32}"),
        reject("tree-kind-5", Tree, "01 01 01 61 05 {44*32}"),
        reject("tree-name-dot", Tree, "01 01 01 2e 00 {44*32}"),
        reject("tree-file-name-dotdot", Tree, "01 01 02 2e2e 02 {22*32} 00 {33*32}"),
        reject("tree-name-slash", Tree, "01 01 03 612f62 00 {44*32}"),
        reject("tree-name-empty", Tree, "01 01 00 00 {44*32}"),
        reject("tree-name-nul", Tree, "01 01 02 6100 00 {44*32}"),
        reject("tree-shard-name-slash", Tree, "01 01 03 612f62 01 {44*32}"),
        reject("tree-shard-name-empty", Tree, "01 01 00 01 {44*32}"),
        reject("tree-shard-unequal-lengths", Tree, "01 02 01 2e 01 {45*32} 02 2e2e 01 {46*32}"),
        reject("tree-shard-prefixes-differ", Tree, "01 02 02 6162 01 {45*32} 02 6263 01 {46*32}"),
        reject("tree-shard-beside-file", Tree, "01 02 01 61 01 {45*32} 01 62 02 {22*32} 00 {33*32}"),
        reject("tree-shard-beside-prefix-and-dir", Tree,
            "01 03 01 61 02 {22*32} 00 {33*32} 02 6162 00 {44*32} 02 6163 01 {45*32}"),
        reject("tree-single-shard", Tree, "01 01 01 61 01 {45*32}"),
        reject("tree-dir-with-size", Tree, "01 01 01 64 00 {44*32} 00 {33*32}"),
        reject("tree-file-missing-sha", Tree, "01 01 01 66 02 {22*32} 00"),
        reject("tree-nonminimal-size", Tree, "01 01 01 66 02 {22*32} 80 00 {33*32}"),
        // ---- frames: header, payload, BLAKE3 digest of both
        accept("frame-blob-raw", Frame, "ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27",
            &format!("{HI_FRAME_HEAD} 00 02 02 6869 45c895bcb7f7387b36465667bdf8ae3c1d3558af3fd1c8a4b456e21647d1a475")),
        accept("frame-blob-empty", Frame, "395e5ff2c5ebbdece5384a16c163976f3037d28f4b4b889bdf0c103fcd199bdc",
            "53434631 01 395e5ff2c5ebbdece5384a16c163976f3037d28f4b4b889bdf0c103fcd199bdc 00 00 00
             40f9cc94916ccec99361e7b2da1814b322dba4b3bf4e09405faecfac74c786a3"),
        accept("frame-tree", Frame, "7c007800ec20b9f58d356d2136b30da8a2194d7e0bdc697c4b74d8b592f354e4",
            "53434631 03 7c007800ec20b9f58d356d2136b30da8a2194d7e0bdc697c4b74d8b592f354e4 00 02 02 0100
             7910780432e543a2e4533416304ccaa1680b4f9e49b6c6aefb81cf46c34e9073"),
        // The payload is zstd's compression of 100 bytes of `a`.
        accept("frame-chunk-zstd", Frame, "8c17519b35ab651973a544b881cb0ec8288a14ec8cdfdc0f98126fac3d8d7e21",
            "53434631 02 8c17519b35ab651973a544b881cb0ec8288a14ec8cdfdc0f98126fac3d8d7e21 01 64 11
             28b52ffd206445000010616101003f012c
             39293f0e27c18c0537bfc4234e20eb76c847701bf1ef0f0260342d3501cfc336"),
        // A blob of 4 MiB + 1 zero bytes: two chunks.
        accept("frame-chunk-list", Frame, BIG_BLOB,
            &format!("53434631 04 {BIG_BLOB} 00 81808002 41 02 {CHUNK0} {CHUNK1}
             93b001f77fd1b96679111a51bbf0f3130bc46c620e43bafc4887f5c68c7e25b8")),
        reject("frame-empty-input", Frame, ""),
        reject("frame-header-truncated", Frame, "53434631 01 ef69ad061737"),
        reject("frame-bad-magic", Frame,
            "53434632 01 ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27 00 02 02 6869
             1f15a1291f9375bcaa4a86d25c856da1c60980a686d2830a9cc3e85da7836c6c"),
        reject("frame-kind-0", Frame,
            "53434631 00 ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27 00 02 02 6869
             c09d2b2a59657c84c5d52c24c063528ed118e75c04a49a757987ca67f55909da"),
        reject("frame-kind-5", Frame,
            "53434631 05 ef69ad061737ab7a417292a7a2b3cc7a17fd7946beb32678d4804224b92e1c27 00 02 02 6869
             8b9c9d4520af3b464cf1e6b7d7321728e0ded7871f92ef2699304f5722b411ed"),
        hi_frame("frame-codec-2", "02 02 02 6869 a392a0bdf3fd61108a2f9f0996f113c1d84d2cde8017d28e3ac918a7f40a2cc1"),
        hi_frame("frame-digest-mismatch", "00 02 02 6869 45c895bcb7f7387b36465667bdf8ae3c1d3558af3fd1c8a4b456e21647d1a474"),
        hi_frame("frame-trailing-byte", "00 02 02 6869 45c895bcb7f7387b36465667bdf8ae3c1d3558af3fd1c8a4b456e21647d1a475 00"),
        hi_frame("frame-truncated", "00 02 02 6869 45c895bcb7f7387b36465667bdf8ae3c1d3558af3fd1c8a4b456e21647d1a4"),
        hi_frame("frame-raw-len-mismatch", "00 03 02 6869 fa714fcd7ef73b4252c5a143eefceeb9134a0edd1625692abcdff29ee3ecf266"),
        hi_frame("frame-zstd-not-smaller", "01 02 02 6869 2431296543a1faed514fb514ff1b73f0f609eafbef772d0a9694acd21ebe5f99"),
        hi_frame("frame-nonminimal-raw-len", "00 8200 02 6869 92bed6efbbcb11c46a75c9fa5dd14ba53f1df1ce975811ccc6844eadca6fa527"),
        hi_frame("frame-nonminimal-payload-len", "00 02 8200 6869 6fe9c058d99d1399545114a20354c3a89db7153c8be9379ae5440cdb56224176"),
        reject("frame-blob-over-chunk-bytes", Frame,
            &format!("53434631 01 {BIG_BLOB} 01 81808002 03 7a7a7a
             426bfd0492716c8ca4079dde6c0fdeb4ff1edea2bda6bb9be89c03c9fe010d75")),
        reject("frame-chunk-empty", Frame,
            "53434631 02 8c17519b35ab651973a544b881cb0ec8288a14ec8cdfdc0f98126fac3d8d7e21 00 00 00
             91d79d008a137ecf894fe52d76d60f77d43e448697b93f5110f8bcf8c88b8f3b"),
        big("frame-chunk-list-small-blob",
            &format!("00 80808002 21 01 {CHUNK0} dd8066deff4b870686394641e4f966ef278f24470789145fff3b04ac6a5a553d")),
        big("frame-chunk-list-short",
            &format!("00 81808002 21 01 {CHUNK0} 4c0e5a77c8add3c8a4d6cb1cf64cb1e5494f16ef6b4bbbf53ad983036969d623")),
        big("frame-chunk-list-count-below-chunks",
            &format!("00 81808002 41 01 {CHUNK0} {CHUNK1} ca25ddbc28fc59fdd8515c09e05b8f8551da1e5814e36b52707461db1ab1e6a8")),
        big("frame-chunk-list-zstd",
            &format!("01 81808002 41 02 {CHUNK0} {CHUNK1} 4ee14b58b90113e8c70cb8d571ca34a36f00eaad269e32591f08e74bcb99a7ff")),
    ]
}

fn derive(ty: Ty, bytes: &[u8]) -> [u8; 32] {
    blake3::derive_key(ty.context().expect("only object types have ids"), bytes)
}

fn built(name: &str, ty: Ty, bytes: Vec<u8>, ok: bool) -> Vector {
    Vector {
        name: name.into(),
        ty,
        accept: ok,
        id: (ok && ty.context().is_some()).then(|| derive(ty, &bytes)),
        bytes,
    }
}

/// A valid path of `depth` components from `rng`, avoiding `/`, NUL, and
/// dot-only components.
fn path(rng: &mut SplitMix, depth: u64) -> Vec<u8> {
    let mut p = Vec::new();
    for i in 0..depth {
        if i > 0 {
            p.push(b'/');
        }
        let len = 1 + rng.below(12);
        for _ in 0..len {
            const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_.ABC\xc3\xa9";
            p.push(ALPHABET[rng.below(ALPHABET.len() as u64) as usize]);
        }
        if p.ends_with(b"/.") || p == b"." || p.ends_with(b"..") {
            p.push(b'x');
        }
    }
    p
}

fn large_changeset() -> RefChangeset {
    let mut rng = SplitMix(0x5c10_0001);
    let mut changes = std::collections::BTreeMap::new();
    while changes.len() < 1000 {
        let depth = 1 + rng.below(6);
        let p = path(&mut rng, depth);
        let change = match rng.below(4) {
            0 => RefChange::Deleted,
            k => RefChange::File(k as u8, rng.id(), rng.next() >> rng.below(64)),
        };
        changes.insert(p, change);
    }
    // One long path whose length needs a two-byte varint.
    changes.insert(
        [b"z".repeat(200), b"y".repeat(100)].join(&b'/'),
        RefChange::Deleted,
    );
    RefChangeset {
        parents: (0..64).map(|_| rng.id()).collect(),
        author: rng.bytes(300),
        author_date: (-(rng.next() as i64 >> 3), -86_399),
        committer: Some((rng.bytes(128), (rng.next() as i64, 86_399))),
        message: rng.bytes(20_000),
        signature: Some(rng.bytes(127)),
        git_headers: Some(rng.bytes(16_384)),
        changes: changes.into_iter().collect(),
    }
}

fn tree_entries(n: usize) -> Vec<(Vec<u8>, RefEntry)> {
    let mut rng = SplitMix(0x5c10_0002);
    (0..n)
        .map(|i| {
            // Two-byte names from 0x40..=0x7f: valid, distinct, and sorted.
            let name = vec![0x40 + (i / 64) as u8, 0x40 + (i % 64) as u8];
            let entry = match i % 16 {
                0 => RefEntry::File(2 + (i / 16 % 3) as u8, rng.id(), rng.next() >> 20, rng.id()),
                _ => RefEntry::Tree(rng.id()),
            };
            (name, entry)
        })
        .collect()
}

pub(crate) fn large() -> Vec<Vector> {
    let cs = large_changeset();
    let mut unsorted = cs.clone();
    let last = unsorted.changes.len() - 1;
    unsorted.changes.swap(0, last);
    let mut rng = SplitMix(0x5c10_0003);
    let blob = rng.bytes(5000);
    let mut too_many = tree_entries(4096);
    too_many.push((vec![0x80, 0x40], RefEntry::Tree(rng.id())));
    let chunk = rng.bytes(3000);
    let chunk_id = derive(Ty::Chunk, &chunk);
    vec![
        built("blob-random-5000", Ty::Blob, blob, true),
        built("cs-large", Ty::Changeset, r::changeset(&cs), true),
        built(
            "cs-large-unsorted",
            Ty::Changeset,
            r::changeset(&unsorted),
            false,
        ),
        built(
            "tree-max-entries",
            Ty::Tree,
            r::tree(&tree_entries(4096)),
            true,
        ),
        built("tree-over-max-entries", Ty::Tree, r::tree(&too_many), false),
        Vector {
            name: "frame-chunk-raw-3000".into(),
            ty: Ty::Frame,
            accept: true,
            id: Some(chunk_id),
            bytes: r::frame(2, &chunk_id, 0, 3000, &chunk),
        },
    ]
    .into_iter()
    .chain(more_changesets())
    .chain(length_rules())
    .chain(chunk_lists())
    .collect()
}

fn minimal_changeset(changes: Vec<(&[u8], RefChange)>) -> Vec<u8> {
    r::changeset(&RefChangeset {
        author: b"x".to_vec(),
        changes: changes.into_iter().map(|(p, c)| (p.to_vec(), c)).collect(),
        ..RefChangeset::default()
    })
}

/// Changesets beyond the hand-written ones: option tags past the committer,
/// and paths whose components merely contain dots.
fn more_changesets() -> Vec<Vector> {
    let file = || RefChange::File(1, [0x22; 32], 1);
    vec![
        reject(
            "cs-signature-tag-2",
            Ty::Changeset,
            "01 00 01 78 00 00 00 00 00 02 00 00",
        ),
        reject(
            "cs-git-headers-tag-2",
            Ty::Changeset,
            "01 00 01 78 00 00 00 00 00 00 02 00",
        ),
        built(
            "cs-dotted-path-names",
            Ty::Changeset,
            minimal_changeset(vec![(b".a/..b/...", file())]),
            true,
        ),
    ]
}

/// Frame length rules not covered by the hand-written frames, each in an
/// otherwise well-formed frame with a correct digest.
fn length_rules() -> Vec<Vector> {
    let chunk = CHUNK_BYTES_U64;
    let id = [0x5a; 32];
    let empty_blob = derive(Ty::Blob, b"");
    let frame = |name: &str, kind, codec, raw_len, payload: &[u8]| Vector {
        name: name.into(),
        ty: Ty::Frame,
        accept: false,
        id: None,
        bytes: r::frame(kind, &id, codec, raw_len, payload),
    };
    vec![
        frame("frame-chunk-over-chunk-bytes", 2, 1, chunk + 1, b"zzz"),
        frame("frame-tree-over-max-field", 3, 1, (64 << 20) + 1, b"zzz"),
        Vector {
            name: "frame-blob-zstd-empty".into(),
            ty: Ty::Frame,
            accept: false,
            id: None,
            bytes: r::frame(1, &empty_blob, 1, 0, b""),
        },
    ]
}

const CHUNK_BYTES_U64: u64 = 4 << 20;

/// Chunk-list payloads on their own, and a frame whose list needs a two-byte
/// count.
fn chunk_lists() -> Vec<Vector> {
    let chunk0 = id(CHUNK0);
    let chunk1 = id(CHUNK1);
    let list = |count: &[u8], ids: &[[u8; 32]]| {
        let mut out = count.to_vec();
        for i in ids {
            out.extend_from_slice(i);
        }
        out
    };
    let item = |name: &str, accept, bytes| Vector {
        name: name.into(),
        ty: Ty::ChunkList,
        accept,
        id: None,
        bytes,
    };
    // A blob of 128 chunks of zeros: every chunk is CHUNK0, the first chunk
    // of the 4 MiB + 1 zero-byte blob above.
    let zeros = vec![0; CHUNK_BYTES_U64 as usize];
    assert_eq!(derive(Ty::Chunk, &zeros), chunk0);
    let mut blob = blake3::Hasher::new_derive_key(Ty::Blob.context().unwrap());
    for _ in 0..128 {
        blob.update(&zeros);
    }
    let blob_id: [u8; 32] = blob.finalize().into();
    let list_128 = list(&[0x80, 0x01], &[chunk0; 128]);
    vec![
        item("chunk-list-two", true, list(&[2], &[chunk0, chunk1])),
        item("chunk-list-empty", true, list(&[0], &[])),
        item("chunk-list-128", true, list_128.clone()),
        item(
            "chunk-list-nonminimal-count",
            false,
            list(&[0x82, 0x00], &[chunk0, chunk1]),
        ),
        item(
            "chunk-list-trailing-id",
            false,
            list(&[1], &[chunk0, chunk1]),
        ),
        item(
            "chunk-list-truncated",
            false,
            list(&[2], &[chunk0, chunk1])[..64].to_vec(),
        ),
        item(
            "chunk-list-count-over-input",
            false,
            list(&[3], &[chunk0, chunk1]),
        ),
        Vector {
            name: "frame-chunk-list-128".into(),
            ty: Ty::Frame,
            accept: true,
            id: Some(blob_id),
            bytes: r::frame(4, &blob_id, 0, 128 * CHUNK_BYTES_U64, &list_128),
        },
    ]
}

pub(crate) fn all() -> Vec<Vector> {
    let mut v = small();
    v.extend(large());
    v
}

pub(crate) const FILE_HEADER: &str = "\
# Scion v1 format test vectors.
#
# Records are separated by blank lines; lines starting with '#' are comments.
# Each record has, in order: name, type
# (blob|chunk|tree|changeset|frame|chunk_list), expect (accept|reject), id
# (accepted records other than chunk_list only), and bytes (lowercase hex,
# possibly empty). See scion-core's crate documentation for the formats
# and for what accepting a record means.
";

pub(crate) fn render(vectors: &[Vector]) -> String {
    let mut out = String::from(FILE_HEADER);
    for v in vectors {
        out.push('\n');
        out.push_str(&format!("name: {}\ntype: {}\n", v.name, v.ty.name()));
        out.push_str(if v.accept {
            "expect: accept\n"
        } else {
            "expect: reject\n"
        });
        if let Some(id) = v.id {
            out.push_str(&format!("id: {}\n", to_hex(&id)));
        }
        out.push_str(&format!("bytes: {}\n", to_hex(&v.bytes)));
    }
    out
}

/// Parses the language-neutral file. Strict: unknown keys, missing keys, or
/// keys out of order are errors.
pub(crate) fn parse(text: &str) -> Result<Vec<Vector>, String> {
    let mut out = Vec::new();
    let mut record: Vec<(String, String)> = Vec::new();
    let lines = text.split('\n').map(|l| l.strip_suffix('\r').unwrap_or(l));
    for line in lines.chain(std::iter::once("")) {
        if line.starts_with('#') {
            continue;
        }
        if line.is_empty() {
            if !record.is_empty() {
                out.push(parse_record(&std::mem::take(&mut record))?);
            }
            continue;
        }
        let (k, v) = line
            .split_once(": ")
            .or_else(|| line.strip_suffix(':').map(|k| (k, "")))
            .ok_or(format!("bad line {line:?}"))?;
        record.push((k.to_owned(), v.to_owned()));
    }
    Ok(out)
}

fn parse_record(rec: &[(String, String)]) -> Result<Vector, String> {
    let keys: Vec<&str> = rec.iter().map(|(k, _)| k.as_str()).collect();
    let get = |k: &str| {
        rec.iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.as_str())
    };
    let accept = match get("expect") {
        Some("accept") => true,
        Some("reject") => false,
        e => return Err(format!("bad expect {e:?}")),
    };
    let ty = Ty::parse(get("type").unwrap_or_default()).ok_or("bad type")?;
    let want: &[&str] = if accept && ty != Ty::ChunkList {
        &["name", "type", "expect", "id", "bytes"]
    } else {
        &["name", "type", "expect", "bytes"]
    };
    if keys != want {
        return Err(format!("record keys {keys:?}, expected {want:?}"));
    }
    let hex = |s: &str| -> Result<Vec<u8>, String> {
        if !s.len().is_multiple_of(2)
            || !s
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(format!("bad hex {s:?}"));
        }
        Ok(hx(s))
    };
    Ok(Vector {
        name: get("name").unwrap().to_owned(),
        ty,
        accept,
        id: match get("id") {
            Some(s) => Some(hex(s)?.try_into().map_err(|_| "id is not 32 bytes")?),
            None => None,
        },
        bytes: hex(get("bytes").unwrap())?,
    })
}
