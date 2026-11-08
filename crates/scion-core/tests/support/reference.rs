//! A test-only reference encoder written directly from the byte layout in
//! CLAUDE.md. It shares no code with scion-core and uses plain bytes, so a
//! bug in scion-core's encoder cannot leak into the expected values.

/// Unsigned LEB128, written group by group from the most significant end
/// down, as a cross-check on the usual low-to-high loop.
pub(crate) fn uvarint(out: &mut Vec<u8>, v: u64) {
    let mut groups = Vec::new();
    let mut rest = v;
    loop {
        groups.push((rest & 0x7f) as u8);
        rest >>= 7;
        if rest == 0 {
            break;
        }
    }
    let last = groups.len() - 1;
    for (i, g) in groups.into_iter().enumerate() {
        out.push(if i == last { g } else { g | 0x80 });
    }
    maybe_pad(out);
}

pub(crate) fn svarint(out: &mut Vec<u8>, v: i64) {
    let z = if v >= 0 {
        (v as u64) * 2
    } else {
        // -1 -> 1, -2 -> 3, ..., i64::MIN -> u64::MAX
        (!(v as u64)) * 2 + 1
    };
    uvarint(out, z);
}

pub(crate) fn bytes(out: &mut Vec<u8>, b: &[u8]) {
    uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub(crate) fn timestamp(out: &mut Vec<u8>, (secs, offset): (i64, i64)) {
    svarint(out, secs);
    svarint(out, offset);
}

#[derive(Clone, Debug)]
pub(crate) enum RefChange {
    Deleted,
    /// kind code 1 Regular, 2 Executable, 3 Symlink.
    File(u8, [u8; 32], u64),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RefChangeset {
    pub(crate) parents: Vec<[u8; 32]>,
    pub(crate) author: Vec<u8>,
    pub(crate) author_date: (i64, i64),
    pub(crate) committer: Option<(Vec<u8>, (i64, i64))>,
    pub(crate) message: Vec<u8>,
    pub(crate) signature: Option<Vec<u8>>,
    pub(crate) git_headers: Option<Vec<u8>>,
    /// Must already be in strictly increasing path byte order.
    pub(crate) changes: Vec<(Vec<u8>, RefChange)>,
}

pub(crate) fn changeset(c: &RefChangeset) -> Vec<u8> {
    let mut out = vec![0x01];
    uvarint(&mut out, c.parents.len() as u64);
    for p in &c.parents {
        out.extend_from_slice(p);
    }
    bytes(&mut out, &c.author);
    timestamp(&mut out, c.author_date);
    match &c.committer {
        Some((ident, date)) => {
            out.push(0x01);
            bytes(&mut out, ident);
            out.push(0x01);
            timestamp(&mut out, *date);
        }
        None => {
            out.push(0x00);
            out.push(0x00);
        }
    }
    bytes(&mut out, &c.message);
    for field in [&c.signature, &c.git_headers] {
        if let Some(b) = field {
            out.push(0x01);
            bytes(&mut out, b);
        } else {
            out.push(0x00);
        }
    }
    uvarint(&mut out, c.changes.len() as u64);
    for (path, change) in &c.changes {
        bytes(&mut out, path);
        match change {
            RefChange::Deleted => out.push(0),
            RefChange::File(kind, blob, size) => {
                out.push(*kind);
                out.extend_from_slice(blob);
                uvarint(&mut out, *size);
            }
        }
    }
    out
}

#[derive(Clone, Debug)]
pub(crate) enum RefEntry {
    Tree([u8; 32]),
    Shard([u8; 32]),
    /// kind code 2 Regular, 3 Executable, 4 Symlink.
    File(u8, [u8; 32], u64, [u8; 32]),
}

/// Entries must already be in strictly increasing name byte order.
pub(crate) fn tree(entries: &[(Vec<u8>, RefEntry)]) -> Vec<u8> {
    let mut out = vec![0x01];
    uvarint(&mut out, entries.len() as u64);
    for (name, e) in entries {
        bytes(&mut out, name);
        match e {
            RefEntry::Tree(id) => {
                out.push(0);
                out.extend_from_slice(id);
            }
            RefEntry::Shard(id) => {
                out.push(1);
                out.extend_from_slice(id);
            }
            RefEntry::File(kind, id, size, sha) => {
                out.push(*kind);
                out.extend_from_slice(id);
                uvarint(&mut out, *size);
                out.extend_from_slice(sha);
            }
        }
    }
    out
}

pub(crate) fn frame(kind: u8, id: &[u8; 32], codec: u8, raw_len: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = b"SCF1".to_vec();
    out.push(kind);
    out.extend_from_slice(id);
    out.push(codec);
    uvarint(&mut out, raw_len);
    uvarint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    let digest = blake3::hash(&out);
    out.extend_from_slice(digest.as_bytes());
    out
}

/// A deterministic pseudo-random generator (SplitMix64) for building large
/// vectors without a dependency.
pub(crate) struct SplitMix(pub u64);

impl SplitMix {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    pub(crate) fn id(&mut self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for chunk in out.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes());
        }
        out
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

thread_local! {
    static PAD_AT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static VARINTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Runs `f` with the `k`-th varint it writes (counting from 0) padded with a
/// redundant `0x80 .. 0x00` continuation, the smallest non-minimal form.
/// Returns the output and how many varints were written.
pub(crate) fn with_padded_varint<T>(k: Option<usize>, f: impl FnOnce() -> T) -> (T, usize) {
    PAD_AT.with(|p| p.set(k));
    VARINTS.with(|c| c.set(0));
    let out = f();
    PAD_AT.with(|p| p.set(None));
    (out, VARINTS.with(|c| c.get()))
}

pub(crate) fn maybe_pad(out: &mut Vec<u8>) {
    let n = VARINTS.with(|c| {
        let n = c.get();
        c.set(n + 1);
        n
    });
    if PAD_AT.with(|p| p.get()) == Some(n) {
        *out.last_mut().expect("a varint was written") |= 0x80;
        out.push(0x00);
    }
}
