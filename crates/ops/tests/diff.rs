//! Diff on Postgres and the object store: changed paths equal a naive
//! comparison of the two file maps, line counts and patches are consistent
//! with the contents (every patch applies to the old text and yields the new
//! one), binary files are flagged, and path filters select exactly the
//! files under them.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use ops::{DiffMode, FileDiff};
use scion_core::{ChangesetId, FileKind, MAX_TREE_ENTRIES, RepoPath};
use store::{BlobStore, MetaStore};

/// Applies a unified patch to `old`.
fn apply_patch(old: &[u8], patch: &[u8]) -> Vec<u8> {
    let old_lines: Vec<&[u8]> = old.split_inclusive(|&b| b == b'\n').collect();
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut lines = patch.split_inclusive(|&b| b == b'\n').peekable();
    while let Some(line) = lines.next() {
        if line.starts_with(b"@@ -") {
            let header = String::from_utf8_lossy(line);
            let old_range = header.split(' ').nth(1).unwrap().trim_start_matches('-');
            let mut it = old_range.split(',');
            let start: usize = it.next().unwrap().parse().unwrap();
            let len: usize = it.next().unwrap().parse().unwrap();
            let start = if len == 0 { start } else { start - 1 };
            for l in &old_lines[pos..start] {
                out.extend_from_slice(l);
            }
            pos = start;
            continue;
        }
        let mut body = line[1..].to_vec();
        if lines
            .peek()
            .is_some_and(|n| n.starts_with(b"\\ No newline"))
        {
            lines.next();
            body.pop();
        }
        match line[0] {
            b' ' => {
                assert_eq!(old_lines[pos], body.as_slice());
                out.extend_from_slice(&body);
                pos += 1;
            }
            b'-' => {
                assert_eq!(old_lines[pos], body.as_slice());
                pos += 1;
            }
            b'+' => out.extend_from_slice(&body),
            other => panic!("unexpected patch line {other}"),
        }
    }
    for l in &old_lines[pos..] {
        out.extend_from_slice(l);
    }
    out
}
/// A random cursor: any bytes from the names' alphabet, valid or not.
fn random_cursor(rng: &mut Rng) -> Vec<u8> {
    const ALPHABET: &[u8] = b"a-.x0/bcwn1\0";
    (0..rng.below(9))
        .map(|_| ALPHABET[rng.index(ALPHABET.len())])
        .collect()
}

/// Random path filters: none, or a few of the files, their directories,
/// paths beneath files, and paths that exist on neither side.
fn random_filters(rng: &mut Rng, ma: &Model, mb: &Model, wide: u64) -> Vec<RepoPath> {
    if rng.one_in(3) {
        return Vec::new();
    }
    let files: Vec<&Vec<u8>> = ma.files.keys().chain(mb.files.keys()).collect();
    let mut out = BTreeSet::new();
    for _ in 0..1 + rng.below(3) {
        let p = match rng.below(4) {
            0 if !files.is_empty() => files[rng.index(files.len())].clone(),
            1 if !files.is_empty() => {
                let f = files[rng.index(files.len())];
                let cut = f.iter().position(|&b| b == b'/').unwrap_or(f.len());
                f[..cut].to_vec()
            }
            2 if !files.is_empty() => {
                let mut f = files[rng.index(files.len())].clone();
                f.extend_from_slice(b"/a");
                f
            }
            _ => random_path(rng, 4, wide),
        };
        out.insert(p);
    }
    out.into_iter().map(|p| RepoPath::new(p).unwrap()).collect()
}

fn kept(p: &[u8], filters: &[RepoPath]) -> bool {
    filters.is_empty()
        || filters.iter().any(|f| {
            let f = f.as_bytes();
            p == f || (p.len() > f.len() && p.starts_with(f) && p[f.len()] == b'/')
        })
}

fn is_text(c: &[u8]) -> bool {
    !c[..c.len().min(ops::blob::TEXT_SNIFF_BYTES)].contains(&0)
}

/// The model's diff: every kept path whose (kind, content) differs.
fn model_diff(ma: &Model, mb: &Model, filters: &[RepoPath], after: &[u8]) -> Vec<Vec<u8>> {
    ma.files
        .keys()
        .chain(mb.files.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|p| ma.files.get(*p) != mb.files.get(*p))
        .filter(|p| kept(p, filters) && p.as_slice() > after)
        .cloned()
        .collect()
}

/// Every page from `after` on, checking that each page but the last is
/// full and that only the last has no cursor.
async fn pages<M: MetaStore, B: BlobStore>(
    o: &ops::Ops<M, B>,
    (a, b): (ChangesetId, ChangesetId),
    mode: DiffMode,
    paths: &[RepoPath],
    mut after: Option<ops::DiffCursor>,
    limit: usize,
) -> Vec<FileDiff> {
    let mut files = Vec::new();
    loop {
        let options = ops::DiffOptions {
            mode,
            paths,
            after: after.as_ref(),
            limit,
        };
        let page = o.diff(REPO, a, b, &options).await.unwrap();
        match page.next {
            Some(next) => {
                assert_eq!(page.files.len(), limit, "a page before the last is short");
                assert_eq!(next.to_bytes(), page.files.last().unwrap().path);
                after = Some(ops::DiffCursor::from_bytes(&next.to_bytes()).unwrap());
                files.extend(page.files);
            }
            None => {
                assert!(page.files.len() <= limit);
                assert!(
                    !page.files.is_empty() || files.is_empty(),
                    "the last page is empty though an earlier one is not"
                );
                files.extend(page.files);
                return files;
            }
        }
    }
}

/// Checks one diff of `a` against `b` with random filters, page size, and
/// mode against the models, then forged cursors, then which trees a cold
/// server reads for it.
async fn check_pair(
    e: &Env,
    o: &ops::Ops<store::PgMetaStore, store::ObjectBlobStore>,
    rng: &mut Rng,
    (a, ma): (ChangesetId, &Model),
    (b, mb): (ChangesetId, &Model),
    wide: u64,
    label: &str,
) {
    let filters = random_filters(rng, ma, mb, wide);
    let mode = [DiffMode::Names, DiffMode::Stat, DiffMode::Patch][rng.index(3)];
    let want = model_diff(ma, mb, &filters, b"");
    // Small pages, and pages near and at the maximum; a diff of thousands
    // of files is read in at most about 64 pages, to keep the test fast.
    let limit = [1, 2, 3, 7, ops::MAX_DIFF_PAGE - 1, ops::MAX_DIFF_PAGE][rng.index(6)]
        .max(want.len() / 64)
        .min(ops::MAX_DIFF_PAGE);
    let label = format!("{label} filters {filters:?} page {limit} {mode:?}");
    let got = pages(o, (a, b), mode, &filters, None, limit).await;
    assert_eq!(
        got.iter().map(|f| f.path.clone()).collect::<Vec<_>>(),
        want,
        "{label}"
    );
    let full_patch = if mode == DiffMode::Stat {
        pages(
            o,
            (a, b),
            DiffMode::Patch,
            &filters,
            None,
            ops::MAX_DIFF_PAGE,
        )
        .await
    } else {
        Vec::new()
    };
    for (k, f) in got.iter().enumerate() {
        let p = String::from_utf8_lossy(&f.path);
        let state = |m: &Model| {
            m.files.get(&f.path).map(|(k, c)| ops::FileState {
                kind: *k,
                blob: scion_core::BlobId::hash(c),
                size: c.len() as u64,
            })
        };
        assert_eq!((f.old, f.new), (state(ma), state(mb)), "{label}: {p}");
        let content = |m: &Model| m.files.get(&f.path).map(|(_, c)| c.clone());
        let (old, new) = (content(ma), content(mb));
        if mode == DiffMode::Names {
            assert!(!f.binary && f.patch.is_empty() && f.added_lines + f.removed_lines == 0);
            continue;
        }
        let binary = [&old, &new].into_iter().flatten().any(|c| !is_text(c));
        assert_eq!(f.binary, binary, "{label}: {p}");
        if binary {
            assert!(f.patch.is_empty() && f.added_lines + f.removed_lines == 0);
            continue;
        }
        let (old, new) = (old.unwrap_or_default(), new.unwrap_or_default());
        match mode {
            DiffMode::Patch => {
                assert_eq!(apply_patch(&old, &f.patch), new, "{label}: {p}");
                let count = |prefix: u8| {
                    f.patch
                        .split(|&b| b == b'\n')
                        .filter(|l| l.first() == Some(&prefix))
                        .count() as u64
                };
                assert_eq!((f.added_lines, f.removed_lines), (count(b'+'), count(b'-')));
            }
            _ => {
                assert!(f.patch.is_empty());
                let full = &full_patch[k];
                assert_eq!(
                    (f.added_lines, f.removed_lines),
                    (full.added_lines, full.removed_lines),
                    "{label}: {p}"
                );
            }
        }
    }
    // A cursor a client forges returns exactly the entries after it, or is
    // refused as an invalid argument.
    for _ in 0..3 {
        let bytes = random_cursor(rng);
        match ops::DiffCursor::from_bytes(&bytes) {
            Ok(cursor) => {
                let got = pages(o, (a, b), DiffMode::Names, &filters, Some(cursor), limit).await;
                assert_eq!(
                    got.into_iter().map(|f| f.path).collect::<Vec<_>>(),
                    model_diff(ma, mb, &filters, &bytes),
                    "{label}: after {bytes:?}"
                );
            }
            Err(err) => {
                assert!(RepoPath::new(bytes.as_slice()).is_err(), "{bytes:?}");
                assert_eq!(err.code, ops::Code::InvalidArgument, "{bytes:?}");
            }
        }
    }
    // A cold server reads only the tree nodes of directories that hold a
    // filter or lie under one.
    if !filters.is_empty() {
        let reads = Arc::new(TreeReads::new(e.meta.clone()));
        let cold = ops(&reads, &e.blobs);
        pages(&cold, (a, b), DiffMode::Names, &filters, None, limit).await;
        let mut allowed = BTreeSet::new();
        for m in [ma, mb] {
            let files = m.files.iter().map(|(p, (k, c))| (p.clone(), *k, c.clone()));
            for (dir, ids) in reference_nodes(files).1 {
                let leads_to_filter = filters.iter().any(|f| {
                    let f = f.as_bytes();
                    f.len() > dir.len() && f.starts_with(&dir) && f[dir.len()] == b'/'
                });
                if dir.is_empty() || kept(&dir, &filters) || leads_to_filter {
                    allowed.extend(ids);
                }
            }
        }
        let read = reads.take();
        for t in read {
            assert!(
                allowed.contains(&t),
                "{label}: read a tree outside the filters"
            );
        }
    }
}

/// Diffs between random pairs of changesets of random histories equal a
/// naive comparison of the two file maps, with random path filters, page
/// sizes, modes, and forged cursors; patches apply to the old text and give
/// the new one; and a filtered diff reads no tree outside its filters.
async fn naive_comparison(seeds: &[u64], wide: u64) {
    let env = env().await;
    let o = ops(&env.meta, &env.blobs);
    let (mut merges, mut swaps) = (0, 0);
    for &seed in seeds {
        let mut rng = Rng(0xd1ff_0000 + seed);
        let label = format!("seed {seed}");
        let h = random_history(&o, &mut rng, 16, (4, wide), &label).await;
        let (history, m) = (h.steps, h.merges);
        merges += m;
        // Paths that are a file in one changeset and a directory in another.
        let files: BTreeSet<&Vec<u8>> = history.iter().flat_map(|(_, m)| m.files.keys()).collect();
        swaps += files
            .iter()
            .filter(|f| {
                let mut under = f.to_vec();
                under.push(b'/');
                files.iter().any(|g| g.starts_with(&under))
            })
            .count();
        for _ in 0..16 {
            let x = rng.index(history.len());
            // Old against recent, neighbours, and a changeset with itself.
            let y = match rng.below(3) {
                0 => history.len() - 1 - rng.index(history.len().min(3)),
                1 => x.saturating_sub(rng.index(3)),
                _ => rng.index(history.len()),
            };
            let (a, b) = if rng.one_in(2) { (x, y) } else { (y, x) };
            check_pair(
                &env,
                &o,
                &mut rng,
                (history[a].0, &history[a].1),
                (history[b].0, &history[b].1),
                wide,
                &label,
            )
            .await;
        }
    }
    assert!(swaps > 0, "no path was both a file and a directory");
    assert!(merges > 0, "no merges");
}

#[tokio::test]
async fn diffs_with_a_sharded_directory_equal_a_naive_comparison() {
    naive_comparison(&[0], MAX_TREE_ENTRIES as u64 + 200).await;
}

#[tokio::test]
async fn more_diffs_with_a_sharded_directory_equal_a_naive_comparison() {
    naive_comparison(&[1], MAX_TREE_ENTRIES as u64 + 200).await;
}

#[tokio::test]
async fn diffs_of_small_histories_equal_a_naive_comparison() {
    naive_comparison(&[2, 3, 4, 5, 6, 7], 0).await;
}

#[tokio::test]
async fn more_diffs_of_small_histories_equal_a_naive_comparison() {
    naive_comparison(&[8, 9, 10, 11, 12, 13], 0).await;
}

/// A file is "binary differs" when either side is binary: text to binary
/// and binary to text both have no patch.
#[tokio::test]
async fn either_side_binary_makes_the_file_binary() {
    let e = env().await;
    let ops = ops(&e.meta, &e.blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("t2b", Some((FileKind::Regular, b"text\n"))),
        ("b2t", Some((FileKind::Regular, b"bin\0ary\n"))),
        ("t2t", Some((FileKind::Regular, b"text\n"))),
    ]);
    let one = commit(&ops, &[], "one", c, &b).await.unwrap();
    let (c, b) = m.apply(&[
        ("t2b", Some((FileKind::Regular, b"bin\0ary\n"))),
        ("b2t", Some((FileKind::Regular, b"text\n"))),
        ("t2t", Some((FileKind::Regular, b"other\n"))),
    ]);
    let two = commit(&ops, &[one], "two", c, &b).await.unwrap();
    let out = diff_all(&ops, one, two, DiffMode::Patch, &[])
        .await
        .unwrap();
    let got: Vec<(String, bool, bool)> = out
        .iter()
        .map(|f| {
            (
                String::from_utf8(f.path.clone()).unwrap(),
                f.binary,
                f.patch.is_empty(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("b2t".to_owned(), true, true),
            ("t2b".to_owned(), true, true),
            ("t2t".to_owned(), false, false),
        ]
    );
}

/// Kind-only changes, symlinks, and a path that is a file on one side and a
/// directory on the other are each reported, with filters naming them.
#[tokio::test]
async fn kind_changes_and_file_directory_swaps_are_reported() {
    let e = env().await;
    let ops = ops(&e.meta, &e.blobs);
    let mut m = Model::default();
    let (c, b) = m.apply(&[
        ("x", Some((FileKind::Regular, b"file x\n"))),
        ("y/z", Some((FileKind::Regular, b"in y\n"))),
        ("k", Some((FileKind::Regular, b"same\n"))),
        ("l", Some((FileKind::Symlink, b"x"))),
    ]);
    let one = commit(&ops, &[], "one", c, &b).await.unwrap();
    let before = m.clone();
    let (c, b) = m.apply(&[
        ("x", None),
        ("x/w", Some((FileKind::Regular, b"under x\n"))),
        ("y/z", None),
        ("k", Some((FileKind::Executable, b"same\n"))),
        ("l", Some((FileKind::Symlink, b"k"))),
    ]);
    let mid = commit(&ops, &[one], "mid", c, &b).await.unwrap();
    // A directory becomes a file only in a later changeset (INV-12).
    let (c, b) = m.apply(&[("y", Some((FileKind::Regular, b"file y\n")))]);
    let two = commit(&ops, &[mid], "two", c, &b).await.unwrap();
    let want = |paths: &[&str]| {
        paths
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect::<Vec<_>>()
    };
    for (a, bb) in [(one, two), (two, one)] {
        let out = diff_all(&ops, a, bb, DiffMode::Stat, &[]).await.unwrap();
        let got: Vec<Vec<u8>> = out.iter().map(|f| f.path.clone()).collect();
        assert_eq!(got, want(&["k", "l", "x", "x/w", "y", "y/z"]));
        let (ma, mb) = if a == one {
            (&before, &m)
        } else {
            (&m, &before)
        };
        for f in &out {
            assert_eq!(f.old.map(|s| s.kind), ma.files.get(&f.path).map(|v| v.0));
            assert_eq!(f.new.map(|s| s.kind), mb.files.get(&f.path).map(|v| v.0));
        }
        let k = &out[0];
        assert_eq!((k.added_lines, k.removed_lines), (0, 0), "kind-only change");
        let filtered = diff_all(&ops, a, bb, DiffMode::Names, &[path("x"), path("y/z")])
            .await
            .unwrap();
        assert_eq!(
            filtered.into_iter().map(|f| f.path).collect::<Vec<_>>(),
            want(&["x", "x/w", "y/z"])
        );
    }
}

/// Names around a directory `a`: `a-c` and `a.x` sort before the contents of
/// `a/`, and `a0` after them, so path byte order is not tree order.
const ORDER_PATHS: &[&str] = &[
    "a-c", "a.x", "a/b-1", "a/b/c", "a/b/d", "a/b0", "a/bb", "a0", "b", "b-/a", "z/y/x",
];

async fn order_fixture() -> (Env, ChangesetId, ChangesetId) {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "base",
        vec![(path("keep"), write(b"k\n"))],
        &[bytes::Bytes::from_static(b"k\n")],
    )
    .await
    .unwrap();
    let changes = ORDER_PATHS
        .iter()
        .map(|p| (path(p), write(p.as_bytes())))
        .collect();
    let contents: Vec<bytes::Bytes> = ORDER_PATHS
        .iter()
        .map(|p| bytes::Bytes::copy_from_slice(p.as_bytes()))
        .collect();
    let next = commit(&plain, &[base], "next", changes, &contents)
        .await
        .unwrap();
    (e, base, next)
}

#[tokio::test]
async fn pages_reassemble_the_whole_diff_in_path_byte_order_at_every_size() {
    let (e, base, next) = order_fixture().await;
    let o = ops(&e.meta, &e.blobs);
    let mut want: Vec<&[u8]> = ORDER_PATHS.iter().map(|p| p.as_bytes()).collect();
    want.sort();
    for page in [1, 2, 3, 5, ops::MAX_DIFF_PAGE] {
        for mode in [DiffMode::Names, DiffMode::Patch] {
            let got = diff_paged(&o, base, next, mode, &[], page).await.unwrap();
            let paths: Vec<&[u8]> = got.iter().map(|f| f.path.as_slice()).collect();
            assert_eq!(paths, want, "page {page}");
        }
    }
}

#[tokio::test]
async fn a_cursor_at_any_path_returns_exactly_the_entries_after_it() {
    let (e, base, next) = order_fixture().await;
    let o = ops(&e.meta, &e.blobs);
    let all = diff_paged(&o, base, next, DiffMode::Names, &[], ops::MAX_DIFF_PAGE)
        .await
        .unwrap();
    // Cursors a client could forge, at, between, and around the changed
    // paths.
    for after in [
        "a", "a-c", "a/", "a/b", "a/b/", "a/b/c", "a/c", "a0", "aa", "b/a", "zz", "\u{1}",
    ] {
        let cursor = match ops::DiffCursor::from_bytes(after.as_bytes()) {
            Ok(cursor) => cursor,
            Err(err) => {
                // Only a path is a cursor; one ending in `/` is refused.
                assert!(after.ends_with('/'), "{after:?}: {err}");
                assert_eq!(err.code, ops::Code::InvalidArgument, "{after:?}");
                continue;
            }
        };
        let page = o
            .diff(
                REPO,
                base,
                next,
                &ops::DiffOptions {
                    mode: DiffMode::Names,
                    paths: &[],
                    after: Some(&cursor),
                    limit: ops::MAX_DIFF_PAGE,
                },
            )
            .await
            .unwrap();
        let want: Vec<&FileDiff> = all
            .iter()
            .filter(|f| f.path.as_slice() > after.as_bytes())
            .collect();
        assert_eq!(
            page.files.iter().collect::<Vec<_>>(),
            want,
            "after {after:?}"
        );
        assert_eq!(page.next, None);
    }
}

#[tokio::test]
async fn malformed_cursors_and_page_sizes_are_invalid_arguments() {
    let (e, base, next) = order_fixture().await;
    let o = ops(&e.meta, &e.blobs);
    for bad in [&b""[..], b"/a", b"a//b", b"a\0"] {
        let err = ops::DiffCursor::from_bytes(bad).unwrap_err();
        assert_eq!(err.code, ops::Code::InvalidArgument, "{bad:?}");
    }
    for limit in [0, ops::MAX_DIFF_PAGE + 1] {
        let err = o
            .diff(
                REPO,
                base,
                next,
                &ops::DiffOptions {
                    mode: DiffMode::Names,
                    paths: &[],
                    after: None,
                    limit,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ops::Code::InvalidArgument, "{limit}");
    }
}

#[tokio::test]
async fn a_kind_only_change_reads_no_content() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let body = bytes::Bytes::from_static(b"#!/bin/sh\necho hi\n");
    let base = commit(
        &plain,
        &[],
        "regular",
        vec![(path("run"), write(&body))],
        std::slice::from_ref(&body),
    )
    .await
    .unwrap();
    let id = scion_core::BlobId::hash(&body);
    let exec = scion_core::FileChange::File {
        kind: FileKind::Executable,
        blob: id,
        size: body.len() as u64,
    };
    let next = commit(&plain, &[base], "exec", vec![(path("run"), exec)], &[])
        .await
        .unwrap();
    let faults = store::fault::Faults::new();
    let meta = std::sync::Arc::new(store::fault::FaultMetaStore::new(
        e.meta.clone(),
        faults.clone(),
    ));
    let blobs = std::sync::Arc::new(store::fault::FaultBlobStore::new(
        e.blobs.clone(),
        faults.clone(),
    ));
    let got = diff_all(&ops(&meta, &blobs), base, next, DiffMode::Patch, &[])
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].old.unwrap().kind, FileKind::Regular);
    assert_eq!(got[0].new.unwrap().kind, FileKind::Executable);
    assert!(!got[0].binary);
    assert_eq!((got[0].added_lines, got[0].removed_lines), (0, 0));
    assert!(got[0].patch.is_empty());
    assert_eq!(faults.counts().blob_reads, 0);
}

#[tokio::test]
async fn a_text_record_over_binary_bytes_is_data_loss() {
    use store::{BlobStore, MetaKey, MetaStore, ObjectKey};
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "text",
        vec![(path("f"), write(b"a\n"))],
        &[bytes::Bytes::from_static(b"a\n")],
    )
    .await
    .unwrap();
    // A record that claims text for bytes with a NUL, with the real frame.
    let binary = b"x\0y\n";
    let id = scion_core::BlobId::hash(binary);
    let record = derived::BlobRecord {
        size: binary.len() as u64,
        sha256: [0; 32],
        git_sha1: None,
        text: true,
    };
    e.meta
        .put(
            REPO,
            &[(MetaKey::Blob(id), bytes::Bytes::from(record.encode()))],
        )
        .await
        .unwrap();
    let frame = derived::encode_frame(scion_core::FrameId::Blob(id), binary).unwrap();
    e.blobs
        .put_if_absent(REPO, ObjectKey::blob(id), frame)
        .await
        .unwrap();
    let next = commit(
        &plain,
        &[base],
        "lie",
        vec![(path("f"), write(binary))],
        &[],
    )
    .await
    .unwrap();
    let err = diff_all(&plain, base, next, DiffMode::Stat, &[])
        .await
        .unwrap_err();
    assert_eq!(err.code, ops::Code::DataLoss);
    // Names mode reads no content, so it reports the change.
    assert_eq!(
        diff_all(&plain, base, next, DiffMode::Names, &[])
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_cursor_under_path_filters_returns_exactly_the_filtered_entries_after_it() {
    let (e, base, next) = order_fixture().await;
    let o = ops(&e.meta, &e.blobs);
    // Filters naming a directory, files beside it that sort before its
    // contents, and one after them.
    let filters = [path("a"), path("a-c"), path("a.x"), path("a0")];
    let all = diff_paged(
        &o,
        base,
        next,
        DiffMode::Names,
        &filters,
        ops::MAX_DIFF_PAGE,
    )
    .await
    .unwrap();
    let paths: Vec<&[u8]> = all.iter().map(|f| f.path.as_slice()).collect();
    assert_eq!(
        paths,
        [
            &b"a-c"[..],
            b"a.x",
            b"a/b-1",
            b"a/b/c",
            b"a/b/d",
            b"a/b0",
            b"a/bb",
            b"a0"
        ]
    );
    for after in [
        "a", "a-c", "a.x", "a/b", "a/b/c", "a/bb", "a0", "a-", "a.y", "b",
    ] {
        let cursor = ops::DiffCursor::from_bytes(after.as_bytes()).unwrap();
        let page = o
            .diff(
                REPO,
                base,
                next,
                &ops::DiffOptions {
                    mode: DiffMode::Names,
                    paths: &filters,
                    after: Some(&cursor),
                    limit: ops::MAX_DIFF_PAGE,
                },
            )
            .await
            .unwrap();
        let want: Vec<&FileDiff> = all
            .iter()
            .filter(|f| f.path.as_slice() > after.as_bytes())
            .collect();
        assert_eq!(
            page.files.iter().collect::<Vec<_>>(),
            want,
            "after {after:?}"
        );
    }
}
