//! Diff on Postgres and the object store: changed paths equal a naive
//! comparison of the two file maps, line counts and patches are consistent
//! with the contents (every patch applies to the old text and yields the new
//! one), binary files are flagged, and path filters select exactly the
//! files under them.

mod common;

use std::collections::BTreeSet;

use common::*;
use ops::{DiffMode, FileDiff};
use scion_core::{ChangesetId, FileKind};

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

fn text(rng: &mut Rng, lines: u64) -> Vec<u8> {
    (0..lines)
        .flat_map(|_| format!("line {}\n", rng.below(6)).into_bytes())
        .collect()
}

#[tokio::test]
async fn diffs_equal_a_naive_comparison() {
    let env = env().await;
    let ops = ops(&env.meta, &env.blobs);
    let mut rng = Rng(42);
    let paths = [
        "a", "b/c", "b/d/e", "b/d/f", "g/h", "big/x1", "big/x2", "bin",
    ];
    let mut models: Vec<(ChangesetId, Model)> = Vec::new();
    let mut model = Model::default();
    let mut parent: Option<ChangesetId> = None;
    // Seed a sharded directory too.
    let spec: Vec<(String, Vec<u8>)> = (0..4200)
        .map(|i| (format!("big/n{i:04}"), b"same\n".to_vec()))
        .collect();
    let s: Vec<Spec<'_>> = spec
        .iter()
        .map(|(p, c)| (p.as_str(), Some((FileKind::Regular, c.as_slice()))))
        .collect();
    let (fc, contents) = model.apply(&s);
    let id = commit(&ops, &[], "seed", fc, &contents[..1]).await.unwrap();
    models.push((id, model.clone()));
    parent = Some(id).or(parent);
    for step in 0..10 {
        let mut spec: Vec<(String, Option<Vec<u8>>)> = Vec::new();
        for p in paths {
            if rng.below(3) != 0 {
                continue;
            }
            let content = if p == "bin" {
                let mut b = text(&mut rng, 3);
                b.insert(0, 0);
                Some(b)
            } else if model.files.contains_key(p.as_bytes()) && rng.below(4) == 0 {
                None
            } else {
                let n = 1 + rng.below(12);
                Some(text(&mut rng, n))
            };
            spec.push((p.to_owned(), content));
        }
        if rng.below(2) == 0 {
            spec.push((
                format!("big/n{:04}", rng.below(4200)),
                Some(format!("changed {step}\n").into_bytes()),
            ));
        }
        let s: Vec<Spec<'_>> = spec
            .iter()
            .map(|(p, c)| {
                (
                    p.as_str(),
                    c.as_ref().map(|c| (FileKind::Regular, c.as_slice())),
                )
            })
            .collect();
        let mut next = model.clone();
        let (fc, contents) = next.apply(&s);
        match commit(&ops, &[parent.unwrap()], &format!("{step}"), fc, &contents).await {
            Ok(id) => {
                model = next;
                parent = Some(id);
                models.push((id, model.clone()));
            }
            Err(e) => assert_eq!(e.code, ops::Code::NoOpChange, "{e}"),
        }
    }
    assert!(models.len() > 6);
    for (i, (a, ma)) in models.iter().enumerate() {
        for (b, mb) in models.iter().skip(i.saturating_sub(2)).take(4) {
            let want: Vec<Vec<u8>> = ma
                .files
                .keys()
                .chain(mb.files.keys())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|p| ma.files.get(*p) != mb.files.get(*p))
                .cloned()
                .collect();
            let got: Vec<FileDiff> = diff_all(&ops, *a, *b, DiffMode::Patch, &[]).await.unwrap();
            assert_eq!(got.iter().map(|f| f.path.clone()).collect::<Vec<_>>(), want);
            for f in &got {
                let old = ma
                    .files
                    .get(&f.path)
                    .map(|(_, c)| c.clone())
                    .unwrap_or_default();
                let new = mb
                    .files
                    .get(&f.path)
                    .map(|(_, c)| c.clone())
                    .unwrap_or_default();
                // Each side's entry is exactly the model's: kind, blob, and size.
                let state = |m: &Model| {
                    m.files.get(&f.path).map(|(k, c)| ops::FileState {
                        kind: *k,
                        blob: scion_core::BlobId::hash(c),
                        size: c.len() as u64,
                    })
                };
                assert_eq!(f.old, state(ma), "{}", String::from_utf8_lossy(&f.path));
                assert_eq!(f.new, state(mb), "{}", String::from_utf8_lossy(&f.path));
                if old.contains(&0) || new.contains(&0) {
                    assert!(f.binary && f.patch.is_empty());
                    continue;
                }
                assert!(!f.binary);
                assert_eq!(
                    apply_patch(&old, &f.patch),
                    new,
                    "{}",
                    String::from_utf8_lossy(&f.path)
                );
                let pluses = f
                    .patch
                    .split(|&b| b == b'\n')
                    .filter(|l| l.starts_with(b"+"))
                    .count() as u64;
                let minuses = f
                    .patch
                    .split(|&b| b == b'\n')
                    .filter(|l| l.starts_with(b"-"))
                    .count() as u64;
                assert_eq!((f.added_lines, f.removed_lines), (pluses, minuses));
            }
            // Names mode reports the same entries, with no contents read.
            let names = diff_all(&ops, *a, *b, DiffMode::Names, &[]).await.unwrap();
            assert_eq!(
                names
                    .iter()
                    .map(|f| (&f.path, f.old, f.new))
                    .collect::<Vec<_>>(),
                got.iter()
                    .map(|f| (&f.path, f.old, f.new))
                    .collect::<Vec<_>>()
            );
            assert!(
                names
                    .iter()
                    .all(|f| f.patch.is_empty() && f.added_lines == 0)
            );
            // Filters select exactly the files under them.
            let filter = [path("b/d"), path("big")];
            let filtered = diff_all(&ops, *a, *b, DiffMode::Names, &filter)
                .await
                .unwrap();
            let want_filtered: Vec<_> = got
                .iter()
                .filter(|f| f.path.starts_with(b"b/d/") || f.path.starts_with(b"big/"))
                .map(|f| f.path.clone())
                .collect();
            assert_eq!(
                filtered.into_iter().map(|f| f.path).collect::<Vec<_>>(),
                want_filtered
            );
        }
    }
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
        let Ok(cursor) = ops::DiffCursor::from_bytes(after.as_bytes()) else {
            continue;
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
