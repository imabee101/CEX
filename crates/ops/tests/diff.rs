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
            let got: Vec<FileDiff> = ops.diff(REPO, *a, *b, DiffMode::Patch, &[]).await.unwrap();
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
                assert_eq!(f.old.is_some(), ma.files.contains_key(&f.path));
                assert_eq!(f.new.is_some(), mb.files.contains_key(&f.path));
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
            // Names mode reports the same paths, with no contents read.
            let names = ops.diff(REPO, *a, *b, DiffMode::Names, &[]).await.unwrap();
            assert_eq!(names.len(), got.len());
            assert!(
                names
                    .iter()
                    .all(|f| f.patch.is_empty() && f.added_lines == 0)
            );
            // Filters select exactly the files under them.
            let filter = [path("b/d"), path("big")];
            let filtered = ops
                .diff(REPO, *a, *b, DiffMode::Names, &filter)
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
