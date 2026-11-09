//! Diff cost on cold server caches, on Postgres and the object store: equal
//! shards are skipped, binary files and classification read only metadata
//! in bounded batches, and shard collapse and file-directory swaps diff
//! correctly.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use ops::DiffMode;
use store::fault::{FaultBlobStore, FaultMetaStore, Faults};

#[tokio::test]
async fn diff_skips_equal_shards_even_when_the_split_prefix_changes() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "wide",
        (0..50_000)
            .map(|i| (path(&format!("f{i:06}")), write(b"x")))
            .collect(),
        &[Bytes::from_static(b"x")],
    )
    .await
    .unwrap();
    for (name, content) in [("f012345", &b"y"[..]), ("a", &b"x"[..])] {
        let next = commit(
            &plain,
            &[base],
            name,
            vec![(path(name), write(content))],
            &[Bytes::copy_from_slice(content)],
        )
        .await
        .unwrap();
        for (a, b) in [(base, next), (next, base)] {
            let faults = Faults::new();
            let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
            let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
            let cold = ops(&meta, &blobs);
            let diff = diff_all(&cold, a, b, DiffMode::Names, &[]).await.unwrap();
            assert_eq!(diff.len(), 1);
            assert_eq!(diff[0].path, name.as_bytes());
            assert!(
                faults.counts().meta_get_keys <= 10,
                "read unchanged shards: {:?}",
                faults.counts()
            );
            assert_eq!(faults.counts().blob_reads, 0);
        }
    }
}

#[tokio::test]
async fn binary_diff_uses_metadata_without_reading_content() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "text",
        vec![(path("f"), write(b"text\n"))],
        &[Bytes::from_static(b"text\n")],
    )
    .await
    .unwrap();
    for bytes in [vec![0; 32], vec![0; 9 << 20], vec![b'x'; (16 << 20) + 1]] {
        let next = commit(
            &plain,
            &[base],
            "binary",
            vec![(path("f"), write(&bytes))],
            &[Bytes::from(bytes)],
        )
        .await
        .unwrap();
        for mode in [DiffMode::Stat, DiffMode::Patch] {
            for (a, b) in [(base, next), (next, base)] {
                let faults = Faults::new();
                let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
                let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
                let cold = ops(&meta, &blobs);
                let out = diff_all(&cold, a, b, mode, &[]).await.unwrap();
                assert_eq!(out.len(), 1);
                assert!(out[0].binary);
                assert_eq!((out[0].added_lines, out[0].removed_lines), (0, 0));
                assert!(out[0].patch.is_empty());
                assert_eq!(faults.counts().blob_reads, 0);
            }
        }
    }
}

#[tokio::test]
async fn diff_classification_batches_metadata_reads() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    // Distinct blobs on both sides rule out caching or deduplication alone
    // satisfying the round-trip bound. More files than one batch exercises
    // the boundary and the partial final batch.
    let n = 1025;
    let old: Vec<_> = (0..n).map(|i| Bytes::from(format!("old\0{i}"))).collect();
    let new: Vec<_> = (0..n).map(|i| Bytes::from(format!("new\0{i}"))).collect();
    let changes = |contents: &[Bytes]| {
        contents
            .iter()
            .enumerate()
            .map(|(i, b)| (path(&format!("f{i:04}")), write(b)))
            .collect()
    };
    let base = commit(&plain, &[], "old", changes(&old), &old)
        .await
        .unwrap();
    let next = commit(&plain, &[base], "new", changes(&new), &new)
        .await
        .unwrap();
    let mut names_counts = None;
    for mode in [DiffMode::Names, DiffMode::Stat, DiffMode::Patch] {
        let faults = Faults::new();
        let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let cold = ops(&meta, &blobs);
        let out = diff_paged(&cold, base, next, mode, &[], ops::MAX_DIFF_PAGE)
            .await
            .unwrap();
        assert_eq!(out.len(), n);
        assert_eq!(faults.counts().blob_reads, 0);
        if mode == DiffMode::Names {
            names_counts = Some(faults.counts());
        } else {
            let counts = faults.counts();
            let baseline = names_counts.unwrap();
            let reads = counts.meta_gets - baseline.meta_gets;
            assert!(
                (2..=10).contains(&reads),
                "2050 records must use bounded batches, not {reads} calls"
            );
            assert_eq!(counts.meta_get_keys - baseline.meta_get_keys, 2 * n as u64);
            for (i, diff) in out.iter().enumerate() {
                assert!(diff.binary);
                assert_eq!(diff.path, format!("f{i:04}").as_bytes());
                assert_eq!(diff.old.unwrap().blob, scion_core::BlobId::hash(&old[i]));
                assert_eq!(diff.new.unwrap().blob, scion_core::BlobId::hash(&new[i]));
                assert!(diff.patch.is_empty());
                assert_eq!((diff.added_lines, diff.removed_lines), (0, 0));
            }
        }
    }
}

#[tokio::test]
async fn diff_classification_deduplicates_blobs_within_each_batch() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(&plain, &[], "empty", vec![], &[]).await.unwrap();
    let next = commit(
        &plain,
        &[base],
        "copies",
        (0..100)
            .map(|i| (path(&format!("f{i:03}")), write(b"\0binary")))
            .collect(),
        &[Bytes::from_static(b"\0binary")],
    )
    .await
    .unwrap();
    let mut baseline = None;
    for mode in [DiffMode::Names, DiffMode::Stat] {
        let faults = Faults::new();
        let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let out = diff_paged(&ops(&meta, &blobs), base, next, mode, &[], 100)
            .await
            .unwrap();
        assert_eq!(out.len(), 100);
        if mode == DiffMode::Names {
            baseline = Some(faults.counts());
        } else {
            assert!(out.iter().all(|f| f.binary));
            assert_eq!(
                faults.counts().meta_get_keys - baseline.unwrap().meta_get_keys,
                1
            );
            assert_eq!(faults.counts().blob_reads, 0);
        }
    }
}

#[tokio::test]
async fn diff_handles_shard_collapse_and_directory_file_replacement() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = commit(
        &plain,
        &[],
        "split",
        (0..4097)
            .map(|i| (path(&format!("d/f{i:04}")), write(b"x")))
            .collect(),
        &[Bytes::from_static(b"x")],
    )
    .await
    .unwrap();
    let collapsed = commit(
        &plain,
        &[base],
        "collapse",
        vec![(path("d/f4096"), scion_core::FileChange::Deleted)],
        &[],
    )
    .await
    .unwrap();
    let replaced = commit(&plain, &[], "file", vec![(path("d"), write(b"x"))], &[])
        .await
        .unwrap();
    for (a, b) in [(base, collapsed), (collapsed, base)] {
        let out = diff_all(&ops(&e.meta, &e.blobs), a, b, DiffMode::Names, &[])
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].path, b"d/f4096");
    }
    for (a, b) in [(collapsed, replaced), (replaced, collapsed)] {
        let out = diff_all(&ops(&e.meta, &e.blobs), a, b, DiffMode::Names, &[])
            .await
            .unwrap();
        assert_eq!(out.len(), 4097);
        assert_eq!(out[0].path, b"d");
        for (i, f) in out[1..].iter().enumerate() {
            assert_eq!(f.path, format!("d/f{i:04}").as_bytes());
        }
    }
}

/// A page reads only the tree nodes on the path to its entries, however wide
/// the directory and wherever the page starts: it never lists the directory.
#[tokio::test]
async fn every_page_of_a_wide_directory_reads_only_the_nodes_on_its_path() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let n = 20_000;
    let base = commit(
        &plain,
        &[],
        "old",
        (0..n)
            .map(|i| (path(&format!("d/f{i:05}")), write(b"old\n")))
            .collect(),
        &[Bytes::from_static(b"old\n")],
    )
    .await
    .unwrap();
    let next = commit(
        &plain,
        &[base],
        "new",
        (0..n)
            .map(|i| (path(&format!("d/f{i:05}")), write(b"new\n")))
            .collect(),
        &[Bytes::from_static(b"new\n")],
    )
    .await
    .unwrap();
    let page = |after: Option<&str>| {
        let (meta, blobs) = (e.meta.clone(), e.blobs.clone());
        let after = after.map(|a| ops::DiffCursor::from_bytes(a.as_bytes()).unwrap());
        async move {
            let faults = Faults::new();
            let meta = Arc::new(FaultMetaStore::new(meta, faults.clone()));
            let blobs = Arc::new(FaultBlobStore::new(blobs, faults.clone()));
            let cold = ops(&meta, &blobs);
            let got = cold
                .diff(
                    REPO,
                    base,
                    next,
                    &ops::DiffOptions {
                        mode: DiffMode::Names,
                        paths: &[],
                        after: after.as_ref(),
                        limit: 100,
                    },
                )
                .await
                .unwrap();
            (got, faults.counts().meta_get_keys)
        }
    };
    let (first, first_keys) = page(None).await;
    assert_eq!(first.files[0].path, b"d/f00000");
    let (deep, deep_keys) = page(Some("d/f15000")).await;
    assert_eq!(deep.files[0].path, b"d/f15001");
    assert_eq!(deep.files.len(), 100);
    // Each side of `d` is a trie of about 23 nodes (a top node, two shards,
    // and 20 under them). A page reads only the nodes on the path to its
    // entries, and the two RootTree values: about 10 keys, wherever it
    // starts. Listing the whole directory would read more than 46.
    for (what, keys) in [("first", first_keys), ("deep", deep_keys)] {
        assert!(keys <= 16, "the {what} page read {keys} keys");
    }
}

/// Keys read by one Names-mode diff page on a cold server.
async fn page_keys(
    e: &Env,
    a: scion_core::ChangesetId,
    b: scion_core::ChangesetId,
    paths: &[scion_core::RepoPath],
    after: Option<&str>,
) -> (Vec<ops::FileDiff>, u64) {
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    let after = after.map(|a| ops::DiffCursor::from_bytes(a.as_bytes()).unwrap());
    let got = ops(&meta, &blobs)
        .diff(
            REPO,
            a,
            b,
            &ops::DiffOptions {
                mode: DiffMode::Names,
                paths,
                after: after.as_ref(),
                limit: 100,
            },
        )
        .await
        .unwrap();
    (got.files, faults.counts().meta_get_keys)
}

/// 20,000 changed files named `gen-NNNNN` beside a changed directory `gen/`:
/// in path order they all come before `gen/`'s contents.
async fn siblings_fixture(e: &Env) -> (scion_core::ChangesetId, scion_core::ChangesetId) {
    let plain = ops(&e.meta, &e.blobs);
    let files = |content: &[u8]| -> Vec<_> {
        (0..20_000)
            .map(|i| (path(&format!("gen-{i:05}")), write(content)))
            .chain((0..300).map(|i| (path(&format!("gen/f{i:03}")), write(content))))
            .collect()
    };
    let old = commit(
        &plain,
        &[],
        "old",
        files(b"old\n"),
        &[Bytes::from_static(b"old\n")],
    )
    .await
    .unwrap();
    let new = commit(
        &plain,
        &[old],
        "new",
        files(b"new\n"),
        &[Bytes::from_static(b"new\n")],
    )
    .await
    .unwrap();
    (old, new)
}

#[tokio::test]
async fn resuming_inside_a_directory_skips_the_siblings_sorting_before_it() {
    let e = env().await;
    let (old, new) = siblings_fixture(&e).await;
    let (files, keys) = page_keys(&e, old, new, &[], Some("gen/f100")).await;
    assert_eq!(files[0].path, b"gen/f101");
    assert_eq!(files.len(), 100);
    // The root and `gen/` on both sides, and the RootTree values; walking
    // the 20,000 siblings would read their shards (more than 40 keys).
    assert!(keys <= 16, "resuming read {keys} keys");
}

#[tokio::test]
async fn a_path_filter_reads_only_the_nodes_it_names() {
    let e = env().await;
    let (old, new) = siblings_fixture(&e).await;
    let (files, keys) = page_keys(&e, old, new, &[path("gen-12345")], None).await;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, b"gen-12345");
    assert!(keys <= 16, "the filter read {keys} keys");
}

#[tokio::test]
async fn a_filtered_page_after_a_cursor_reads_only_what_follows_it() {
    let e = env().await;
    let (old, new) = siblings_fixture(&e).await;
    let filters: Vec<_> = (0..2_000)
        .map(|i| path(&format!("gen-{:05}", i * 10)))
        .collect();
    let all = diff_paged(
        &ops(&e.meta, &e.blobs),
        old,
        new,
        DiffMode::Names,
        &filters,
        1000,
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 2_000);
    // Forged cursors anywhere give exactly the filtered entries after them.
    for after in [
        "gen-00000",
        "gen-09995",
        "gen-10000",
        "gen-19990",
        "gen-5",
        "gen",
    ] {
        let (files, _) = page_keys(&e, old, new, &filters, Some(after)).await;
        let want: Vec<&[u8]> = all
            .iter()
            .map(|f| f.path.as_slice())
            .filter(|p| *p > after.as_bytes())
            .take(100)
            .collect();
        assert_eq!(
            files.iter().map(|f| f.path.as_slice()).collect::<Vec<_>>(),
            want,
            "{after}"
        );
    }
    // A page reads the nodes for the filtered names it reaches (two batches
    // of 64 for a page of 100): about a dozen keys wherever it starts. Looking up all
    // 2,000 names would read every shard of the root on both sides.
    for after in [None, Some("gen-09995"), Some("gen-19000")] {
        let (_, keys) = page_keys(&e, old, new, &filters, after).await;
        assert!(keys <= 16, "the page after {after:?} read {keys} keys");
    }
}

/// A filter of 20,000 names in a sharded directory, with the one change at
/// the end, takes store reads bounded by the trie nodes it reaches, not by
/// the number of names.
#[tokio::test]
async fn a_large_filter_takes_reads_bounded_by_the_nodes_it_reaches() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let names: Vec<String> = (0..20_000).map(|i| format!("d/f{i:05}")).collect();
    let base = commit(
        &plain,
        &[],
        "base",
        names.iter().map(|p| (path(p), write(b"x\n"))).collect(),
        &[Bytes::from_static(b"x\n")],
    )
    .await
    .unwrap();
    let next = commit(
        &plain,
        &[base],
        "one change",
        vec![(path("d/f19999"), write(b"y\n"))],
        &[Bytes::from_static(b"y\n")],
    )
    .await
    .unwrap();
    let filters: Vec<_> = names.iter().map(|p| path(p)).collect();
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    // No shared tree cache: only the request's own record of the nodes it
    // has read can keep the reads down.
    let uncached = ops::Ops::new(Arc::new(derived::Deriver::new(
        meta,
        blobs,
        derived::CacheSizes {
            trees: 0,
            ..derived::CacheSizes::default()
        },
    )));
    let got = diff_paged(&uncached, base, next, DiffMode::Names, &filters, 100)
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].path, b"d/f19999");
    // `d` is a trie of 23 nodes (a top node, two shards, and 20 leaves),
    // and one store call reads both sides' node at each step; each is
    // read once per request, so `d` costs about 23 calls and the root and
    // derived values the rest. Reading per name would take thousands.
    let reads = faults.counts().meta_gets;
    assert!(reads <= 30, "{reads} store reads");
}
