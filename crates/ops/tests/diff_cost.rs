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
