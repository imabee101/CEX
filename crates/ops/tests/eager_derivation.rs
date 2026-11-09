//! Eager values and their parents, on Postgres and the object store. An ACL
//! config change never makes create walk history; creates under different
//! config versions each get their own value; and a parent that lacks eager
//! values (written before a type existed, or before a new config version) is
//! derived in bounded batches, or refused retryably past the per-request
//! bound.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use derived::{AclConfig, DerivedType, MAX_DERIVE_PER_REQUEST, RestrictedRoots, RootTree};
use ops::{Code, CreateOptions};
use scion_core::{ChangesetId, EntryTarget};
use store::fault::{FaultBlobStore, FaultMetaStore, Faults};
use store::{MetaKey, MetaStore};

fn restricting_private(version: u64) -> Arc<AclConfig> {
    Arc::new(AclConfig {
        version,
        default_domain: "default".into(),
        domains: [(path("private"), "secret".into())].into(),
    })
}

/// The tree id of `private` in `cs`'s root tree, looked up directly.
async fn private_dir<M: MetaStore, B: store::BlobStore>(
    o: &ops::Ops<M, B>,
    cs: ChangesetId,
) -> Option<scion_core::TreeId> {
    let root = root_of(o, cs).await;
    match o
        .deriver()
        .cx(REPO)
        .trees
        .lookup_path(root, &path("private"))
        .await
        .unwrap()
    {
        Some(EntryTarget::Tree(t)) => Some(t),
        _ => None,
    }
}

#[tokio::test]
async fn an_acl_change_costs_create_the_same_however_long_the_history() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let mut tips = Vec::new();
    let mut parent: Option<ChangesetId> = None;
    for i in 0..64 {
        let content = Bytes::from(format!("{i}\n"));
        let id = commit(
            &plain,
            &parent.into_iter().collect::<Vec<_>>(),
            &format!("c{i}"),
            vec![(path("private/file"), write(&content))],
            &[content],
        )
        .await
        .unwrap();
        parent = Some(id);
        // Both tips have parents, so both children read the same kinds of
        // commit-graph values; only the history beneath them differs.
        if i == 1 || i == 63 {
            tips.push(id);
        }
    }
    let config = restricting_private(1);
    let opts = CreateOptions {
        acl: config.clone(),
        ..CreateOptions::default()
    };
    let restricted = RestrictedRoots::new(config);
    let mut costs = Vec::new();
    for tip in tips {
        let faults = Faults::new();
        let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
        let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
        let cold = ops(&meta, &blobs);
        let cs = changeset(&[tip], "new ACL", vec![(path("public"), write(b"0\n"))]);
        upload(&plain, b"0\n").await;
        let id = cold.create_changeset(REPO, cs, &opts).await.unwrap();
        costs.push(faults.counts().meta_get_keys);
        // The new value is right, and no ancestor's value was derived.
        let roots =
            RestrictedRoots::decode(&cold.deriver().get(REPO, &restricted, id).await.unwrap())
                .unwrap();
        assert_eq!(
            roots.get(&path("private")).copied(),
            private_dir(&cold, id).await
        );
        let parent_value = e
            .meta
            .get(REPO, &[MetaKey::Derived(restricted.key(tip))])
            .await
            .unwrap();
        assert_eq!(
            parent_value,
            vec![None],
            "create derived the parent's value"
        );
    }
    assert_eq!(
        costs[0], costs[1],
        "create cost grew with history: {costs:?}"
    );
}

async fn upload<M: MetaStore, B: store::BlobStore>(o: &ops::Ops<M, B>, content: &[u8]) {
    let d = o.deriver();
    ops::blob::upload_blobs(
        &**d.meta(),
        d.blobs(),
        REPO,
        &[Bytes::copy_from_slice(content)],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn creates_under_two_acl_versions_each_get_their_own_value() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    upload(&plain, b"s\n").await;
    let base = plain
        .create_changeset(
            REPO,
            changeset(&[], "base", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let v0 = CreateOptions::default();
    let v1 = CreateOptions {
        acl: restricting_private(1),
        ..CreateOptions::default()
    };
    let cs = changeset(&[base], "same id", vec![(path("private/s"), write(b"s\n"))]);
    let (a, b) = tokio::join!(
        plain.create_changeset(REPO, cs.clone(), &v0),
        plain.create_changeset(REPO, cs.clone(), &v1)
    );
    let id = a.unwrap();
    assert_eq!(b.unwrap(), id);
    let dir = private_dir(&plain, id).await.unwrap();
    for (opts, want) in [(&v0, None), (&v1, Some(dir))] {
        let t = RestrictedRoots::new(opts.acl.clone());
        let stored = e
            .meta
            .get(REPO, &[MetaKey::Derived(t.key(id))])
            .await
            .unwrap()
            .pop()
            .flatten()
            .expect("each version's value is written by its create");
        assert_eq!(
            RestrictedRoots::decode(&stored)
                .unwrap()
                .get(&path("private"))
                .copied(),
            want
        );
    }
}

/// Writes a chain of `n` empty changesets onto `base` as bare changeset
/// rows, with no derived values, and returns its tip.
async fn bare_chain(e: &Env, base: ChangesetId, n: usize) -> ChangesetId {
    let mut tip = base;
    let mut rows = Vec::new();
    for i in 0..n {
        let cs = changeset(&[tip], &format!("bare {i}"), vec![]);
        tip = cs.id();
        rows.push((MetaKey::Changeset(tip), Bytes::from(cs.encode())));
        if rows.len() == 1000 {
            e.meta.put(REPO, &rows).await.unwrap();
            rows.clear();
        }
    }
    e.meta.put(REPO, &rows).await.unwrap();
    tip
}

#[tokio::test]
async fn parents_lacking_eager_values_are_derived_within_the_bound() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = plain
        .create_changeset(
            REPO,
            changeset(&[], "base", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let tip = bare_chain(&e, base, 300).await;
    let child = plain
        .create_changeset(
            REPO,
            changeset(&[tip], "child", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    // Every ancestor got its values, equal to a from-scratch derivation's.
    let fresh = ops(&e.meta, &e.blobs);
    assert_eq!(root_of(&fresh, child).await, root_of(&fresh, base).await);
    let stored = e
        .meta
        .get(REPO, &[MetaKey::Derived(RootTree.key(tip))])
        .await
        .unwrap();
    assert!(stored[0].is_some());
}

#[tokio::test]
async fn slow_a_parent_chain_past_the_bound_is_finished_by_retries() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let base = plain
        .create_changeset(
            REPO,
            changeset(&[], "base", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let tip = bare_chain(&e, base, MAX_DERIVE_PER_REQUEST + 1).await;
    let child = changeset(&[tip], "child", vec![]);
    let err = plain
        .create_changeset(REPO, child.clone(), &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable, "{err}");
    assert!(err.code.retryable());
    // Nothing was written for the child, but the first request derived the
    // oldest ancestors of every type, so one retry finishes.
    assert_eq!(
        e.meta
            .get(REPO, &[MetaKey::Changeset(child.id())])
            .await
            .unwrap(),
        vec![None]
    );
    plain
        .create_changeset(REPO, child, &CreateOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn an_underived_ancestry_past_the_exploration_limit_is_refused_for_good() {
    use derived::MAX_DERIVE_EXPLORE;
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    // A wide bare history: 64 × 1,025 roots under 1,025 merges under one
    // tip, more underived ancestors than one request may explore.
    let mut rows = Vec::new();
    let mut merges = Vec::new();
    for m in 0..(MAX_DERIVE_EXPLORE / 64 + 1) {
        let roots: Vec<ChangesetId> = (0..64)
            .map(|r| {
                let cs = changeset(&[], &format!("root {m} {r}"), vec![]);
                rows.push((MetaKey::Changeset(cs.id()), Bytes::from(cs.encode())));
                cs.id()
            })
            .collect();
        let merge = changeset(&roots, &format!("merge {m}"), vec![]);
        rows.push((MetaKey::Changeset(merge.id()), Bytes::from(merge.encode())));
        merges.push(merge.id());
    }
    let mut tips = Vec::new();
    for chunk in merges.chunks(64) {
        let cs = changeset(chunk, &format!("tip {}", tips.len()), vec![]);
        rows.push((MetaKey::Changeset(cs.id()), Bytes::from(cs.encode())));
        tips.push(cs.id());
    }
    for batch in rows.chunks(1000) {
        e.meta.put(REPO, batch).await.unwrap();
    }
    let top = changeset(&tips, "top", vec![]);
    e.meta
        .put(
            REPO,
            &[(MetaKey::Changeset(top.id()), Bytes::from(top.encode()))],
        )
        .await
        .unwrap();
    let err = plain
        .create_changeset(
            REPO,
            changeset(&[top.id()], "child", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::LimitExceeded, "{err}");
    assert!(!err.code.retryable());
}

#[tokio::test]
async fn filling_in_an_existing_changeset_never_case_checks_its_history() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    upload(&plain, b"r\n").await;
    // An imported changeset with names that conflict by case (exempt), and a
    // child, stored with no derived values, as after a CaseIndex version bump.
    let mut parts = changeset(
        &[],
        "imported",
        vec![
            (path("README"), write(b"r\n")),
            (path("readme"), write(b"r\n")),
        ],
    )
    .into_parts();
    parts.git_headers = Some(b"author x 0 +0000\n\nimported".to_vec());
    let imported = scion_core::Changeset::new(parts).unwrap();
    let child = changeset(&[imported.id()], "child", vec![]);
    e.meta
        .put(
            REPO,
            &[
                (
                    MetaKey::Changeset(imported.id()),
                    Bytes::from(imported.encode()),
                ),
                (MetaKey::Changeset(child.id()), Bytes::from(child.encode())),
            ],
        )
        .await
        .unwrap();
    // Creating the child again, with the case check on, fills in every
    // eager value of it and its parent without checking either.
    plain
        .create_changeset(REPO, child, &CreateOptions::default())
        .await
        .unwrap();
    let index = ops::case::CaseIndex::default().key(imported.id());
    assert!(e.meta.get(REPO, &[MetaKey::Derived(index)]).await.unwrap()[0].is_some());
}
