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
use derived::MAX_DERIVE_EXPLORE;
use derived::{
    AclConfig, CommitGraph, DerivedType, MAX_DERIVE_PER_REQUEST, RestrictedRoots, RootTree,
};
use ops::case::CaseIndex;
use ops::{Code, CreateOptions};
use scion_core::{Changeset, ChangesetId, EntryTarget};
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
/// rows, with no derived values, and returns them, oldest first.
async fn bare_chain(e: &Env, base: ChangesetId, n: usize) -> Vec<ChangesetId> {
    let mut chain: Vec<ChangesetId> = Vec::with_capacity(n);
    let mut rows = Vec::new();
    for i in 0..n {
        let cs = changeset(
            &[chain.last().copied().unwrap_or(base)],
            &format!("bare {i}"),
            vec![],
        );
        chain.push(cs.id());
        rows.push((MetaKey::Changeset(cs.id()), Bytes::from(cs.encode())));
        if rows.len() == 1000 {
            e.meta.put(REPO, &rows).await.unwrap();
            rows.clear();
        }
    }
    e.meta.put(REPO, &rows).await.unwrap();
    chain
}

/// Which of `ids` have a stored value of type `t`.
async fn stored<T: DerivedType>(e: &Env, t: &T, ids: &[ChangesetId]) -> Vec<bool> {
    let mut out = Vec::new();
    for chunk in ids.chunks(1000) {
        let keys: Vec<MetaKey> = chunk
            .iter()
            .map(|id| MetaKey::Derived(t.key(*id)))
            .collect();
        out.extend(
            e.meta
                .get(REPO, &keys)
                .await
                .unwrap()
                .iter()
                .map(Option::is_some),
        );
    }
    out
}

async fn base_and_chain(
    e: &Env,
    n: usize,
) -> (
    ops::Ops<store::PgMetaStore, store::ObjectBlobStore>,
    Vec<ChangesetId>,
) {
    let plain = ops(&e.meta, &e.blobs);
    let base = plain
        .create_changeset(
            REPO,
            changeset(&[], "base", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let chain = bare_chain(e, base, n).await;
    (plain, chain)
}

#[tokio::test]
async fn parents_lacking_eager_values_are_derived_within_the_bound() {
    let e = env().await;
    let (plain, chain) = base_and_chain(&e, 300).await;
    let tip = *chain.last().unwrap();
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
    assert_eq!(
        root_of(&fresh, child).await,
        root_of(&fresh, chain[0]).await
    );
    assert!(stored(&e, &RootTree, &chain).await.iter().all(|s| *s));
}

#[tokio::test]
async fn slow_a_parent_chain_at_the_bound_is_derived_in_one_request() {
    let e = env().await;
    let (plain, chain) = base_and_chain(&e, MAX_DERIVE_PER_REQUEST).await;
    let child = changeset(&[*chain.last().unwrap()], "child", vec![]);
    plain
        .create_changeset(REPO, child, &CreateOptions::default())
        .await
        .unwrap();
    assert!(stored(&e, &CommitGraph, &chain).await.iter().all(|s| *s));
}

#[tokio::test]
async fn slow_a_parent_chain_past_the_bound_is_finished_by_retries() {
    let e = env().await;
    let (plain, chain) = base_and_chain(&e, MAX_DERIVE_PER_REQUEST + 1).await;
    let child = changeset(&[*chain.last().unwrap()], "child", vec![]);
    let err = plain
        .create_changeset(REPO, child.clone(), &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable, "{err}");
    assert!(err.code.retryable());
    // Nothing was written for the child, and the first request derived
    // exactly the oldest MAX_DERIVE_PER_REQUEST ancestors of every type that
    // reads its parents' values, so one retry finishes.
    assert_eq!(
        e.meta
            .get(REPO, &[MetaKey::Changeset(child.id())])
            .await
            .unwrap(),
        vec![None]
    );
    let oldest =
        |s: Vec<bool>| s[..MAX_DERIVE_PER_REQUEST].iter().all(|s| *s) && !s[MAX_DERIVE_PER_REQUEST];
    assert!(oldest(stored(&e, &RootTree, &chain).await), "RootTree");
    assert!(
        oldest(stored(&e, &CommitGraph, &chain).await),
        "CommitGraph"
    );
    assert!(
        oldest(stored(&e, &CaseIndex::default(), &chain).await),
        "CaseIndex"
    );
    plain
        .create_changeset(REPO, child, &CreateOptions::default())
        .await
        .unwrap();
}

/// Writes exactly `n` bare changesets (no derived values) that are all
/// ancestors of the returned top, in few levels: 64,000 roots under 1,000
/// merges under 16 tips under one changeset, then a chain up to `n`.
async fn wide_bare_history(e: &Env, n: usize) -> ChangesetId {
    let mut rows = Vec::new();
    let mut put = |cs: Changeset| {
        let id = cs.id();
        rows.push((MetaKey::Changeset(id), Bytes::from(cs.encode())));
        id
    };
    let mut merges = Vec::new();
    for m in 0..1000 {
        let roots: Vec<ChangesetId> = (0..64)
            .map(|r| put(changeset(&[], &format!("root {m} {r}"), vec![])))
            .collect();
        merges.push(put(changeset(&roots, &format!("merge {m}"), vec![])));
    }
    let tips: Vec<ChangesetId> = merges
        .chunks(64)
        .enumerate()
        .map(|(i, c)| put(changeset(c, &format!("tip {i}"), vec![])))
        .collect();
    let mut top = put(changeset(&tips, "top", vec![]));
    let wide = 64_000 + 1000 + 16 + 1;
    for i in wide..n {
        top = put(changeset(&[top], &format!("chain {i}"), vec![]));
    }
    assert_eq!(rows.len(), n);
    for batch in rows.chunks(1000) {
        e.meta.put(REPO, batch).await.unwrap();
    }
    top
}

#[tokio::test]
async fn an_underived_ancestry_past_the_exploration_limit_is_refused_for_good() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let top = wide_bare_history(&e, MAX_DERIVE_EXPLORE + 1).await;
    let err = plain
        .create_changeset(
            REPO,
            changeset(&[top], "child", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::LimitExceeded, "{err}");
    assert!(!err.code.retryable());
}

#[tokio::test]
async fn slow_an_underived_ancestry_at_the_exploration_limit_is_pending_not_refused() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    let top = wide_bare_history(&e, MAX_DERIVE_EXPLORE).await;
    let err = plain
        .create_changeset(
            REPO,
            changeset(&[top], "child", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Unavailable, "{err}");
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

#[tokio::test]
async fn creating_a_child_never_case_checks_its_history() {
    let e = env().await;
    let plain = ops(&e.meta, &e.blobs);
    upload(&plain, b"r\n").await;
    // An exempt imported changeset with names that conflict by case, stored
    // with no derived values, as after a CaseIndex version bump.
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
    e.meta
        .put(
            REPO,
            &[(
                MetaKey::Changeset(imported.id()),
                Bytes::from(imported.encode()),
            )],
        )
        .await
        .unwrap();
    // A new child that touches neither name is created, with the check on.
    upload(&plain, b"n\n").await;
    plain
        .create_changeset(
            REPO,
            changeset(
                &[imported.id()],
                "child",
                vec![(path("new"), write(b"n\n"))],
            ),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    // The check still applies to the new changeset itself.
    let conflicting = changeset(
        &[imported.id()],
        "conflict",
        vec![(path("NEW"), write(b"n\n")), (path("new"), write(b"n\n"))],
    );
    let err = plain
        .create_changeset(REPO, conflicting, &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::CaseConflict);
}
