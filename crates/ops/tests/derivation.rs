//! The derived-data mechanism on Postgres and the object store (INV-20):
//! values written by create equal values derived from scratch in a store
//! holding only the changesets and blobs; concurrent derivations of one key
//! on a server compute and write it once; and CommitGraph and
//! RestrictedRoots equal naive models.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use common::*;
use derived::{AclConfig, CommitGraph, DerivedType, GraphNode, RestrictedRoots, RootTree};
use ops::CreateOptions;
use ops::blob::upload_blobs;
use ops::case::CaseIndex;
use scion_core::{ChangesetId, FileKind, RepoPath, TreeId};
use store::fault::{FaultBlobStore, FaultMetaStore, Faults, Write};
use store::{MetaKey, MetaStore};

fn acl(version: u64, restricted: &[&str]) -> Arc<AclConfig> {
    let mut domains = BTreeMap::new();
    for p in restricted {
        domains.insert(path(p), "secret".to_owned());
    }
    domains.insert(path("open"), "default".to_owned());
    Arc::new(AclConfig {
        version,
        default_domain: "default".into(),
        domains,
    })
}

struct Dag {
    ids: Vec<ChangesetId>,
    parents: HashMap<ChangesetId, Vec<ChangesetId>>,
    models: HashMap<ChangesetId, Model>,
}

/// A random DAG of `n` changesets with merges and several roots, created
/// through CreateChangeset under `config`.
async fn build_dag<M: MetaStore, B: store::BlobStore>(
    ops: &ops::Ops<M, B>,
    n: usize,
    seed: u64,
    config: &Arc<AclConfig>,
) -> Dag {
    let opts = CreateOptions {
        acl: config.clone(),
        ..CreateOptions::default()
    };
    let mut rng = Rng(seed);
    let mut dag = Dag {
        ids: Vec::new(),
        parents: HashMap::new(),
        models: HashMap::new(),
    };
    let dirs = ["sec", "sec/inner", "open", "pub/a", "pub/b", "deep/sec2/x"];
    while dag.ids.len() < n {
        let i = dag.ids.len();
        let mut parents = Vec::new();
        if i > 0 && !i.is_multiple_of(23) {
            parents.push(dag.ids[rng.below(i as u64) as usize]);
            if rng.below(4) == 0 {
                let other = dag.ids[rng.below(i as u64) as usize];
                if !parents.contains(&other) {
                    parents.push(other);
                }
            }
        }
        let mut model = parents
            .first()
            .map(|p| dag.models[p].clone())
            .unwrap_or_default();
        let mut spec = Vec::new();
        for k in 0..1 + rng.below(3) {
            let p = format!(
                "{}/f{}",
                dirs[rng.below(dirs.len() as u64) as usize],
                rng.below(4)
            );
            if spec.iter().any(|(q, _): &(String, _)| *q == p) {
                continue;
            }
            let del = model.files.contains_key(p.as_bytes()) && rng.below(3) == 0;
            spec.push((
                p,
                if del {
                    None
                } else {
                    Some(format!("{i}.{k}").into_bytes())
                },
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
        let (fc, contents) = model.apply(&s);
        let d = ops.deriver();
        upload_blobs(&**d.meta(), d.blobs(), REPO, &contents)
            .await
            .unwrap();
        let cs = changeset(&parents, &format!("{seed}/{i}"), fc);
        match ops.create_changeset(REPO, cs, &opts).await {
            Ok(id) => {
                dag.ids.push(id);
                dag.parents.insert(id, parents);
                dag.models.insert(id, model);
            }
            Err(e) => assert_eq!(e.code, ops::Code::PathConflict, "{e}"),
        }
    }
    dag
}

#[tokio::test]
async fn incremental_derivation_equals_derivation_from_scratch() {
    let first = env().await;
    let config = acl(1, &["sec", "sec/inner", "deep/sec2", "missing/dir"]);
    let ops1 = ops(&first.meta, &first.blobs);
    let dag = build_dag(&ops1, 120, 11, &config).await;

    // A second database with only the changesets and blob records.
    let second = bare_copy(&first, &dag).await;
    let ops2 = ops(&second.meta, &first.blobs);
    let d2 = ops2.deriver();
    let restricted = RestrictedRoots::new(config.clone());
    // The changeset with the most ancestors; deriving it derives every
    // ancestor first.
    let ancestors_of = |tip: ChangesetId| {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![tip];
        while let Some(c) = stack.pop() {
            if seen.insert(c) {
                stack.extend(dag.parents[&c].iter().copied());
            }
        }
        seen
    };
    let tip = *dag
        .ids
        .iter()
        .max_by_key(|id| ancestors_of(**id).len())
        .unwrap();
    d2.get(REPO, &RootTree, tip).await.unwrap();
    d2.get(REPO, &CommitGraph, tip).await.unwrap();
    d2.get(REPO, &restricted, tip).await.unwrap();
    d2.get(REPO, &CaseIndex::default(), tip).await.unwrap();

    async fn values<T: DerivedType>(
        meta: &store::PgMetaStore,
        t: &T,
        ids: &[ChangesetId],
    ) -> Vec<Option<Bytes>> {
        let keys: Vec<MetaKey> = ids.iter().map(|id| MetaKey::Derived(t.key(*id))).collect();
        meta.get(REPO, &keys).await.unwrap()
    }
    // The tip's ancestors were derived without being asked for directly.
    let seen = ancestors_of(tip);
    let ancestors: Vec<ChangesetId> = dag
        .ids
        .iter()
        .copied()
        .filter(|c| seen.contains(c))
        .collect();
    assert!(ancestors.len() > 10, "{}", ancestors.len());
    for (name, a, b) in [
        (
            "RootTree",
            values(&first.meta, &RootTree, &ancestors).await,
            values(&second.meta, &RootTree, &ancestors).await,
        ),
        (
            "CommitGraph",
            values(&first.meta, &CommitGraph, &ancestors).await,
            values(&second.meta, &CommitGraph, &ancestors).await,
        ),
        (
            "CaseIndex",
            values(&first.meta, &CaseIndex::default(), &ancestors).await,
            values(&second.meta, &CaseIndex::default(), &ancestors).await,
        ),
    ] {
        assert!(
            a.iter().all(Option::is_some),
            "{name}: create wrote every value"
        );
        assert_eq!(a, b, "{name}: incremental and from-scratch values differ");
    }
    // RestrictedRoots does not read its parents' values: deriving the tip
    // derived no ancestor's.
    let below: Vec<ChangesetId> = ancestors.iter().copied().filter(|c| *c != tip).collect();
    assert!(
        values(&second.meta, &restricted, &below)
            .await
            .iter()
            .all(Option::is_none)
    );
    // Every other changeset, derived from scratch too, also matches. Create
    // derived RestrictedRoots from each first parent's stored value; here it
    // is derived children first, so no parent's value is stored yet and each
    // comes from looking its roots up directly: two independent paths.
    for id in &dag.ids {
        d2.get(REPO, &RootTree, *id).await.unwrap();
        d2.get(REPO, &CommitGraph, *id).await.unwrap();
        d2.get(REPO, &CaseIndex::default(), *id).await.unwrap();
    }
    for id in dag.ids.iter().rev() {
        d2.get(REPO, &restricted, *id).await.unwrap();
    }
    assert_eq!(
        values(&first.meta, &RootTree, &dag.ids).await,
        values(&second.meta, &RootTree, &dag.ids).await
    );
    assert_eq!(
        values(&first.meta, &CommitGraph, &dag.ids).await,
        values(&second.meta, &CommitGraph, &dag.ids).await
    );
    assert_eq!(
        values(&first.meta, &restricted, &dag.ids).await,
        values(&second.meta, &restricted, &dag.ids).await
    );
    assert_eq!(
        values(&first.meta, &CaseIndex::default(), &dag.ids).await,
        values(&second.meta, &CaseIndex::default(), &dag.ids).await
    );
    // And every root equals the reference root of the changeset's files.
    for id in &ancestors {
        assert_eq!(root_of(&ops2, *id).await, dag.models[id].root());
    }
}

/// The skew-binary jump depth for a node at depth `d`.
fn skew_depth(d: u64) -> u64 {
    if d == 0 {
        return 0;
    }
    let p = d - 1;
    let j = skew_depth(p);
    let jj = skew_depth(j);
    if p - j == j - jj { jj } else { p }
}

#[tokio::test]
async fn commit_graph_and_restricted_roots_equal_naive_models() {
    let env = env().await;
    let config = acl(
        3,
        &["sec", "sec/inner", "deep/sec2", "missing/dir", "pub/a/f1"],
    );
    let ops = ops(&env.meta, &env.blobs);
    let dag = build_dag(&ops, 150, 5, &config).await;
    let d = ops.deriver();
    let mut nodes: HashMap<ChangesetId, GraphNode> = HashMap::new();
    for id in &dag.ids {
        nodes.insert(
            *id,
            GraphNode::decode(&d.get(REPO, &CommitGraph, *id).await.unwrap()).unwrap(),
        );
    }
    // Naive ancestor sets.
    let mut anc: HashMap<ChangesetId, std::collections::HashSet<ChangesetId>> = HashMap::new();
    for id in &dag.ids {
        let mut s = std::collections::HashSet::from([*id]);
        for p in &dag.parents[id] {
            s.extend(anc[p].iter().copied());
        }
        anc.insert(*id, s);
    }
    let mut skip_parent: HashMap<ChangesetId, Option<ChangesetId>> = HashMap::new();
    let skip_chain = |x: ChangesetId, sp: &HashMap<ChangesetId, Option<ChangesetId>>| {
        let mut chain = vec![x];
        while let Some(Some(p)) = sp.get(chain.last().unwrap()) {
            chain.push(*p);
        }
        chain
    };
    for id in &dag.ids {
        let n = &nodes[id];
        let ps = &dag.parents[id];
        assert_eq!(&n.parents, ps);
        let generation = ps
            .iter()
            .map(|p| nodes[p].generation + 1)
            .max()
            .unwrap_or(1);
        assert_eq!(n.generation, generation);
        // First-parent chain.
        let mut chain = vec![*id];
        while let Some(p) = dag.parents[chain.last().unwrap()].first() {
            chain.push(*p);
        }
        let depth = chain.len() as u64 - 1;
        assert_eq!(n.p1_depth, depth);
        assert_eq!(n.p1_skew, chain[(depth - skew_depth(depth)) as usize]);
        // Skip tree: the closest common ancestor of the parents' skip chains.
        let sp = match ps.split_first() {
            None => None,
            Some((first, rest)) => {
                let mut cur = Some(*first);
                for p in rest {
                    let Some(c) = cur else { break };
                    let other: std::collections::HashSet<_> =
                        skip_chain(*p, &skip_parent).into_iter().collect();
                    cur = skip_chain(c, &skip_parent)
                        .into_iter()
                        .find(|x| other.contains(x));
                }
                cur
            }
        };
        assert_eq!(n.skip_parent, sp, "skip parent of {id}");
        if let Some(sp) = sp {
            // The skip parent is a common ancestor of every parent.
            assert!(ps.iter().all(|p| anc[p].contains(&sp)));
        }
        skip_parent.insert(*id, sp);
        let chain = skip_chain(*id, &skip_parent);
        let depth = chain.len() as u64 - 1;
        assert_eq!(n.skip_depth, depth);
        assert_eq!(n.skip_skew, chain[(depth - skew_depth(depth)) as usize]);
    }
    assert!(
        dag.parents.values().any(|p| p.len() == 2),
        "the DAG has merges"
    );
    assert!(
        dag.parents.values().filter(|p| p.is_empty()).count() > 1,
        "the DAG has several roots"
    );
    assert!(skip_parent.values().any(Option::is_none) && skip_parent.len() > 1);

    // RestrictedRoots: the tree of each restricted prefix that is a
    // directory, from the reference builder.
    let restricted = RestrictedRoots::new(config.clone());
    let mut seen_some = false;
    for id in &dag.ids {
        let got = RestrictedRoots::decode(&d.get(REPO, &restricted, *id).await.unwrap()).unwrap();
        let model = &dag.models[id];
        let mut want: BTreeMap<RepoPath, TreeId> = BTreeMap::new();
        for prefix in config.restricted() {
            let mut under = prefix.as_bytes().to_vec();
            under.push(b'/');
            let files: Vec<_> = model
                .files
                .iter()
                .filter(|(p, _)| p.starts_with(&under))
                .map(|(p, (k, c))| (p[under.len()..].to_vec(), *k, c.clone()))
                .collect();
            if !files.is_empty() {
                want.insert(prefix.clone(), reference_root(files));
            }
        }
        seen_some |= !want.is_empty();
        assert_eq!(got, want, "RestrictedRoots of {id}");
    }
    assert!(seen_some);
    // Another config version is its own key space.
    let other = RestrictedRoots::new(acl(4, &[]));
    assert_ne!(other.key(dag.ids[0]), restricted.key(dag.ids[0]));
    assert_eq!(
        RestrictedRoots::decode(&d.get(REPO, &other, dag.ids[0]).await.unwrap()).unwrap(),
        BTreeMap::new()
    );
}

#[tokio::test]
async fn concurrent_derivations_of_one_key_write_it_once() {
    let env = env().await;
    let config = acl(1, &["sec"]);
    let base_ops = ops(&env.meta, &env.blobs);
    let dag = build_dag(&base_ops, 40, 3, &config).await;
    let tip = *dag.ids.last().unwrap();
    // A new ACL config version: its RestrictedRoots key space is underived.
    let fresh = RestrictedRoots::new(acl(2, &["sec", "pub/a"]));

    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(env.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(env.blobs.clone(), faults.clone()));
    let server = Arc::new(ops(&meta, &blobs));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let server = server.clone();
        let fresh = fresh.clone();
        tasks.spawn(async move { server.deriver().get(REPO, &fresh, tip).await.unwrap() });
    }
    let mut results = Vec::new();
    while let Some(r) = tasks.join_next().await {
        results.push(r.unwrap());
    }
    assert!(results.windows(2).all(|w| w[0] == w[1]));
    let mut writes: HashMap<MetaKey, usize> = HashMap::new();
    for w in faults.writes() {
        if let Write::Meta(keys) = w {
            for k in keys {
                *writes.entry(k).or_default() += 1;
            }
        }
    }
    let derived: Vec<_> = writes
        .iter()
        .filter(|(k, _)| matches!(k, MetaKey::Derived(_)))
        .collect();
    // RestrictedRoots does not read its parents' values, so only the tip's
    // value is derived, once.
    assert_eq!(derived.len(), 1, "{derived:?}");
    assert_eq!(*derived[0].1, 1, "written more than once");
    let stored = env
        .meta
        .get(REPO, &[MetaKey::Derived(fresh.key(tip))])
        .await
        .unwrap()
        .pop()
        .flatten()
        .unwrap();
    assert_eq!(stored, results[0]);

    // Two servers deriving another fresh key at once store one value, which
    // both return.
    let fresh = RestrictedRoots::new(acl(5, &["pub"]));
    let a = ops(&env.meta, &env.blobs);
    let b = ops(&env.meta, &env.blobs);
    let (ra, rb) = tokio::join!(
        a.deriver().get(REPO, &fresh, tip),
        b.deriver().get(REPO, &fresh, tip)
    );
    let stored = env
        .meta
        .get(REPO, &[MetaKey::Derived(fresh.key(tip))])
        .await
        .unwrap()
        .pop()
        .flatten()
        .unwrap();
    assert_eq!((ra.unwrap(), rb.unwrap()), (stored.clone(), stored));
}

/// A restricted root's tree is re-read when a file beneath it changes in
/// the same changeset as a sibling whose name sorts between the root and
/// its contents (`a/b.c` sorts between `a/b` and `a/b/c`).
#[tokio::test]
async fn restricted_roots_follow_changes_next_to_sorting_siblings() {
    let env = env().await;
    let config = acl(1, &["a/b"]);
    let ops = ops(&env.meta, &env.blobs);
    let opts = CreateOptions {
        acl: config.clone(),
        ..CreateOptions::default()
    };
    let restricted = RestrictedRoots::new(config);
    let mut m = Model::default();
    let mut parent: Vec<ChangesetId> = Vec::new();
    for (i, round) in [
        vec![("a/b/c", "1"), ("a/b.c", "1")],
        vec![("a/b/c", "2"), ("a/b.c", "2")],
        vec![("a/b/c", "3"), ("a/b-c", "3"), ("a/b.c", "3")],
    ]
    .into_iter()
    .enumerate()
    {
        let specs: Vec<Spec<'_>> = round
            .iter()
            .map(|(p, c)| (*p, Some((FileKind::Regular, c.as_bytes()))))
            .collect();
        let (changes, contents) = m.apply(&specs);
        let d = ops.deriver();
        upload_blobs(&**d.meta(), d.blobs(), REPO, &contents)
            .await
            .unwrap();
        let id = ops
            .create_changeset(REPO, changeset(&parent, &format!("r{i}"), changes), &opts)
            .await
            .unwrap();
        let got = RestrictedRoots::decode(&d.get(REPO, &restricted, id).await.unwrap()).unwrap();
        let mut sub = Model::default();
        for (p, (k, c)) in &m.files {
            if let Some(rest) = p.strip_prefix(b"a/b/") {
                sub.files.insert(rest.to_vec(), (*k, c.clone()));
            }
        }
        assert_eq!(got.get(&path("a/b")), Some(&sub.root()), "round {i}");
        parent = vec![id];
    }
}

/// Concurrent derivations of a type that reads its parents' values, on a
/// database holding only changesets and blob records: every ancestor's value
/// is derived, and each is written exactly once (INV-20).
#[tokio::test]
async fn concurrent_derivations_of_a_history_write_each_value_once() {
    let first = env().await;
    let config = acl(1, &["sec"]);
    let dag = build_dag(&ops(&first.meta, &first.blobs), 120, 11, &config).await;
    let ancestors = |tip: ChangesetId| {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![tip];
        while let Some(c) = stack.pop() {
            if seen.insert(c) {
                stack.extend(dag.parents[&c].iter().copied());
            }
        }
        seen.len()
    };
    let tip = *dag.ids.iter().max_by_key(|id| ancestors(**id)).unwrap();
    let bare = bare_copy(&first, &dag).await;
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(bare.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(first.blobs.clone(), faults.clone()));
    let server = Arc::new(ops(&meta, &blobs));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let server = server.clone();
        tasks.spawn(async move { server.deriver().get(REPO, &CommitGraph, tip).await.unwrap() });
    }
    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    let mut writes: HashMap<MetaKey, usize> = HashMap::new();
    for w in faults.writes() {
        if let Write::Meta(keys) = w {
            for k in keys {
                *writes.entry(k).or_default() += 1;
            }
        }
    }
    let graph_writes: Vec<_> = writes
        .iter()
        .filter(|(k, _)| matches!(k, MetaKey::Derived(d) if d.kind == CommitGraph.key(tip).kind))
        .collect();
    assert!(
        graph_writes.len() > 10,
        "the tip's ancestors were derived: {}",
        graph_writes.len()
    );
    for (k, n) in &graph_writes {
        assert_eq!(**n, 1, "{k:?} was written {n} times");
    }
}

/// A second database holding only `dag`'s changesets and blob records.
async fn bare_copy(first: &Env, dag: &Dag) -> Env {
    let second = env().await;
    let mut keys: Vec<MetaKey> = dag.ids.iter().map(|id| MetaKey::Changeset(*id)).collect();
    for id in &dag.ids {
        for (_, c) in dag.models[id].files.values() {
            let k = MetaKey::Blob(scion_core::BlobId::hash(c));
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
    }
    let rows: Vec<(MetaKey, Bytes)> = keys
        .iter()
        .zip(first.meta.get(REPO, &keys).await.unwrap())
        .map(|(k, v)| (*k, v.unwrap()))
        .collect();
    second.meta.put(REPO, &rows).await.unwrap();
    second
}
