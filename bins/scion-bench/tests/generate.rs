//! The bench generator end to end on Postgres and the object store, at a
//! small size: the tip holds exactly the layout's files with their seeded
//! contents, built as a chain of `files_per_changeset` changesets; the
//! cached repo is reused, not rebuilt; and `scion-bench gen` reports it.

use std::sync::Arc;

use derived::{CacheSizes, Deriver, RootTree};
use scion_bench::{BENCH, BenchParams, REPO, cached_repo, content, generate, layout};
use scion_core::{BlobId, EntryTarget, FileKind, RepoPath};
use store::testing::{TestDatabase, base_config, object_store};
use store::{MetaStore, ObjectBlobStore, PgMetaStore};

const SMALL: BenchParams = BenchParams {
    files: 250,
    files_per_changeset: 100,
    ..BENCH
};

/// The tip of `SMALL` for `GENERATOR_VERSION` 1.
const SMALL_TIP: &str = "ff8fee915b6292bee4cb9bbf19b1abe0562e46e1e5d1f49fda44e9e5dd4aa917";

async fn template_oid(template: &str) -> u32 {
    let mut config = base_config();
    config.dbname("postgres");
    let (client, conn) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);
    client
        .query_one(
            "SELECT oid FROM pg_database WHERE datname = $1",
            &[&template],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_small_bench_repo_holds_its_layout_and_is_reused() {
    let repo = cached_repo(&SMALL).await;
    let oid = template_oid(&repo.template).await;
    // A second call finds the same repo without rebuilding it.
    let again = cached_repo(&SMALL).await;
    assert_eq!((again.tip, &again.template), (repo.tip, &repo.template));
    assert_eq!(
        template_oid(&repo.template).await,
        oid,
        "the repo was rebuilt"
    );

    let db = TestDatabase::create_from(&repo.template).await;
    let meta = Arc::new(PgMetaStore::new(db.config().clone(), 4).unwrap());
    assert_eq!(
        meta.get_ref(REPO, "main").await.unwrap().unwrap().target,
        repo.tip
    );

    // Generating afresh, into empty stores, gives the same tip: the
    // generator is deterministic, and its output is pinned.
    let fresh_db = TestDatabase::create().await;
    let fresh_meta = Arc::new(PgMetaStore::new(fresh_db.config().clone(), 8).unwrap());
    fresh_meta.migrate().await.unwrap();
    let fresh_blobs = Arc::new(ObjectBlobStore::new(Arc::new(object_store())));
    let tip = generate(&SMALL, fresh_meta.clone(), fresh_blobs.clone()).await;
    assert_eq!(tip, repo.tip);
    assert_eq!(tip.to_string(), SMALL_TIP);
    let (meta, blobs) = (fresh_meta, fresh_blobs);
    let d = Deriver::new(meta, blobs, CacheSizes::default());
    // A chain of ⌈250 / 100⌉ = 3 changesets, the last two with one parent.
    let mut chain = vec![repo.tip];
    loop {
        let cs = d.changeset(REPO, *chain.last().unwrap()).await.unwrap();
        match cs.parts().parents.as_slice() {
            [] => break,
            [p] => chain.push(*p),
            more => panic!("{} parents", more.len()),
        }
    }
    assert_eq!(chain.len(), 3);
    let root = RootTree::decode(&d.get(REPO, &RootTree, repo.tip).await.unwrap()).unwrap();
    let mut cx = d.cx(REPO);
    let files = layout(&SMALL);
    assert_eq!(files.len(), 250);
    let mut total = 0;
    for f in &files {
        let c = content(&SMALL, f);
        let got = cx
            .trees
            .lookup_path(root, &RepoPath::new(f.path.as_bytes()).unwrap())
            .await
            .unwrap();
        match got {
            Some(EntryTarget::File {
                kind: FileKind::Regular,
                blob,
                size,
                ..
            }) => {
                assert_eq!((blob, size), (BlobId::hash(&c), f.size), "{}", f.path);
                total += 1;
            }
            other => panic!("{}: {other:?}", f.path),
        }
    }
    assert_eq!(total, 250);

    // The binary reports the same repo, and refuses unknown commands.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_scion-bench"))
        .args(["gen", "250"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let full = cached_repo(&BenchParams {
        files: 250,
        ..BENCH
    })
    .await;
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains(&full.template), "{text}");
    assert!(text.contains(&full.tip.to_string()), "{text}");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_scion-bench"))
        .arg("bogus")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
}

/// The generator uploads at most `UPLOADS_AHEAD` (6) changesets' blobs
/// ahead of the create chain: when changeset `k` (0-based) is written, at
/// most `k + 6` batches' blob records are stored, so its memory stays
/// bounded however many files the repo has.
#[tokio::test(flavor = "multi_thread")]
async fn uploads_run_a_bounded_distance_ahead_of_creates() {
    use store::MetaKey;
    use store::fault::{FaultBlobStore, FaultMetaStore, Faults, Write};
    const AHEAD: usize = 6;
    let p = BenchParams {
        files: 600,
        files_per_changeset: 4,
        ..BENCH
    };
    let db = TestDatabase::create().await;
    let pg = Arc::new(PgMetaStore::new(db.config().clone(), 16).unwrap());
    pg.migrate().await.unwrap();
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(pg, faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(
        Arc::new(ObjectBlobStore::new(Arc::new(object_store()))),
        faults.clone(),
    ));
    generate(&p, meta, blobs).await;
    let mut blob_records = 0usize;
    let mut creates = 0usize;
    for w in faults.writes() {
        let Write::Meta(keys) = w else { continue };
        blob_records += keys
            .iter()
            .filter(|k| matches!(k, MetaKey::Blob(_)))
            .count();
        if keys.iter().any(|k| matches!(k, MetaKey::Changeset(_))) {
            // Each batch is at most files_per_changeset distinct blobs.
            let bound = (creates + AHEAD) * p.files_per_changeset;
            assert!(
                blob_records <= bound,
                "{blob_records} blob records stored at create {creates}, bound {bound}"
            );
            creates += 1;
        }
    }
    assert_eq!(creates, 150);
}
