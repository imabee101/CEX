//! CreateChangeset's validation, on Postgres and the object store: no-op
//! entries (INV-11), path-prefix and file/directory conflicts (INV-12), and
//! case conflicts per Unicode 18.0 (INV-19).

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use ops::blob::upload_blobs;
use ops::{Code, CreateOptions, OpError, Ops};
use scion_core::{ChangesetId, FileChange, FileKind};
use store::{MetaKey, MetaStore, ObjectBlobStore, PgMetaStore};

type PgOps = Ops<PgMetaStore, ObjectBlobStore>;

/// A repo with `a/x`, `a/y`, `b`, and `README`, and its ops.
async fn base(env: &Env) -> (PgOps, ChangesetId) {
    let ops = ops(&env.meta, &env.blobs);
    let mut m = Model::default();
    let (changes, contents) = m.apply(&[
        ("a/x", Some((FileKind::Regular, b"x\n"))),
        ("a/y", Some((FileKind::Regular, b"y\n"))),
        ("b", Some((FileKind::Executable, b"#!b\n"))),
        ("README", Some((FileKind::Regular, b"read me\n"))),
    ]);
    let id = commit(&ops, &[], "base", changes, &contents).await.unwrap();
    (ops, id)
}

async fn try_create(
    ops: &PgOps,
    parent: ChangesetId,
    changes: Vec<(&str, FileChange)>,
    opts: &CreateOptions,
) -> Result<ChangesetId, OpError> {
    let contents: Vec<Bytes> = changes
        .iter()
        .map(|(p, _)| Bytes::from(format!("content of {p}\n")))
        .collect();
    let d = ops.deriver();
    upload_blobs(&**d.meta(), d.blobs(), REPO, &contents)
        .await
        .unwrap();
    let changes = changes.into_iter().map(|(p, c)| (path(p), c)).collect();
    ops.create_changeset(REPO, changeset(&[parent], "change", changes), opts)
        .await
}

fn content(p: &str) -> FileChange {
    write(format!("content of {p}\n").as_bytes())
}

async fn expect(
    ops: &PgOps,
    parent: ChangesetId,
    changes: Vec<(&str, FileChange)>,
    code: Code,
    at: &str,
) {
    let err = try_create(ops, parent, changes, &CreateOptions::default())
        .await
        .expect_err("the changeset must be rejected");
    assert_eq!(err.code, code, "{err}");
    assert_eq!(err.paths, vec![at.as_bytes().to_vec()], "{err}");
}

/// Asserts the rejected changeset left nothing behind in the metadata store.
async fn assert_not_stored(env: &Env, parent: ChangesetId, changes: Vec<(&str, FileChange)>) {
    let changes = changes.into_iter().map(|(p, c)| (path(p), c)).collect();
    let id = changeset(&[parent], "change", changes).id();
    let got = env.meta.get(REPO, &[MetaKey::Changeset(id)]).await.unwrap();
    assert_eq!(got, vec![None]);
}

#[tokio::test]
async fn no_op_entries_are_rejected() {
    let env = env().await;
    let (ops, base) = base(&env).await;
    // Writing the same kind and blob a path already has.
    expect(
        &ops,
        base,
        vec![("a/x", write(b"x\n"))],
        Code::NoOpChange,
        "a/x",
    )
    .await;
    // Deleting a file that does not exist, at the top level and nested.
    expect(
        &ops,
        base,
        vec![("nope", FileChange::Deleted)],
        Code::NoOpChange,
        "nope",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("a/nope", FileChange::Deleted)],
        Code::NoOpChange,
        "a/nope",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("new/dir/f", FileChange::Deleted)],
        Code::NoOpChange,
        "new/dir/f",
    )
    .await;
    // A no-op among real changes still fails the whole changeset.
    expect(
        &ops,
        base,
        vec![("a/x", write(b"x\n")), ("c", content("c"))],
        Code::NoOpChange,
        "a/x",
    )
    .await;
    assert_not_stored(
        &env,
        base,
        vec![("a/x", write(b"x\n")), ("c", content("c"))],
    )
    .await;
    // A kind change of the same blob is a real change.
    let exec = FileChange::File {
        kind: FileKind::Executable,
        blob: scion_core::BlobId::hash(b"x\n"),
        size: 2,
    };
    try_create(&ops, base, vec![("a/x", exec)], &CreateOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn path_prefix_and_file_directory_conflicts_are_rejected() {
    let env = env().await;
    let (ops, base) = base(&env).await;
    // Two changed paths where the shorter one is written, not deleted.
    expect(
        &ops,
        base,
        vec![("c", content("c")), ("c/d", content("c/d"))],
        Code::PathConflict,
        "c",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("b", content("b")), ("b/d", content("b/d"))],
        Code::PathConflict,
        "b",
    )
    .await;
    // A file beneath a file that stays.
    expect(
        &ops,
        base,
        vec![("b/d", content("b/d"))],
        Code::PathConflict,
        "b",
    )
    .await;
    // A file over a directory, and a delete of a directory.
    expect(
        &ops,
        base,
        vec![("a", content("a"))],
        Code::PathConflict,
        "a",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("a", FileChange::Deleted)],
        Code::PathConflict,
        "a",
    )
    .await;
    // A directory becoming a file in one changeset breaks the prefix rule.
    expect(
        &ops,
        base,
        vec![
            ("a", content("a")),
            ("a/x", FileChange::Deleted),
            ("a/y", FileChange::Deleted),
        ],
        Code::PathConflict,
        "a",
    )
    .await;
    // The exception's shape over a directory: "deleting" a directory and
    // writing beneath it would replace the directory's contents.
    expect(
        &ops,
        base,
        vec![("a", FileChange::Deleted), ("a/z", content("a/z"))],
        Code::PathConflict,
        "a",
    )
    .await;
    assert_not_stored(&env, base, vec![("b/d", content("b/d"))]).await;
    // The exception: a deleted file whose path becomes a directory.
    let id = try_create(
        &ops,
        base,
        vec![("b", FileChange::Deleted), ("b/d", content("b/d"))],
        &CreateOptions::default(),
    )
    .await
    .unwrap();
    let mut m = Model::default();
    m.apply(&[
        ("a/x", Some((FileKind::Regular, b"x\n"))),
        ("a/y", Some((FileKind::Regular, b"y\n"))),
        ("README", Some((FileKind::Regular, b"read me\n"))),
        ("b/d", Some((FileKind::Regular, b"content of b/d\n"))),
    ]);
    assert_eq!(root_of(&ops, id).await, m.root());
}

#[tokio::test]
async fn case_conflicts_are_rejected_unless_exempt() {
    let env = env().await;
    let (ops, base) = base(&env).await;
    // Case, normalization, and simple folding, against existing names.
    expect(
        &ops,
        base,
        vec![("readme", content("readme"))],
        Code::CaseConflict,
        "readme",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("A/z", content("A/z"))],
        Code::CaseConflict,
        "A",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("a/X", content("a/X"))],
        Code::CaseConflict,
        "a/X",
    )
    .await;
    // Between two names added together.
    expect(
        &ops,
        base,
        vec![
            ("caf\u{e9}", content("caf\u{e9}")),
            ("cafe\u{301}", content("cafe\u{301}")),
        ],
        Code::CaseConflict,
        "cafe\u{301}",
    )
    .await;
    expect(
        &ops,
        base,
        vec![("\u{212a}", content("\u{212a}")), ("k", content("k"))],
        Code::CaseConflict,
        "k",
    )
    .await;
    assert_not_stored(&env, base, vec![("readme", content("readme"))]).await;
    // Full folding is not used: sharp s and "ss" do not conflict.
    try_create(
        &ops,
        base,
        vec![
            ("stra\u{df}e", content("stra\u{df}e")),
            ("strasse", content("strasse")),
        ],
        &CreateOptions::default(),
    )
    .await
    .unwrap();
    // Replacing a file with a differently cased name in one changeset is
    // fine: the old name is gone from the resulting tree.
    try_create(
        &ops,
        base,
        vec![
            ("README", FileChange::Deleted),
            ("Readme", content("Readme")),
        ],
        &CreateOptions::default(),
    )
    .await
    .unwrap();
    // The repo setting turns the check off.
    let off = CreateOptions {
        case_check: false,
        ..CreateOptions::default()
    };
    try_create(&ops, base, vec![("readme", content("readme"))], &off)
        .await
        .unwrap();
    // Imported changesets are exempt.
    let d = ops.deriver();
    upload_blobs(&**d.meta(), d.blobs(), REPO, &[Bytes::from_static(b"r\n")])
        .await
        .unwrap();
    let mut parts =
        changeset(&[base], "imported", vec![(path("ReadMe"), write(b"r\n"))]).into_parts();
    parts.git_headers = Some(b"author A <a@b> 0 +0000\n".to_vec());
    let cs = scion_core::Changeset::new(parts).unwrap();
    let imported = CreateOptions {
        imported: true,
        ..CreateOptions::default()
    };
    ops.create_changeset(REPO, cs.clone(), &imported)
        .await
        .unwrap();
    // Without the import flag, git_headers are refused.
    let err = ops
        .create_changeset(REPO, cs, &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::InvalidArgument);
}

/// The case-conflict check uses Unicode 18.0: the normalization and case
/// mapping data linked in are generated from it.
#[test]
fn case_conflict_checks_use_unicode_18() {
    assert_eq!(ops::case::UNICODE_VERSION, "18.0.0");
    assert_eq!(
        icu_normalizer_data::unicode_tag!(),
        ops::case::UNICODE_VERSION
    );
    assert_eq!(icu_casemap_data::unicode_tag!(), ops::case::UNICODE_VERSION);
}

#[tokio::test]
async fn missing_parents_and_blobs_and_bad_input_are_rejected() {
    let env = env().await;
    let (ops, base) = base(&env).await;
    let missing = ChangesetId::from_bytes([9; 32]);
    let err = ops
        .create_changeset(
            REPO,
            changeset(&[missing], "m", vec![(path("q"), write(b"q"))]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::NotFound);
    // A blob that was never uploaded.
    let err = ops
        .create_changeset(
            REPO,
            changeset(&[base], "m", vec![(path("q"), write(b"never uploaded"))]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::NotFound);
    // A size that is not the blob's.
    let wrong = FileChange::File {
        kind: FileKind::Regular,
        blob: scion_core::BlobId::hash(b"x\n"),
        size: 3,
    };
    let err = ops
        .create_changeset(
            REPO,
            changeset(&[base], "m", vec![(path("q"), wrong)]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (err.code, err.paths),
        (Code::InvalidArgument, vec![b"q".to_vec()])
    );
    // Duplicate parents.
    let err = ops
        .create_changeset(
            REPO,
            changeset(&[base, base], "m", vec![]),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::InvalidArgument);
    // Non-canonical bytes.
    let mut bytes = changeset(&[base], "m", vec![]).encode();
    bytes.push(0);
    let err = ops
        .create_changeset_bytes(REPO, &bytes, &CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::NotCanonical);
}

#[tokio::test]
async fn limits_are_enforced_except_on_imports() {
    let env = env().await;
    let (ops, base) = base(&env).await;
    let long_name = "n".repeat(ops::MAX_COMPONENT_BYTES + 1);
    let err = try_create(
        &ops,
        base,
        vec![(long_name.as_str(), content(&long_name))],
        &CreateOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, Code::LimitExceeded);
    let mut parts = changeset(&[base], "m", vec![]).into_parts();
    parts.message = vec![b'm'; ops::MAX_MESSAGE_BYTES + 1];
    let err = ops
        .create_changeset(
            REPO,
            scion_core::Changeset::new(parts.clone()).unwrap(),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::LimitExceeded);
    parts.message = b"m".to_vec();
    parts.author = vec![b'a'; ops::MAX_AUTHOR_BYTES + 1];
    let err = ops
        .create_changeset(
            REPO,
            scion_core::Changeset::new(parts.clone()).unwrap(),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::LimitExceeded);
    parts.git_headers = Some(b"h".to_vec());
    let imported = CreateOptions {
        imported: true,
        ..CreateOptions::default()
    };
    ops.create_changeset(REPO, scion_core::Changeset::new(parts).unwrap(), &imported)
        .await
        .unwrap();
}

#[tokio::test]
async fn creating_an_existing_changeset_again_succeeds_and_writes_nothing() {
    let env = env().await;
    let (_, base) = base(&env).await;
    let faults = store::fault::Faults::new();
    let meta = Arc::new(store::fault::FaultMetaStore::new(
        env.meta.clone(),
        faults.clone(),
    ));
    let blobs = Arc::new(store::fault::FaultBlobStore::new(
        env.blobs.clone(),
        faults.clone(),
    ));
    let again = ops(&meta, &blobs);
    let mut m = Model::default();
    let (changes, _) = m.apply(&[
        ("a/x", Some((FileKind::Regular, b"x\n"))),
        ("a/y", Some((FileKind::Regular, b"y\n"))),
        ("b", Some((FileKind::Executable, b"#!b\n"))),
        ("README", Some((FileKind::Regular, b"read me\n"))),
    ]);
    let id = again
        .create_changeset(
            REPO,
            changeset(&[], "base", changes),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(id, base);
    assert!(faults.writes().is_empty(), "{:?}", faults.writes());
}

/// The limits are AGENTS.md's, inclusive: a value at a limit is accepted
/// and one byte (or parent) more is rejected.
#[tokio::test]
async fn limits_are_exact() {
    assert_eq!(ops::MAX_AUTHOR_BYTES, 1024);
    assert_eq!(ops::MAX_PATH_BYTES, 4096);
    assert_eq!(ops::MAX_COMPONENT_BYTES, 255);
    assert_eq!(ops::MAX_MESSAGE_BYTES, 1 << 20);
    assert_eq!(ops::MAX_PARENTS, 64);
    assert_eq!(ops::MAX_SIGNATURE_BYTES, 64 << 10);
    let env = env().await;
    let (ops, base) = base(&env).await;
    let opts = CreateOptions::default();
    let create = |parts: scion_core::ChangesetParts| {
        let ops = &ops;
        let opts = &opts;
        async move {
            ops.create_changeset(REPO, scion_core::Changeset::new(parts).unwrap(), opts)
                .await
        }
    };
    let check = |r: Result<ChangesetId, OpError>, ok: bool, what: &str| match (r, ok) {
        (Ok(_), true) => {}
        (Err(e), false) => assert_eq!(e.code, Code::LimitExceeded, "{what}: {e}"),
        (r, _) => panic!("{what}: {r:?}"),
    };
    for (over, ok) in [(0, true), (1, false)] {
        let mut p = changeset(&[base], &format!("a{over}"), vec![]).into_parts();
        p.author = vec![b'a'; 1024 + over];
        check(create(p).await, ok, "author");
        let mut p = changeset(&[base], "", vec![]).into_parts();
        p.message = vec![b'm'; (1 << 20) + over];
        check(create(p).await, ok, "message");
        let mut p = changeset(&[base], &format!("s{over}"), vec![]).into_parts();
        p.signature = Some(vec![b's'; (64 << 10) + over]);
        check(create(p).await, ok, "signature");
        // 17 components of 240 bytes and 16 slashes: 4,096 bytes.
        let mut comps = vec!["c".repeat(240); 17];
        comps[0].push_str(&"c".repeat(over));
        let long = comps.join("/");
        check(
            try_create(&ops, base, vec![(long.as_str(), content(&long))], &opts).await,
            ok,
            "path",
        );
        let name = "n".repeat(255 + over);
        check(
            try_create(&ops, base, vec![(name.as_str(), content(&name))], &opts).await,
            ok,
            "component",
        );
    }
    // 64 parents are accepted, 65 are not.
    let mut roots = Vec::new();
    for i in 0..65 {
        let p = changeset(&[], &format!("root {i}"), vec![]).into_parts();
        roots.push(create(p).await.unwrap());
    }
    for (n, ok) in [(64, true), (65, false)] {
        let p = changeset(&roots[..n], "merge", vec![]).into_parts();
        check(create(p).await, ok, "parents");
    }
}
