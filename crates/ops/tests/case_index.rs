//! The private case-conflict index on Postgres and the object store: its
//! nodes stay in their own namespace, never become object frames or public
//! trees, and a cold server still finds conflicts with them.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::*;
use ops::{Code, CreateOptions};
use scion_core::Tree;
use store::fault::{FaultBlobStore, FaultMetaStore, Faults, Write};
use store::{BlobStore, MetaKey, MetaStore, ObjectKey};

#[tokio::test]
async fn case_nodes_are_private_and_have_no_object_frames() {
    let e = env().await;
    let faults = Faults::new();
    let meta = Arc::new(FaultMetaStore::new(e.meta.clone(), faults.clone()));
    let blobs = Arc::new(FaultBlobStore::new(e.blobs.clone(), faults.clone()));
    ops::blob::upload_blobs(&*e.meta, &e.blobs, REPO, &[Bytes::from_static(b"x")])
        .await
        .unwrap();
    let checked = ops(&meta, &blobs);
    let id = checked
        .create_changeset(
            REPO,
            changeset(
                &[],
                "private",
                vec![(path("restricted/Secret"), write(b"x"))],
            ),
            &CreateOptions::default(),
        )
        .await
        .unwrap();
    let mut tree_count = 0;
    let mut index_count = 0;
    for write in faults.writes() {
        if let Write::Meta(keys) = write {
            for key in keys {
                match key {
                    MetaKey::Tree(_) => tree_count += 1,
                    MetaKey::CaseNode(node) => {
                        index_count += 1;
                        let got = e.meta.get(REPO, &[key, MetaKey::Tree(node)]).await.unwrap();
                        assert_eq!(Tree::decode(got[0].as_ref().unwrap()).unwrap().id(), node);
                        assert!(got[1].is_none(), "private index escaped into trees");
                        assert!(!e.blobs.contains(REPO, ObjectKey::tree(node)).await.unwrap());
                    }
                    _ => {}
                }
            }
        }
    }
    assert!(index_count >= 2);
    assert_eq!(faults.counts().blob_puts, tree_count);
    // A new server must read the private namespace, and still reject conflicts.
    let cold = ops(&e.meta, &e.blobs);
    let err = cold
        .create_changeset(
            REPO,
            changeset(
                &[id],
                "conflict",
                vec![(path("restricted/secret"), write(b"x"))],
            ),
            &CreateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::CaseConflict);
    // Warming the private cache cannot make an index readable as a public tree.
    let value = cold
        .deriver()
        .get(REPO, &ops::case::CaseIndex::default(), id)
        .await
        .unwrap();
    let index = scion_core::TreeId::from_bytes(value.as_ref().try_into().unwrap());
    let mut cx = cold.deriver().cx(REPO);
    cx.case_nodes.get(index).await.unwrap();
    assert!(cx.trees.get(index).await.is_err());
}
