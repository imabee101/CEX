//! M1 golden-vector acceptance through the real development backends.

#[path = "../../scion-core/tests/support/mod.rs"]
mod support;

use object_store::ObjectStoreExt;
use scion_core::{BlobId, Changeset, ChunkId, ChunkList, Frame, Tree};
use store::testing::{TestDatabase, TestPrefix};
use support::vectors::{self, Ty};

#[tokio::test]
async fn canonical_vectors_and_ids_survive_postgres_and_object_storage() {
    let vectors = vectors::parse(include_str!("../../scion-core/vectors/v1.txt")).unwrap();
    assert_eq!(vectors, vectors::all());
    let db = TestDatabase::create().await;
    let client = db.connect().await;
    client
        .batch_execute("CREATE TABLE vectors (name TEXT PRIMARY KEY, bytes BYTEA NOT NULL)")
        .await
        .unwrap();
    let prefix = TestPrefix::create();
    let objects = prefix.store();
    for vector in vectors {
        client
            .execute(
                "INSERT INTO vectors VALUES ($1, $2)",
                &[&vector.name, &vector.bytes],
            )
            .await
            .unwrap();
        let key = vector.name.clone().into();
        objects
            .put(&key, vector.bytes.clone().into())
            .await
            .unwrap();
        let row = client
            .query_one("SELECT bytes FROM vectors WHERE name = $1", &[&vector.name])
            .await
            .unwrap();
        let postgres: Vec<u8> = row.get(0);
        let object = objects.get(&key).await.unwrap().bytes().await.unwrap();
        for bytes in [postgres.as_slice(), object.as_ref()] {
            assert_eq!(
                bytes, vector.bytes,
                "{}: storage changed bytes",
                vector.name
            );
            let decoded = match vector.ty {
                Ty::Blob => Ok((bytes.to_vec(), Some(*BlobId::hash(bytes).as_bytes()))),
                Ty::Chunk => Ok((bytes.to_vec(), Some(*ChunkId::hash(bytes).as_bytes()))),
                Ty::Changeset => {
                    Changeset::decode(bytes).map(|v| (v.encode(), Some(*v.id().as_bytes())))
                }
                Ty::Tree => Tree::decode(bytes).map(|v| (v.encode(), Some(*v.id().as_bytes()))),
                Ty::Frame => Frame::decode(bytes).map(|v| {
                    let h = v.header();
                    (
                        Frame::encode(h.id(), h.codec(), h.raw_len(), v.payload()).unwrap(),
                        Some(*h.id().as_bytes()),
                    )
                }),
                Ty::ChunkList => ChunkList::decode(bytes).map(|v| (v.encode(), None)),
            };
            if vector.accept {
                let (encoded, id) = decoded.unwrap();
                assert_eq!(encoded, vector.bytes, "{}", vector.name);
                assert_eq!(id, vector.id, "{}", vector.name);
            } else {
                assert!(
                    decoded.is_err(),
                    "{}: noncanonical input accepted",
                    vector.name
                );
            }
        }
        objects.delete(&key).await.unwrap();
    }
}
