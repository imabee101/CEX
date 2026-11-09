//! Blob upload and read: blobs are stored as zstd frames, chunked above
//! `CHUNK_BYTES`, and recorded in the metadata store only after their
//! frames are stored (INV-22).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use derived::BlobRecord;
use scion_core::{BlobId, CHUNK_BYTES, ChunkId, ChunkList, Codec, Frame, FrameId, FrameKind};
use sha2::{Digest, Sha256};
use store::{BlobStore, MetaKey, MetaStore, ObjectKey, RepoId};
use tokio::task::JoinSet;

use crate::{Code, OpError};

/// A blob of at most this many bytes with no NUL in its first 8 KiB is text.
pub const MAX_TEXT_BYTES: usize = 16 << 20;

/// How many bytes are checked for NUL when classifying a blob.
pub const TEXT_SNIFF_BYTES: usize = 8 << 10;

/// Whether a blob is text.
pub fn is_text(bytes: &[u8]) -> bool {
    bytes.len() <= MAX_TEXT_BYTES && !bytes[..bytes.len().min(TEXT_SNIFF_BYTES)].contains(&0)
}

fn frame(id: FrameId, raw: &[u8]) -> Result<Bytes, OpError> {
    Ok(derived::encode_frame(id, raw)?)
}

/// The frames of one blob, in write order: its chunks, then the blob's own
/// frame (a blob frame or a chunk-list frame).
fn blob_frames(content: &[u8]) -> Result<(BlobId, Vec<(ObjectKey, Bytes)>), OpError> {
    let id = BlobId::hash(content);
    if content.len() <= CHUNK_BYTES {
        return Ok((
            id,
            vec![(ObjectKey::blob(id), frame(FrameId::Blob(id), content)?)],
        ));
    }
    let mut frames = Vec::new();
    let mut chunks = Vec::new();
    for piece in content.chunks(CHUNK_BYTES) {
        let cid = ChunkId::hash(piece);
        chunks.push(cid);
        frames.push((ObjectKey::chunk(cid), frame(FrameId::Chunk(cid), piece)?));
    }
    let list = ChunkList { chunks }.encode();
    let f = Frame::encode(
        FrameId::ChunkList(id),
        Codec::Raw,
        content.len() as u64,
        &list,
    )
    .map_err(|e| OpError::new(Code::DataLoss, format!("encoding a chunk list: {e}")))?;
    frames.push((ObjectKey::blob(id), Bytes::from(f)));
    Ok((id, frames))
}

/// Stores blobs that are not stored yet: their frames first, then, in one
/// metadata write, their records. Returns each content's id, in order.
pub async fn upload_blobs<M: MetaStore, B: BlobStore>(
    meta: &M,
    blobs: &Arc<B>,
    repo: RepoId,
    contents: &[Bytes],
) -> Result<Vec<BlobId>, OpError> {
    let ids: Vec<BlobId> = contents.iter().map(|c| BlobId::hash(c)).collect();
    let mut seen = HashSet::new();
    let unique: Vec<usize> = (0..ids.len()).filter(|&i| seen.insert(ids[i])).collect();
    let keys: Vec<MetaKey> = unique.iter().map(|&i| MetaKey::Blob(ids[i])).collect();
    let present = meta.get(repo, &keys).await?;
    let todo: Vec<usize> = unique
        .iter()
        .zip(&present)
        .filter(|(_, p)| p.is_none())
        .map(|(&i, _)| i)
        .collect();
    if todo.is_empty() {
        return Ok(ids);
    }
    let mut tasks = JoinSet::new();
    let mut records = Vec::new();
    for &i in &todo {
        let content = contents[i].clone();
        records.push((
            MetaKey::Blob(ids[i]),
            Bytes::from(
                BlobRecord {
                    size: content.len() as u64,
                    sha256: Sha256::digest(&content).into(),
                    git_sha1: None,
                    text: is_text(&content),
                }
                .encode(),
            ),
        ));
        while tasks.len() >= derived::FRAME_WRITES {
            join_one(&mut tasks).await?;
        }
        let blobs = blobs.clone();
        tasks.spawn(async move {
            let (_, frames) = blob_frames(&content)?;
            // In order, so a blob's own frame is stored after its chunks.
            for (key, f) in frames {
                blobs.put_if_absent(repo, key, f).await?;
            }
            Ok::<(), OpError>(())
        });
    }
    while !tasks.is_empty() {
        join_one(&mut tasks).await?;
    }
    meta.put(repo, &records).await?;
    Ok(ids)
}

async fn join_one(tasks: &mut JoinSet<Result<(), OpError>>) -> Result<(), OpError> {
    match tasks.join_next().await {
        Some(r) => {
            r.map_err(|e| OpError::new(Code::DataLoss, format!("upload task failed: {e}")))?
        }
        None => Ok(()),
    }
}

fn data_loss(id: impl std::fmt::Display, what: &str) -> OpError {
    OpError::new(Code::DataLoss, format!("blob {id}: {what}"))
}

fn decode_payload(bytes: &[u8], id: FrameId) -> Result<Vec<u8>, OpError> {
    let name = BlobId::from_bytes(*id.as_bytes());
    let f = Frame::decode(bytes).map_err(|e| data_loss(name, &format!("bad frame: {e}")))?;
    let h = f.header();
    if h.id() != id {
        return Err(data_loss(name, "frame names another object"));
    }
    match h.codec() {
        Codec::Raw => Ok(f.payload().to_vec()),
        Codec::Zstd => zstd::bulk::decompress(f.payload(), h.raw_len() as usize)
            .ok()
            .filter(|raw| raw.len() as u64 == h.raw_len())
            .ok_or_else(|| data_loss(name, "payload does not decompress to its length")),
    }
}

/// The maximum number of blob records in a classification metadata read.
const CLASSIFICATION_BATCH_SIZE: usize = 256;

/// Classifies distinct blobs as text or binary from their stored records, in
/// bounded metadata batches; no blob content is read.
pub(crate) async fn classify_blobs<M: MetaStore>(
    meta: &M,
    repo: RepoId,
    requests: &[(BlobId, u64)],
) -> Result<HashMap<BlobId, bool>, OpError> {
    let mut sizes = BTreeMap::new();
    for &(id, size) in requests {
        if sizes
            .insert(id, size)
            .is_some_and(|previous| previous != size)
        {
            return Err(data_loss(id, "tree entries disagree on blob size"));
        }
    }
    let mut out = HashMap::new();
    let mut lookups = Vec::new();
    for (&id, &size) in &sizes {
        if size > MAX_TEXT_BYTES as u64 {
            out.insert(id, false);
        } else {
            lookups.push(id);
        }
    }
    for batch in lookups.chunks(CLASSIFICATION_BATCH_SIZE) {
        let keys: Vec<MetaKey> = batch.iter().map(|&id| MetaKey::Blob(id)).collect();
        let records = meta.get(repo, &keys).await?;
        if records.len() != batch.len() {
            return Err(OpError::new(
                Code::Internal,
                "the store returned a short metadata batch",
            ));
        }
        for (&id, bytes) in batch.iter().zip(records) {
            let bytes = bytes.ok_or_else(|| data_loss(id, "missing blob record"))?;
            let record = BlobRecord::decode(&bytes)?;
            if record.size != sizes[&id] {
                return Err(data_loss(id, "tree and blob sizes differ"));
            }
            out.insert(id, record.text);
        }
    }
    Ok(out)
}

/// Reads a blob's bytes, verifying every frame and the blob's id.
pub async fn read_blob<B: BlobStore>(
    blobs: &B,
    repo: RepoId,
    id: BlobId,
) -> Result<Vec<u8>, OpError> {
    let bytes = blobs
        .get(repo, ObjectKey::blob(id))
        .await?
        .ok_or_else(|| OpError::new(Code::NotFound, format!("blob {id}")))?;
    let header = Frame::decode(&bytes)
        .map_err(|e| data_loss(id, &format!("bad frame: {e}")))?
        .header()
        .kind();
    let raw = if header == FrameKind::ChunkList {
        let list = ChunkList::decode(&decode_payload(&bytes, FrameId::ChunkList(id))?)
            .map_err(|e| data_loss(id, &format!("bad chunk list: {e}")))?;
        let mut raw = Vec::new();
        for c in list.chunks {
            let cb = blobs
                .get(repo, ObjectKey::chunk(c))
                .await?
                .ok_or_else(|| data_loss(id, "a chunk is missing"))?;
            let piece = decode_payload(&cb, FrameId::Chunk(c))?;
            if ChunkId::hash(&piece) != c {
                return Err(data_loss(id, "a chunk does not hash to its id"));
            }
            raw.extend_from_slice(&piece);
        }
        raw
    } else {
        decode_payload(&bytes, FrameId::Blob(id))?
    };
    if BlobId::hash(&raw) != id {
        return Err(data_loss(id, "bytes do not hash to the id"));
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use store::{MemoryBlobStore, MemoryMetaStore};

    use super::*;

    #[tokio::test]
    async fn blobs_round_trip_including_chunked_ones() {
        let meta = MemoryMetaStore::new();
        let blobs = Arc::new(MemoryBlobStore::new());
        let big: Vec<u8> = (0..CHUNK_BYTES * 2 + 5)
            .map(|i| (i * 31 % 251) as u8)
            .collect();
        let contents = vec![
            Bytes::new(),
            Bytes::from_static(b"hello\n"),
            Bytes::from(big.clone()),
            Bytes::from_static(b"hello\n"),
        ];
        let ids = upload_blobs(&meta, &blobs, RepoId(1), &contents)
            .await
            .unwrap();
        assert_eq!(ids[1], ids[3]);
        for (id, c) in ids.iter().zip(&contents) {
            assert_eq!(
                read_blob(&*blobs, RepoId(1), *id).await.unwrap(),
                c.to_vec()
            );
            let rec = meta
                .get(RepoId(1), &[MetaKey::Blob(*id)])
                .await
                .unwrap()
                .pop()
                .flatten()
                .unwrap();
            assert_eq!(BlobRecord::decode(&rec).unwrap().size, c.len() as u64);
        }
        assert!(
            blobs
                .contains(
                    RepoId(1),
                    ObjectKey::chunk(ChunkId::hash(&big[..CHUNK_BYTES]))
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reads_reject_bytes_that_do_not_hash_to_the_id() {
        let blobs = MemoryBlobStore::new();
        let id = BlobId::hash(b"real");
        blobs
            .put_if_absent(
                RepoId(1),
                ObjectKey::blob(id),
                frame(FrameId::Blob(id), b"fake").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            read_blob(&blobs, RepoId(1), id).await.unwrap_err().code,
            Code::DataLoss
        );
        assert_eq!(
            read_blob(&blobs, RepoId(1), BlobId::hash(b"x"))
                .await
                .unwrap_err()
                .code,
            Code::NotFound
        );
    }

    #[test]
    fn text_classification() {
        assert!(is_text(b"abc\n"));
        assert!(!is_text(b"a\0c"));
        let mut late_nul = vec![b'a'; TEXT_SNIFF_BYTES];
        late_nul.push(0);
        assert!(is_text(&late_nul));
    }
}
