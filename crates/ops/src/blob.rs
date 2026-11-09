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

/// The chunk-list frame of a blob of `len` bytes stored as `chunks`. The
/// caller passes a consistent list, so a failure is a server defect.
fn chunk_list_frame(id: BlobId, len: u64, chunks: Vec<ChunkId>) -> Result<Bytes, OpError> {
    Frame::encode(
        FrameId::ChunkList(id),
        Codec::Raw,
        len,
        &ChunkList { chunks }.encode(),
    )
    .map(Bytes::from)
    .map_err(|e| OpError::new(Code::Internal, format!("encoding a chunk list: {e}")))
}

/// Stores one blob's frames, put-if-absent: its chunks in order, then the
/// blob's own frame (a blob frame or a chunk-list frame), so the blob's
/// frame is stored only after its chunks. Each frame is built just before
/// it is written, so beyond the content itself this holds at most one
/// chunk's frame.
async fn store_blob<B: BlobStore>(blobs: &B, repo: RepoId, content: &[u8]) -> Result<(), OpError> {
    let id = BlobId::hash(content);
    if content.len() <= CHUNK_BYTES {
        blobs
            .put_if_absent(repo, ObjectKey::blob(id), frame(FrameId::Blob(id), content)?)
            .await?;
        return Ok(());
    }
    let mut chunks = Vec::new();
    for piece in content.chunks(CHUNK_BYTES) {
        let cid = ChunkId::hash(piece);
        chunks.push(cid);
        blobs
            .put_if_absent(repo, ObjectKey::chunk(cid), frame(FrameId::Chunk(cid), piece)?)
            .await?;
    }
    let list = chunk_list_frame(id, content.len() as u64, chunks)?;
    blobs.put_if_absent(repo, ObjectKey::blob(id), list).await?;
    Ok(())
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
        tasks.spawn(async move { store_blob(&*blobs, repo, &content).await });
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
            r.map_err(|e| OpError::new(Code::Internal, format!("upload task failed: {e}")))?
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

/// Reads a blob of `size` bytes (the size its tree entry and record give),
/// verifying every frame and the blob's id. Memory is bounded by `size`:
/// a stored frame that declares another length is data loss before any
/// chunk is read, and chunks are read one at a time.
pub async fn read_blob<B: BlobStore>(
    blobs: &B,
    repo: RepoId,
    id: BlobId,
    size: u64,
) -> Result<Vec<u8>, OpError> {
    let bytes = blobs
        .get(repo, ObjectKey::blob(id))
        .await?
        .ok_or_else(|| OpError::new(Code::NotFound, format!("blob {id}")))?;
    let header = *Frame::decode(&bytes)
        .map_err(|e| data_loss(id, &format!("bad frame: {e}")))?
        .header();
    if header.raw_len() != size {
        return Err(data_loss(id, "the stored frame has another length"));
    }
    let raw = if header.kind() == FrameKind::ChunkList {
        let list = ChunkList::decode(&decode_payload(&bytes, FrameId::ChunkList(id))?)
            .map_err(|e| data_loss(id, &format!("bad chunk list: {e}")))?;
        let mut raw = Vec::with_capacity(size as usize);
        for c in list.chunks {
            let cb = blobs
                .get(repo, ObjectKey::chunk(c))
                .await?
                .ok_or_else(|| data_loss(id, "a chunk is missing"))?;
            let piece = decode_payload(&cb, FrameId::Chunk(c))?;
            if ChunkId::hash(&piece) != c {
                return Err(data_loss(id, "a chunk does not hash to its id"));
            }
            if (raw.len() + piece.len()) as u64 > size {
                return Err(data_loss(id, "the chunks are longer than the blob"));
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
                read_blob(&*blobs, RepoId(1), *id, c.len() as u64)
                    .await
                    .unwrap(),
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
            read_blob(&blobs, RepoId(1), id, 4).await.unwrap_err().code,
            Code::DataLoss
        );
        assert_eq!(
            read_blob(&blobs, RepoId(1), BlobId::hash(b"x"), 1)
                .await
                .unwrap_err()
                .code,
            Code::NotFound
        );
    }


    /// A blob store whose writes panic, and which counts its reads.
    #[derive(Default)]
    struct Probe {
        inner: MemoryBlobStore,
        panic_on_put: bool,
        gets: std::sync::atomic::AtomicUsize,
    }

    impl BlobStore for Probe {
        async fn put_if_absent(
            &self,
            repo: RepoId,
            key: ObjectKey,
            bytes: Bytes,
        ) -> Result<store::PutOutcome, store::StoreError> {
            assert!(!self.panic_on_put, "the store's write panicked");
            self.inner.put_if_absent(repo, key, bytes).await
        }

        async fn get(
            &self,
            repo: RepoId,
            key: ObjectKey,
        ) -> Result<Option<Bytes>, store::StoreError> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.get(repo, key).await
        }

        async fn contains(&self, repo: RepoId, key: ObjectKey) -> Result<bool, store::StoreError> {
            self.inner.contains(repo, key).await
        }
    }

    #[tokio::test]
    async fn a_failed_upload_task_is_internal_and_records_nothing() {
        let meta = MemoryMetaStore::new();
        let blobs = Arc::new(Probe {
            panic_on_put: true,
            ..Probe::default()
        });
        let content = Bytes::from_static(b"x\n");
        let err = upload_blobs(&meta, &blobs, RepoId(1), std::slice::from_ref(&content))
            .await
            .unwrap_err();
        assert_eq!(err.code, Code::Internal, "{err}");
        let id = BlobId::hash(&content);
        assert_eq!(
            meta.get(RepoId(1), &[MetaKey::Blob(id)]).await.unwrap(),
            vec![None]
        );
    }

    #[test]
    fn a_chunk_list_that_does_not_encode_is_internal() {
        // A blob short enough for one frame has no chunk list.
        let err = chunk_list_frame(BlobId::hash(b"x"), 1, vec![ChunkId::hash(b"x")]).unwrap_err();
        assert_eq!(err.code, Code::Internal, "{err}");
    }

    /// A read holds at most the size it was asked for: a stored frame of
    /// another length fails before any chunk is read, and chunks that run
    /// past the size fail before they are kept.
    #[tokio::test]
    async fn reads_hold_no_more_than_the_blob_size() {
        let meta = MemoryMetaStore::new();
        let blobs = Arc::new(Probe::default());
        let big: Vec<u8> = (0..CHUNK_BYTES * 3).map(|i| (i % 253) as u8).collect();
        let id = upload_blobs(&meta, &blobs, RepoId(1), &[Bytes::from(big.clone())])
            .await
            .unwrap()[0];
        let gets = || blobs.gets.load(std::sync::atomic::Ordering::SeqCst);
        for size in [big.len() as u64 - 1, big.len() as u64 + 1, 1] {
            let before = gets();
            let err = read_blob(&*blobs, RepoId(1), id, size).await.unwrap_err();
            assert_eq!(err.code, Code::DataLoss, "{size}: {err}");
            assert_eq!(gets() - before, 1, "{size}: chunks were read");
        }
        assert_eq!(
            read_blob(&*blobs, RepoId(1), id, big.len() as u64)
                .await
                .unwrap(),
            big
        );
        // A list of two full chunks for a blob one byte past a chunk.
        let full = vec![b'a'; CHUNK_BYTES];
        let chunk = ChunkId::hash(&full);
        blobs
            .put_if_absent(
                RepoId(1),
                ObjectKey::chunk(chunk),
                frame(FrameId::Chunk(chunk), &full).unwrap(),
            )
            .await
            .unwrap();
        let len = CHUNK_BYTES as u64 + 1;
        let fake = BlobId::hash(b"claimed");
        blobs
            .put_if_absent(
                RepoId(1),
                ObjectKey::blob(fake),
                chunk_list_frame(fake, len, vec![chunk, chunk]).unwrap(),
            )
            .await
            .unwrap();
        let err = read_blob(&*blobs, RepoId(1), fake, len).await.unwrap_err();
        assert_eq!(err.code, Code::DataLoss);
        assert!(err.message.contains("longer than the blob"), "{err}");
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
