//! Frames: the single byte format for objects on the wire, in cache memory,
//! on daemon and client disks, and in blob storage.

use crate::wire::{Reader, put_uvarint, uvarint_len};
use crate::{BlobId, CHUNK_BYTES, ChunkId, FormatError, MAX_FIELD_BYTES, TreeId};

/// The four bytes every frame starts with.
pub const FRAME_MAGIC: [u8; 4] = *b"SCF1";

/// Length of the BLAKE3 digest that ends every frame.
pub const FRAME_DIGEST_LEN: usize = 32;

/// The longest possible frame header: magic, kind, id, codec, and two
/// 10-byte varints.
pub const MAX_FRAME_HEADER_LEN: usize = FIXED_HEADER_LEN + 10 + 10;

/// The header bytes before the two varints: magic, kind, id, and codec.
const FIXED_HEADER_LEN: usize = 4 + 1 + 32 + 1;

/// What a frame holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// A whole blob of at most `CHUNK_BYTES`; the id is its `BlobId`.
    Blob,
    /// One chunk of a chunked blob; the id is its `ChunkId`.
    Chunk,
    /// A tree node; the id is its `TreeId`.
    Tree,
    /// The chunk list of a blob larger than `CHUNK_BYTES`; the id is the
    /// blob's `BlobId` and the raw length is the blob's length.
    ChunkList,
}

impl FrameKind {
    /// The kind's byte in the frame header.
    pub fn code(self) -> u8 {
        match self {
            Self::Blob => 1,
            Self::Chunk => 2,
            Self::Tree => 3,
            Self::ChunkList => 4,
        }
    }

    /// The kind with header byte `code`.
    pub fn from_code(code: u8) -> Result<Self, FormatError> {
        match code {
            1 => Ok(Self::Blob),
            2 => Ok(Self::Chunk),
            3 => Ok(Self::Tree),
            4 => Ok(Self::ChunkList),
            code => Err(FormatError::InvalidCode {
                field: "frame kind",
                code,
            }),
        }
    }
}

/// How a frame's payload encodes the object's raw bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    /// The payload is the raw bytes.
    Raw,
    /// The payload is the raw bytes compressed with zstd.
    Zstd,
}

impl Codec {
    /// The codec's byte in the frame header.
    pub fn code(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::Zstd => 1,
        }
    }

    /// The codec with header byte `code`.
    pub fn from_code(code: u8) -> Result<Self, FormatError> {
        match code {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            code => Err(FormatError::InvalidCode {
                field: "frame codec",
                code,
            }),
        }
    }
}

/// The id in a frame header, typed by what the frame holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameId {
    /// A whole blob of at most `CHUNK_BYTES`.
    Blob(BlobId),
    /// One chunk of a chunked blob.
    Chunk(ChunkId),
    /// A tree node.
    Tree(TreeId),
    /// The chunk list of the blob with this id, which is larger than
    /// `CHUNK_BYTES`.
    ChunkList(BlobId),
}

impl FrameId {
    /// The id of kind `kind` with bytes `bytes`.
    pub const fn new(kind: FrameKind, bytes: [u8; 32]) -> Self {
        match kind {
            FrameKind::Blob => Self::Blob(BlobId::from_bytes(bytes)),
            FrameKind::Chunk => Self::Chunk(ChunkId::from_bytes(bytes)),
            FrameKind::Tree => Self::Tree(TreeId::from_bytes(bytes)),
            FrameKind::ChunkList => Self::ChunkList(BlobId::from_bytes(bytes)),
        }
    }

    /// What a frame with this id holds.
    pub const fn kind(&self) -> FrameKind {
        match self {
            Self::Blob(_) => FrameKind::Blob,
            Self::Chunk(_) => FrameKind::Chunk,
            Self::Tree(_) => FrameKind::Tree,
            Self::ChunkList(_) => FrameKind::ChunkList,
        }
    }

    /// The id's 32 bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        match self {
            Self::Blob(id) | Self::ChunkList(id) => id.as_bytes(),
            Self::Chunk(id) => id.as_bytes(),
            Self::Tree(id) => id.as_bytes(),
        }
    }
}

/// A frame header. Every header that exists obeys the length rules for its
/// kind and codec: it can only be built by [`FrameHeader::new`] or parsed by
/// [`FrameHeader::parse_prefix`], both of which check them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameHeader {
    id: FrameId,
    codec: Codec,
    raw_len: u64,
    payload_len: u64,
}

impl FrameHeader {
    /// Builds a header, checking that its lengths are valid for its kind and
    /// codec.
    pub fn new(
        id: FrameId,
        codec: Codec,
        raw_len: u64,
        payload_len: u64,
    ) -> Result<Self, FormatError> {
        let h = Self {
            id,
            codec,
            raw_len,
            payload_len,
        };
        h.check().map_err(FormatError::BadFrameLength)?;
        Ok(h)
    }

    /// What the frame holds.
    pub const fn kind(&self) -> FrameKind {
        self.id.kind()
    }

    /// The id of the object the frame holds.
    pub const fn id(&self) -> FrameId {
        self.id
    }

    /// How the payload encodes the raw bytes.
    pub const fn codec(&self) -> Codec {
        self.codec
    }

    /// The length of the object's raw bytes (for a chunk list, the blob's).
    pub const fn raw_len(&self) -> u64 {
        self.raw_len
    }

    /// The length of the payload.
    pub const fn payload_len(&self) -> u64 {
        self.payload_len
    }

    /// The rules every header obeys; the single place they are defined.
    fn check(&self) -> Result<(), FrameLengthRule> {
        let chunk = CHUNK_BYTES as u64;
        match self.kind() {
            FrameKind::Blob if self.raw_len > chunk => return Err(FrameLengthRule::BlobTooLong),
            FrameKind::Chunk if self.raw_len == 0 || self.raw_len > chunk => {
                return Err(FrameLengthRule::ChunkLength);
            }
            FrameKind::Tree if self.raw_len > MAX_FIELD_BYTES as u64 => {
                return Err(FrameLengthRule::TreeTooLong);
            }
            FrameKind::ChunkList => {
                if self.codec != Codec::Raw {
                    return Err(FrameLengthRule::ChunkListCodec);
                }
                if self.raw_len <= chunk {
                    return Err(FrameLengthRule::ChunkListBlobTooShort);
                }
                if self.payload_len != chunk_list_len(chunk_count(self.raw_len)) {
                    return Err(FrameLengthRule::ChunkListPayloadLength);
                }
                return Ok(());
            }
            _ => {}
        }
        match self.codec {
            Codec::Raw if self.payload_len != self.raw_len => {
                Err(FrameLengthRule::RawPayloadLength)
            }
            // Compression is used only when it shrinks the object.
            Codec::Zstd if self.payload_len >= self.raw_len => {
                Err(FrameLengthRule::ZstdPayloadNotShorter)
            }
            _ => Ok(()),
        }
    }

    /// The header's encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(&FRAME_MAGIC);
        out.push(self.kind().code());
        out.extend_from_slice(self.id.as_bytes());
        out.push(self.codec.code());
        put_uvarint(&mut out, self.raw_len);
        put_uvarint(&mut out, self.payload_len);
        out
    }

    /// The length of the header's encoding.
    pub fn encoded_len(&self) -> usize {
        FIXED_HEADER_LEN + uvarint_len(self.raw_len) + uvarint_len(self.payload_len)
    }

    /// The length of the whole frame: header, payload, and digest. It cannot
    /// overflow: a checked payload is at most `MAX_FIELD_BYTES` long.
    pub fn frame_len(&self) -> u64 {
        self.encoded_len() as u64 + self.payload_len + FRAME_DIGEST_LEN as u64
    }

    /// Parses the header at the start of `bytes`, which may hold more of the
    /// frame. Returns the header and its encoded length, or
    /// [`FormatError::Truncated`] if `bytes` ends inside the header.
    pub fn parse_prefix(bytes: &[u8]) -> Result<(Self, usize), FormatError> {
        let mut r = Reader::new(bytes);
        if r.take(4)? != FRAME_MAGIC {
            return Err(FormatError::BadMagic);
        }
        let kind = FrameKind::from_code(r.u8()?)?;
        let id = FrameId::new(kind, r.array32()?);
        let codec = Codec::from_code(r.u8()?)?;
        let raw_len = r.uvarint()?;
        let payload_len = r.uvarint()?;
        let h = Self::new(id, codec, raw_len, payload_len)?;
        Ok((h, bytes.len() - r.remaining()))
    }
}

/// A frame length rule, named by what it requires; see [`FrameHeader`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum FrameLengthRule {
    /// A blob frame holds at most `CHUNK_BYTES`.
    #[error("a blob frame holds at most CHUNK_BYTES")]
    BlobTooLong,
    /// A chunk holds 1 to `CHUNK_BYTES` bytes.
    #[error("a chunk holds 1 to CHUNK_BYTES bytes")]
    ChunkLength,
    /// A tree frame holds at most `MAX_FIELD_BYTES`.
    #[error("a tree frame holds at most MAX_FIELD_BYTES")]
    TreeTooLong,
    /// A chunk-list frame uses codec raw.
    #[error("a chunk-list frame uses codec raw")]
    ChunkListCodec,
    /// Only a blob larger than `CHUNK_BYTES` has a chunk list.
    #[error("only a blob larger than CHUNK_BYTES has a chunk list")]
    ChunkListBlobTooShort,
    /// A chunk list's payload holds exactly one id per chunk of the blob.
    #[error("chunk-list payload length does not match the blob length")]
    ChunkListPayloadLength,
    /// A raw payload's length equals the raw length.
    #[error("a raw payload's length equals the raw length")]
    RawPayloadLength,
    /// A zstd payload is shorter than the raw length.
    #[error("a zstd payload is shorter than the raw length")]
    ZstdPayloadNotShorter,
}

/// A decoded frame, borrowing its payload from the frame bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame<'a> {
    header: FrameHeader,
    payload: &'a [u8],
}

impl<'a> Frame<'a> {
    /// Encodes a frame: header, payload, then the BLAKE3 digest of both.
    ///
    /// For a [`FrameKind::ChunkList`] frame, `payload` must be the canonical
    /// chunk list for a blob of `raw_len` bytes.
    pub fn encode(
        id: FrameId,
        codec: Codec,
        raw_len: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, FormatError> {
        let header = FrameHeader::new(id, codec, raw_len, payload.len() as u64)?;
        if header.kind() == FrameKind::ChunkList {
            ChunkList::decode(payload)?;
        }
        let mut out = header.encode();
        out.reserve(payload.len() + FRAME_DIGEST_LEN);
        out.extend_from_slice(payload);
        let digest = blake3::hash(&out);
        out.extend_from_slice(digest.as_bytes());
        Ok(out)
    }

    /// Decodes a complete frame, checking its header rules, exact length, and
    /// digest. It does not decompress the payload or verify the object id.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, FormatError> {
        let (header, header_len) = FrameHeader::parse_prefix(bytes)?;
        let total = header.frame_len();
        if (bytes.len() as u64) < total {
            return Err(FormatError::Truncated);
        }
        if bytes.len() as u64 > total {
            return Err(FormatError::TrailingBytes(
                (bytes.len() as u64 - total) as usize,
            ));
        }
        let digest_at = bytes.len() - FRAME_DIGEST_LEN;
        let payload = &bytes[header_len..digest_at];
        if blake3::hash(&bytes[..digest_at]).as_bytes() != &bytes[digest_at..] {
            return Err(FormatError::DigestMismatch);
        }
        if header.kind() == FrameKind::ChunkList {
            // The header already fixed the payload length to exactly the
            // blob's chunk count, so a well-formed list has that count.
            ChunkList::decode(payload)?;
        }
        Ok(Self { header, payload })
    }

    /// The frame's header.
    pub fn header(&self) -> &FrameHeader {
        &self.header
    }

    /// The frame's payload.
    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

/// The number of `CHUNK_BYTES` chunks a blob of `raw_len` bytes is split
/// into when it is larger than `CHUNK_BYTES`.
pub fn chunk_count(raw_len: u64) -> u64 {
    raw_len.div_ceil(CHUNK_BYTES as u64)
}

fn chunk_list_len(count: u64) -> u64 {
    uvarint_len(count) as u64 + 32 * count
}

/// The payload of a chunk-list frame: a `uvarint` count, then each chunk's
/// `ChunkId` in order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkList {
    /// The blob's chunks, in order.
    pub chunks: Vec<ChunkId>,
}

impl ChunkList {
    /// The canonical encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(chunk_list_len(self.chunks.len() as u64) as usize);
        put_uvarint(&mut out, self.chunks.len() as u64);
        for c in &self.chunks {
            out.extend_from_slice(c.as_bytes());
        }
        out
    }

    /// Decodes a chunk list, rejecting anything but its canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        let mut r = Reader::new(bytes);
        let n = r.count(32)?;
        let mut chunks = Vec::with_capacity(n);
        for _ in 0..n {
            chunks.push(ChunkId::from_bytes(r.array32()?));
        }
        r.finish()?;
        Ok(Self { chunks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const C: u64 = CHUNK_BYTES as u64;

    fn header(
        kind: FrameKind,
        codec: Codec,
        raw: u64,
        payload: u64,
    ) -> Result<FrameHeader, FormatError> {
        FrameHeader::new(FrameId::new(kind, [7; 32]), codec, raw, payload)
    }

    #[test]
    fn size_limits_by_kind() {
        use Codec::*;
        use FrameKind::*;
        assert!(header(Blob, Raw, 0, 0).is_ok());
        assert!(header(Blob, Raw, C, C).is_ok());
        assert!(header(Blob, Raw, C + 1, C + 1).is_err());
        assert!(header(Blob, Zstd, C + 1, 10).is_err());
        assert!(header(Chunk, Raw, 0, 0).is_err());
        assert!(header(Chunk, Raw, 1, 1).is_ok());
        assert!(header(Chunk, Zstd, C, 9).is_ok());
        assert!(header(Chunk, Zstd, C + 1, 9).is_err());
        assert!(header(Tree, Raw, MAX_FIELD_BYTES as u64, MAX_FIELD_BYTES as u64).is_ok());
        assert!(
            header(
                Tree,
                Raw,
                MAX_FIELD_BYTES as u64 + 1,
                MAX_FIELD_BYTES as u64 + 1
            )
            .is_err()
        );
        assert!(header(Blob, Raw, 5, 4).is_err());
        assert!(header(Blob, Zstd, 5, 5).is_err());
        assert!(header(Blob, Zstd, 5, 4).is_ok());
        assert!(header(Blob, Zstd, 0, 0).is_err());
    }

    #[test]
    fn each_rule_is_reported_by_name() {
        use Codec::*;
        use FrameKind::*;
        use FrameLengthRule as R;
        let rule = |kind, codec, raw, payload| match header(kind, codec, raw, payload) {
            Err(FormatError::BadFrameLength(rule)) => Some(rule),
            Ok(_) => None,
            Err(e) => panic!("unexpected error {e}"),
        };
        assert_eq!(rule(Blob, Raw, C + 1, C + 1), Some(R::BlobTooLong));
        assert_eq!(rule(Chunk, Raw, 0, 0), Some(R::ChunkLength));
        let over = MAX_FIELD_BYTES as u64 + 1;
        assert_eq!(rule(Tree, Raw, over, over), Some(R::TreeTooLong));
        assert_eq!(rule(ChunkList, Zstd, C + 1, 65), Some(R::ChunkListCodec));
        assert_eq!(rule(ChunkList, Raw, C, 33), Some(R::ChunkListBlobTooShort));
        assert_eq!(
            rule(ChunkList, Raw, C + 1, 33),
            Some(R::ChunkListPayloadLength)
        );
        assert_eq!(rule(Blob, Raw, 5, 4), Some(R::RawPayloadLength));
        assert_eq!(rule(Blob, Zstd, 5, 5), Some(R::ZstdPayloadNotShorter));
        assert_eq!(rule(Blob, Zstd, 5, 4), None);
    }

    #[test]
    fn ids_carry_their_kind() {
        for kind in [
            FrameKind::Blob,
            FrameKind::Chunk,
            FrameKind::Tree,
            FrameKind::ChunkList,
        ] {
            let id = FrameId::new(kind, [9; 32]);
            assert_eq!(id.kind(), kind);
            assert_eq!(id.as_bytes(), &[9; 32]);
        }
        let h = header(FrameKind::Tree, Codec::Raw, 2, 2).unwrap();
        assert_eq!(h.id(), FrameId::Tree(TreeId::from_bytes([7; 32])));
        assert_eq!(
            (h.kind(), h.codec(), h.raw_len(), h.payload_len()),
            (FrameKind::Tree, Codec::Raw, 2, 2)
        );
    }

    #[test]
    fn chunk_list_lengths() {
        use FrameKind::ChunkList as L;
        assert!(header(L, Codec::Raw, C, 1 + 32).is_err());
        assert!(header(L, Codec::Raw, C + 1, 1 + 64).is_ok());
        assert!(header(L, Codec::Raw, C + 1, 1 + 32).is_err());
        assert!(header(L, Codec::Raw, 2 * C, 1 + 64).is_ok());
        assert!(header(L, Codec::Raw, 2 * C + 1, 1 + 96).is_ok());
        assert!(header(L, Codec::Zstd, C + 1, 1 + 64).is_err());
        // 128 chunks need a 2-byte count.
        assert!(header(L, Codec::Raw, 128 * C, 2 + 128 * 32).is_ok());
        assert!(header(L, Codec::Raw, 128 * C, 1 + 128 * 32).is_err());
        assert_eq!(chunk_count(C + 1), 2);
        assert_eq!(chunk_count(2 * C), 2);
    }

    #[test]
    fn encode_decode_round_trip() {
        let f = Frame::encode(
            FrameId::new(FrameKind::Blob, [1; 32]),
            Codec::Raw,
            3,
            b"abc",
        )
        .unwrap();
        let d = Frame::decode(&f).unwrap();
        assert_eq!(d.payload(), b"abc");
        assert_eq!(d.header().encoded_len(), f.len() - 3 - 32);
        let mut bad = f.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(Frame::decode(&bad), Err(FormatError::DigestMismatch));
        assert_eq!(
            Frame::decode(&f[..f.len() - 1]),
            Err(FormatError::Truncated)
        );
        let mut long = f.clone();
        long.push(0);
        assert_eq!(Frame::decode(&long), Err(FormatError::TrailingBytes(1)));
    }

    #[test]
    fn parse_prefix_reports_truncation() {
        let f = Frame::encode(
            FrameId::new(FrameKind::Tree, [2; 32]),
            Codec::Raw,
            2,
            &[1, 0],
        )
        .unwrap();
        for n in 0..f.len() - 2 - 32 {
            assert_eq!(
                FrameHeader::parse_prefix(&f[..n]).map(|_| ()),
                Err(FormatError::Truncated),
                "{n}"
            );
        }
        assert!(FrameHeader::parse_prefix(&f[..f.len() - 34]).is_ok());
    }
}
