//! Diff between two changesets' trees: the changed paths, their line
//! counts, or a unified patch, one page at a time in path byte order. Equal
//! subtrees are skipped by id, so the walk skips matching directory and
//! shard ids without loading their entries.

use std::collections::{BinaryHeap, VecDeque};

use derived::{DeriveError, RootTree, TreeWork};
use imara_diff::{Algorithm, Diff, InternedInput};
use scion_core::{BlobId, ChangesetId, EntryTarget, FileKind, RepoPath, TreeEntry, TreeId};
use store::{BlobStore, MetaStore, RepoId};

use tokio::task::JoinSet;

use crate::blob::{MAX_TEXT_BYTES, classify_blobs, is_text, read_blob};
use crate::{Code, OpError, Ops};

/// Lines of context around each patch hunk.
pub const PATCH_CONTEXT: u32 = 3;

/// What a diff reports per path.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DiffMode {
    /// Paths and their old and new entries.
    Names,
    /// Also added and removed line counts.
    Stat,
    /// Also a unified patch.
    Patch,
}

/// A file entry on one side of a diff.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FileState {
    /// The file's kind.
    pub kind: FileKind,
    /// The file's blob.
    pub blob: BlobId,
    /// The file's size.
    pub size: u64,
}

/// One changed file.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct FileDiff {
    /// The path.
    pub path: Vec<u8>,
    /// The entry in `a`, if any.
    pub old: Option<FileState>,
    /// The entry in `b`, if any.
    pub new: Option<FileState>,
    /// Whether either side is binary; then there are no line counts or
    /// patch.
    pub binary: bool,
    /// Lines added (Stat and Patch modes).
    pub added_lines: u64,
    /// Lines removed (Stat and Patch modes).
    pub removed_lines: u64,
    /// The unified patch hunks (Patch mode).
    pub patch: Vec<u8>,
}

fn file(t: Option<EntryTarget>) -> Option<FileState> {
    match t {
        Some(EntryTarget::File {
            kind, blob, size, ..
        }) => Some(FileState { kind, blob, size }),
        _ => None,
    }
}

fn dir(t: Option<EntryTarget>) -> Option<TreeId> {
    match t {
        Some(EntryTarget::Tree(id)) => Some(id),
        _ => None,
    }
}

fn under(path: &[u8], prefix: &[u8]) -> bool {
    path == prefix
        || (path.len() > prefix.len() && path.starts_with(prefix) && path[prefix.len()] == b'/')
}

/// Whether a file at `path` passes the filters.
fn keep_file(path: &[u8], filters: &[RepoPath]) -> bool {
    filters.is_empty() || filters.iter().any(|f| under(path, f.as_bytes()))
}

fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
    let mut p = dir.to_vec();
    if !p.is_empty() {
        p.push(b'/');
    }
    p.extend_from_slice(name);
    p
}

/// One entry name with its target on each side.
type Pair = (Vec<u8>, Option<EntryTarget>, Option<EntryTarget>);

/// The entries that differ between two directories, in name order, read
/// lazily: a shard is expanded only when the merge reaches it, and an entry
/// or shard whose names all sort before `from` (and none of which is a
/// prefix of it) is dropped unread. When a path filter names only some of
/// the directory's entries, those are looked up directly instead (`only`).
/// Memory is bounded by the nodes the request reads, which it keeps until
/// it ends, plus the filter's names, which the request carries anyway.
struct Pairs {
    a: VecDeque<TreeEntry>,
    b: VecDeque<TreeEntry>,
    from: Option<Vec<u8>>,
    only: Option<Only>,
}

/// The names a path filter selects in one directory, and the pairs already
/// looked up for them.
struct Only {
    tops: (Option<TreeId>, Option<TreeId>),
    names: VecDeque<Vec<u8>>,
    ready: VecDeque<Pair>,
}

/// How many filtered names one batched lookup reads. A page stops after the
/// batches its entries need. Store reads are per tree node, and a node read
/// once is kept for the request, so the reads are bounded by the trie nodes
/// the looked-up names reach, whatever the batch size.
const ONLY_BATCH: usize = 64;

impl Only {
    async fn next<M: MetaStore>(
        &mut self,
        work: &mut TreeWork<'_, M>,
    ) -> Result<Option<Pair>, DeriveError> {
        loop {
            if let Some(pair) = self.ready.pop_front() {
                return Ok(Some(pair));
            }
            if self.names.is_empty() {
                return Ok(None);
            }
            let (a, b) = self.tops;
            let batch: Vec<Vec<u8>> = self
                .names
                .drain(..ONLY_BATCH.min(self.names.len()))
                .collect();
            let mut queries: Vec<(TreeId, &[u8])> = Vec::new();
            for name in &batch {
                queries.extend(a.map(|a| (a, name.as_slice())));
                queries.extend(b.map(|b| (b, name.as_slice())));
            }
            let mut found = work.lookup_many(&queries).await?.into_iter();
            for name in batch {
                let ea = if a.is_some() {
                    found.next().flatten()
                } else {
                    None
                };
                let eb = if b.is_some() {
                    found.next().flatten()
                } else {
                    None
                };
                if ea != eb {
                    self.ready.push_back((name, ea, eb));
                }
            }
        }
    }
}

impl Pairs {
    async fn new<M: MetaStore>(
        work: &mut TreeWork<'_, M>,
        a: Option<TreeId>,
        b: Option<TreeId>,
        from: Option<Vec<u8>>,
    ) -> Result<Self, DeriveError> {
        let mut side = async |id: Option<TreeId>| -> Result<VecDeque<TreeEntry>, DeriveError> {
            Ok(match id {
                Some(id) => work.get(id).await?.entries().iter().cloned().collect(),
                None => VecDeque::new(),
            })
        };
        let a = side(a).await?;
        let b = side(b).await?;
        Ok(Self {
            a,
            b,
            from,
            only: None,
        })
    }

    /// The pairs for exactly `names` (sorted), looked up lazily in both
    /// directories [`ONLY_BATCH`] names at a time, so a page reads
    /// the nodes for the names it reaches, not for every name it is given.
    fn only(a: Option<TreeId>, b: Option<TreeId>, names: Vec<Vec<u8>>) -> Self {
        Self {
            a: VecDeque::new(),
            b: VecDeque::new(),
            from: None,
            only: Some(Only {
                tops: (a, b),
                names: names.into(),
                ready: VecDeque::new(),
            }),
        }
    }

    /// Whether every name an entry stands for (for a shard, every name with
    /// its prefix) sorts before `from` without being a prefix of it.
    fn before_from(from: Option<&[u8]>, name: &[u8]) -> bool {
        from.is_some_and(|c| name < c && !c.starts_with(name))
    }

    async fn next<M: MetaStore>(
        &mut self,
        work: &mut TreeWork<'_, M>,
    ) -> Result<Option<Pair>, DeriveError> {
        use std::cmp::Ordering;
        if let Some(only) = &mut self.only {
            return only.next(work).await;
        }
        let from = self.from.as_deref();
        loop {
            for side in [&mut self.a, &mut self.b] {
                while side
                    .front()
                    .is_some_and(|e| Self::before_from(from, &e.name))
                {
                    side.pop_front();
                }
            }
            // Shards name disjoint prefix ranges. Merge the two ordered
            // frontiers, expanding only the first range that is not shared.
            // This also aligns nodes when splits or collapses change the
            // trie's shape.
            if let (Some(x), Some(y)) = (self.a.front(), self.b.front())
                && x == y
            {
                self.a.pop_front();
                self.b.pop_front();
                continue;
            }
            let ord = match (self.a.front(), self.b.front()) {
                (Some(x), Some(y)) => x.name.cmp(&y.name),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => return Ok(None),
            };
            // The side whose front comes first, and on an equal name the
            // right side too, has its front shard expanded before comparing.
            let expand = match ord {
                Ordering::Greater => [false, true],
                Ordering::Less => [true, false],
                Ordering::Equal => [true, true],
            };
            let mut expanded = false;
            for (side, go) in [&mut self.a, &mut self.b].into_iter().zip(expand) {
                if go
                    && let Some(TreeEntry {
                        target: EntryTarget::Shard(id),
                        ..
                    }) = side.front()
                {
                    let node = work.get(*id).await?;
                    side.pop_front();
                    for entry in node.entries().iter().rev() {
                        side.push_front(entry.clone());
                    }
                    expanded = true;
                    break;
                }
            }
            if expanded {
                continue;
            }
            let pair = match ord {
                Ordering::Less => self.a.pop_front().map(|x| (x.name, Some(x.target), None)),
                Ordering::Greater => self.b.pop_front().map(|y| (y.name, None, Some(y.target))),
                Ordering::Equal => match (self.a.pop_front(), self.b.pop_front()) {
                    (Some(x), Some(y)) if x.target != y.target => {
                        Some((x.name, Some(x.target), Some(y.target)))
                    }
                    _ => continue,
                },
            };
            return Ok(pair);
        }
    }
}

/// Appends a unified diff of two texts' lines.
fn unified(old: &[u8], new: &[u8], out: &mut Vec<u8>) -> (u64, u64) {
    let input = InternedInput::new(old, new);
    let mut diff = Diff::compute(Algorithm::Histogram, &input);
    diff.postprocess_lines(&input);
    let hunks: Vec<_> = diff.hunks().collect();
    let line = |side: &[imara_diff::Token], i: u32| -> &[u8] { input.interner[side[i as usize]] };
    let emit = |prefix: u8, text: &[u8], out: &mut Vec<u8>| {
        out.push(prefix);
        out.extend_from_slice(text);
        if !text.ends_with(b"\n") {
            out.extend_from_slice(b"\n\\ No newline at end of file\n");
        }
    };
    let nb = input.before.len() as u32;
    let na = input.after.len() as u32;
    let mut g = 0;
    while g < hunks.len() {
        let mut end = g;
        while end + 1 < hunks.len()
            && hunks[end + 1].before.start - hunks[end].before.end <= 2 * PATCH_CONTEXT
        {
            end += 1;
        }
        let bstart = hunks[g].before.start.saturating_sub(PATCH_CONTEXT);
        let astart = hunks[g].after.start - (hunks[g].before.start - bstart);
        let bend = (hunks[end].before.end + PATCH_CONTEXT).min(nb);
        let aend = hunks[end].after.end + (bend - hunks[end].before.end);
        let aend = aend.min(na);
        let range = |s: u32, e: u32| {
            if e == s {
                format!("{s},0")
            } else {
                format!("{},{}", s + 1, e - s)
            }
        };
        out.extend_from_slice(
            format!("@@ -{} +{} @@\n", range(bstart, bend), range(astart, aend)).as_bytes(),
        );
        let mut i = bstart;
        for h in &hunks[g..=end] {
            for k in i..h.before.start {
                emit(b' ', line(&input.before, k), out);
            }
            for k in h.before.clone() {
                emit(b'-', line(&input.before, k), out);
            }
            for k in h.after.clone() {
                emit(b'+', line(&input.after, k), out);
            }
            i = h.before.end;
        }
        for k in i..bend {
            emit(b' ', line(&input.before, k), out);
        }
        g = end + 1;
    }
    (
        u64::from(diff.count_additions()),
        u64::from(diff.count_removals()),
    )
}

/// The most files one diff page holds.
pub const MAX_DIFF_PAGE: usize = 1000;

/// The most content bytes one Stat or Patch page compares: the sizes of
/// both sides of each file whose blobs differ and may both be text (each at
/// most [`MAX_TEXT_BYTES`]). It is twice [`MAX_TEXT_BYTES`], so the
/// largest pair of texts fits a page alone. A page's blob reads are part of
/// it, and its patches are at most a constant factor larger, so a page's
/// memory is bounded by this and [`MAX_DIFF_PAGE`] whatever the file sizes.
pub const MAX_DIFF_PAGE_BYTES: u64 = 2 * MAX_TEXT_BYTES as u64;

/// The bytes a Stat or Patch page reads to compare one file's contents: both
/// sides' sizes when the blobs differ and both may be text, else nothing (a
/// side over [`MAX_TEXT_BYTES`] is binary, and binary files are compared by
/// id).
fn content_bytes(old: Option<FileState>, new: Option<FileState>) -> u64 {
    let sides = [old, new].into_iter().flatten();
    if old.map(|s| s.blob) == new.map(|s| s.blob)
        || sides.clone().any(|s| s.size > MAX_TEXT_BYTES as u64)
    {
        return 0;
    }
    sides.map(|s| s.size).sum()
}

/// The most blobs a diff page reads at once.
const BLOB_READS: usize = 16;

/// Where the next page of a diff starts: after the last path of the previous
/// page. It carries no state the server trusts: a client that sends any
/// other path gets exactly the diff entries after that path, so a forged
/// cursor can produce neither a wrong result nor extra work.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DiffCursor(Vec<u8>);

impl DiffCursor {
    /// The cursor's opaque bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Parses a cursor's bytes; they must be a valid path.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        RepoPath::new(bytes)
            .map(|p| Self(p.into_bytes()))
            .map_err(|_| OpError::new(Code::InvalidArgument, "malformed diff cursor"))
    }
}

/// What one diff page covers.
#[derive(Clone, Copy, Debug)]
pub struct DiffOptions<'a> {
    /// What to report per path.
    pub mode: DiffMode,
    /// When not empty, only paths equal to or under one of these.
    pub paths: &'a [RepoPath],
    /// Start after this cursor, from the previous page.
    pub after: Option<&'a DiffCursor>,
    /// The most files in the page: 1 to [`MAX_DIFF_PAGE`].
    pub limit: usize,
}

/// One page of a diff.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DiffPage {
    /// The page's files, in path byte order.
    pub files: Vec<FileDiff>,
    /// Where the next page starts, or `None` if this is the last page.
    pub next: Option<DiffCursor>,
}

/// One entry of a directory diff, keyed for path byte order: a file by its
/// path, a directory by its path and a trailing `/`, which is where its
/// contents sort among its siblings.
struct Item {
    key: Vec<u8>,
    path: Vec<u8>,
    file: Option<(Option<FileState>, Option<FileState>)>,
    dir: Option<(Option<TreeId>, Option<TreeId>)>,
}

// Items order by key alone; two items of one walk never share a key.
impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

/// Where a directory's subtree lies relative to the resume point `after`.
enum Resume {
    /// Every path in it comes after `after`.
    All,
    /// `after` lies inside it.
    Partly,
    /// No path in it comes after `after`.
    None,
}

fn resume(key: &[u8], after: Option<&[u8]>) -> Resume {
    match after {
        None => Resume::All,
        Some(after) if after < key => Resume::All,
        Some(after) if after.starts_with(key) => Resume::Partly,
        // `after` follows `key` and does not extend it, so it follows every
        // path that does.
        Some(_) => Resume::None,
    }
}

/// One directory being walked: its differing entries as items, in key
/// order. Pairs arrive in name order, and a directory's key (its path and a
/// `/`) can sort after later names (`a/` after `a-c`), so items are held in
/// a heap until no later pair can sort before them. The heap holds at most
/// the directories whose names are prefixes of one another.
struct Dir {
    path: Vec<u8>,
    pairs: Pairs,
    held: BinaryHeap<std::cmp::Reverse<Item>>,
    done: bool,
}

impl Dir {
    async fn new<M: MetaStore>(
        work: &mut TreeWork<'_, M>,
        path: Vec<u8>,
        a: Option<TreeId>,
        b: Option<TreeId>,
        after: Option<&[u8]>,
        paths: &[RepoPath],
    ) -> Result<Self, DeriveError> {
        let below = |p: &[u8]| -> Option<Vec<u8>> {
            if path.is_empty() {
                Some(p.to_vec())
            } else {
                p.strip_prefix(path.as_slice())?
                    .strip_prefix(b"/")
                    .map(<[u8]>::to_vec)
            }
        };
        // Where to resume within this directory: entries sorting before the
        // bound hold no path after `after`. When `after` lies below entry `c`,
        // the bound is `c/`, so siblings between `c` and `c/` (such as `c-1`)
        // are skipped too; when `after` is entry `c` itself, the bound is `c`.
        let from = after
            .and_then(below)
            .map(|rel| match rel.iter().position(|&b| b == b'/') {
                Some(i) => rel[..=i].to_vec(),
                None => rel,
            });
        // A filter below this directory, when no filter covers it whole,
        // names the only entries to look at.
        let covered = paths.is_empty() || paths.iter().any(|f| under(&path, f.as_bytes()));
        let pairs = if covered {
            Pairs::new(work, a, b, from).await?
        } else {
            let mut names: Vec<Vec<u8>> = paths
                .iter()
                .filter_map(|f| below(f.as_bytes()))
                .map(|rel| rel.split(|&b| b == b'/').next().unwrap_or(&rel).to_vec())
                .collect();
            names.sort();
            names.dedup();
            // Like the stream, skip the names wholly before the resume point.
            names.retain(|n| !Pairs::before_from(from.as_deref(), n));
            Pairs::only(a, b, names)
        };
        Ok(Self {
            pairs,
            path,
            held: BinaryHeap::new(),
            done: false,
        })
    }

    async fn next<M: MetaStore>(
        &mut self,
        work: &mut TreeWork<'_, M>,
        paths: &[RepoPath],
    ) -> Result<Option<Item>, DeriveError> {
        loop {
            if self.done {
                return Ok(self.held.pop().map(|r| r.0));
            }
            let Some((name, ea, eb)) = self.pairs.next(work).await? else {
                self.done = true;
                continue;
            };
            let p = join(&self.path, &name);
            let (fa, fb) = (file(ea), file(eb));
            if (fa.is_some() || fb.is_some()) && keep_file(&p, paths) {
                self.held.push(std::cmp::Reverse(Item {
                    key: p.clone(),
                    path: p.clone(),
                    file: Some((fa, fb)),
                    dir: None,
                }));
            }
            let (da, db) = (dir(ea), dir(eb));
            // Every directory pair reached here can hold filtered files: a
            // covered directory's entries are covered, and an uncovered one
            // yields only the names on the way to a filter (`Pairs::only`).
            if da.is_some() || db.is_some() {
                let mut key = p.clone();
                key.push(b'/');
                self.held.push(std::cmp::Reverse(Item {
                    key,
                    path: p.clone(),
                    file: None,
                    dir: Some((da, db)),
                }));
            }
            // Every later pair has a greater name, so every later item has a
            // key greater than `p`: what is held up to `p` is final.
            if self.held.peek().is_some_and(|r| r.0.key <= p) {
                return Ok(self.held.pop().map(|r| r.0));
            }
        }
    }
}

impl<M: MetaStore, B: BlobStore> Ops<M, B> {
    /// One page of the files that differ between `a` and `b`, in path byte
    /// order, as `options` selects. Equal subtrees are skipped by id, and
    /// the work is bounded by the page, not by the size of the whole diff.
    pub async fn diff(
        &self,
        repo: RepoId,
        a: ChangesetId,
        b: ChangesetId,
        options: &DiffOptions<'_>,
    ) -> Result<DiffPage, OpError> {
        let DiffOptions {
            mode,
            paths,
            after,
            limit,
        } = *options;
        if !(1..=MAX_DIFF_PAGE).contains(&limit) {
            return Err(OpError::new(
                Code::InvalidArgument,
                format!("a diff page holds 1 to {MAX_DIFF_PAGE} files"),
            ));
        }
        let after = after.map(|c| c.0.as_slice());
        let d = self.deriver();
        let ra = RootTree::decode(&d.get(repo, &RootTree, a).await?)?;
        let rb = RootTree::decode(&d.get(repo, &RootTree, b).await?)?;
        let mut cx = d.cx(repo);
        let mut files: Vec<FileDiff> = Vec::new();
        let mut content = 0u64;
        let mut next = None;
        // One frame per directory being walked.
        let mut stack: Vec<Dir> = Vec::new();
        if ra != rb {
            stack
                .push(Dir::new(&mut cx.trees, Vec::new(), Some(ra), Some(rb), after, paths).await?);
        }
        // Walk until the first file that does not fit the page, which proves
        // there is a next page; it starts after the page's last file.
        'walk: while let Some(frame) = stack.last_mut() {
            let Some(item) = frame.next(&mut cx.trees, paths).await? else {
                stack.pop();
                continue;
            };
            if let Some((old, new)) = item.file {
                if after.is_some_and(|after| item.path.as_slice() <= after) {
                    continue;
                }
                let cost = match mode {
                    DiffMode::Names => 0,
                    DiffMode::Stat | DiffMode::Patch => content_bytes(old, new),
                };
                if files.len() == limit || content + cost > MAX_DIFF_PAGE_BYTES {
                    // A file's cost is at most the page's, so the page holds
                    // at least one file.
                    let last = files.last().ok_or_else(|| {
                        OpError::new(Code::Internal, "a diff page stopped before its first file")
                    })?;
                    next = Some(DiffCursor(last.path.clone()));
                    break 'walk;
                }
                content += cost;
                files.push(FileDiff {
                    path: item.path,
                    old,
                    new,
                    ..FileDiff::default()
                });
            } else if let Some((da, db)) = item.dir
                && da != db
            {
                let within = match resume(&item.key, after) {
                    Resume::All => None,
                    Resume::Partly => after,
                    Resume::None => continue,
                };
                stack.push(Dir::new(&mut cx.trees, item.path, da, db, within, paths).await?);
            }
        }
        if mode != DiffMode::Names {
            self.compare_contents(repo, mode, &mut files).await?;
        }
        Ok(DiffPage { files, next })
    }

    /// Fills in each file's binary flag, line counts, and (in Patch mode)
    /// patch. Text classification comes from the blob records; blobs are
    /// read at most [`BLOB_READS`] at a time, and not at all when both sides
    /// have the same blob.
    async fn compare_contents(
        &self,
        repo: RepoId,
        mode: DiffMode,
        files: &mut [FileDiff],
    ) -> Result<(), OpError> {
        let d = self.deriver();
        let requests: Vec<_> = files
            .iter()
            .flat_map(|f| [f.old, f.new].into_iter().flatten())
            .map(|s| (s.blob, s.size))
            .collect();
        let text = classify_blobs(&**d.meta(), repo, &requests).await?;
        let mut reads = JoinSet::new();
        let mut index = 0;
        let mut done = 0;
        while done < files.len() {
            while index < files.len() && reads.len() < BLOB_READS {
                let f = &mut files[index];
                let i = index;
                index += 1;
                f.binary = [f.old, f.new].into_iter().flatten().any(|s| !text[&s.blob]);
                let same = f.old.map(|s| s.blob) == f.new.map(|s| s.blob);
                if f.binary || same {
                    done += 1;
                    continue;
                }
                let (old, new) = (f.old, f.new);
                let blobs = d.blobs().clone();
                reads.spawn(async move {
                    let read = async |s: Option<FileState>| match s {
                        Some(s) => read_blob(&*blobs, repo, s.blob, s.size).await,
                        None => Ok(Vec::new()),
                    };
                    Ok::<_, OpError>((i, read(old).await?, read(new).await?))
                });
            }
            let Some(joined) = reads.join_next().await else {
                continue;
            };
            let (i, old, new) = joined.map_err(|e| {
                OpError::new(Code::Internal, format!("a blob read task failed: {e}"))
            })??;
            let f = &mut files[i];
            if !is_text(&old) || !is_text(&new) {
                // The record said text; the verified bytes disagree.
                return Err(OpError::new(
                    Code::DataLoss,
                    format!(
                        "{}: a blob classified as text holds binary bytes",
                        String::from_utf8_lossy(&f.path)
                    ),
                ));
            }
            let mut patch = Vec::new();
            let (added, removed) = unified(&old, &new, &mut patch);
            f.added_lines = added;
            f.removed_lines = removed;
            if mode == DiffMode::Patch {
                f.patch = patch;
            }
            done += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patch(a: &str, b: &str) -> (String, (u64, u64)) {
        let mut out = Vec::new();
        let n = unified(a.as_bytes(), b.as_bytes(), &mut out);
        (String::from_utf8(out).unwrap(), n)
    }

    #[test]
    fn patches_have_context_and_counts() {
        let a = "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n";
        let b = "1\n2\n3\n4\nfive\n6\n7\n8\n9\n10\n";
        let (p, n) = patch(a, b);
        assert_eq!(n, (1, 1));
        assert_eq!(p, "@@ -2,7 +2,7 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n");
    }

    #[test]
    fn insertion_into_empty_and_missing_newline() {
        let (p, n) = patch("", "a\nb");
        assert_eq!(n, (2, 0));
        assert_eq!(p, "@@ -0,0 +1,2 @@\n+a\n+b\n\\ No newline at end of file\n");
        let (p, n) = patch("x\n", "x\n");
        assert_eq!((p.as_str(), n), ("", (0, 0)));
    }

    #[test]
    fn distant_changes_make_separate_hunks() {
        let a: String = (0..30).map(|i| format!("{i}\n")).collect();
        let b = a
            .replace("\n2\n", "\ntwo\n")
            .replace("\n25\n", "\ntwenty-five\n");
        let (p, n) = patch(&a, &b);
        assert_eq!(n, (2, 2));
        assert_eq!(p.matches("@@ -").count(), 2, "{p}");
    }

    #[test]
    fn hunks_merge_when_their_contexts_meet() {
        // Changes at lines 2 and 2 + gap + 1, with `gap` unchanged lines
        // between: contexts of three lines meet when gap <= 6.
        for (gap, hunks) in [(6, 1), (7, 2)] {
            let a: String = (0..30).map(|i| format!("{i}\n")).collect();
            let b = a
                .replace("\n2\n", "\nx\n")
                .replace(&format!("\n{}\n", 3 + gap), "\ny\n");
            let (p, n) = patch(&a, &b);
            assert_eq!(n, (2, 2));
            assert_eq!(p.matches("@@ -").count(), hunks, "gap {gap}: {p}");
        }
    }


    #[test]
    fn content_bytes_count_both_sides_of_files_that_may_be_text() {
        let s = |n: u8, size: u64| {
            Some(FileState {
                kind: FileKind::Regular,
                blob: BlobId::from_bytes([n; 32]),
                size,
            })
        };
        let max = MAX_TEXT_BYTES as u64;
        assert_eq!(content_bytes(s(1, 5), s(2, 7)), 12);
        assert_eq!(content_bytes(None, s(2, 7)), 7);
        assert_eq!(content_bytes(s(1, 5), None), 5);
        assert_eq!(content_bytes(s(1, max), s(2, max)), MAX_DIFF_PAGE_BYTES);
        // Equal blobs (a kind change) and sides too big for text are not read.
        assert_eq!(content_bytes(s(1, 5), s(1, 5)), 0);
        assert_eq!(content_bytes(s(1, max + 1), s(2, 7)), 0);
        assert_eq!(content_bytes(s(1, 5), s(2, max + 1)), 0);
    }
    #[test]
    fn filters_select_the_files_under_them() {
        let f = [RepoPath::new(&b"a/b"[..]).unwrap()];
        assert!(keep_file(b"a/b", &f));
        assert!(keep_file(b"a/b/c", &f));
        assert!(!keep_file(b"a/bc", &f));
    }
}
