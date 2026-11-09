//! Diff between two changesets' trees: the changed paths, their line
//! counts, or a unified patch, one page at a time in path byte order. Equal
//! subtrees are skipped by id, so the walk skips matching directory and
//! shard ids without loading their entries.

use std::collections::VecDeque;

use derived::{DeriveError, RootTree, TreeWork};
use imara_diff::{Algorithm, Diff, InternedInput};
use scion_core::{BlobId, ChangesetId, EntryTarget, FileKind, RepoPath, TreeId};
use store::{BlobStore, MetaStore, RepoId};

use tokio::task::JoinSet;

use crate::blob::{classify_blobs, is_text, read_blob};
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

/// Whether the directory at `path` can hold a file that passes the filters.
fn enter_dir(path: &[u8], filters: &[RepoPath]) -> bool {
    path.is_empty()
        || filters.is_empty()
        || filters
            .iter()
            .any(|f| under(path, f.as_bytes()) || under(f.as_bytes(), path))
}

fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
    let mut p = dir.to_vec();
    if !p.is_empty() {
        p.push(b'/');
    }
    p.extend_from_slice(name);
    p
}

/// The entries that differ between two directories, by name.
async fn pairs<M: MetaStore>(
    work: &mut TreeWork<'_, M>,
    a: Option<TreeId>,
    b: Option<TreeId>,
) -> Result<Vec<(Vec<u8>, Option<EntryTarget>, Option<EntryTarget>)>, DeriveError> {
    let ea = match a {
        Some(id) => work.get(id).await?.entries().to_vec(),
        None => Vec::new(),
    };
    let eb = match b {
        Some(id) => work.get(id).await?.entries().to_vec(),
        None => Vec::new(),
    };
    let mut out = Vec::new();
    let (mut ea, mut eb) = (VecDeque::from(ea), VecDeque::from(eb));
    while !ea.is_empty() || !eb.is_empty() {
        // Shards name disjoint prefix ranges. Merge the two ordered
        // frontiers, expanding only the first range that is not shared.
        // This also aligns nodes when splits/collapses change trie shape.
        if let (Some(x), Some(y)) = (ea.front(), eb.front())
            && x == y
        {
            ea.pop_front();
            eb.pop_front();
            continue;
        }
        let ord = match (ea.front(), eb.front()) {
            (Some(x), Some(y)) => x.name.cmp(&y.name),
            (Some(_), None) => std::cmp::Ordering::Less,
            _ => std::cmp::Ordering::Greater,
        };
        let first = if ord == std::cmp::Ordering::Greater {
            &mut eb
        } else {
            &mut ea
        };
        if let Some(scion_core::TreeEntry {
            target: EntryTarget::Shard(id),
            ..
        }) = first.front()
        {
            let node = work.get(*id).await?;
            first.pop_front();
            for entry in node.entries().iter().rev() {
                first.push_front(entry.clone());
            }
            continue;
        }
        // An equal-name shard on the other side must be expanded too.
        if ord == std::cmp::Ordering::Equal
            && let Some(scion_core::TreeEntry {
                target: EntryTarget::Shard(id),
                ..
            }) = eb.front()
        {
            let node = work.get(*id).await?;
            eb.pop_front();
            for entry in node.entries().iter().rev() {
                eb.push_front(entry.clone());
            }
            continue;
        }
        match ord {
            std::cmp::Ordering::Less => {
                let x = ea.pop_front().expect("left entry");
                out.push((x.name, Some(x.target), None));
            }
            std::cmp::Ordering::Greater => {
                let y = eb.pop_front().expect("right entry");
                out.push((y.name, None, Some(y.target)));
            }
            std::cmp::Ordering::Equal => {
                let x = ea.pop_front().expect("left entry");
                let y = eb.pop_front().expect("right entry");
                if x.target != y.target {
                    out.push((x.name, Some(x.target), Some(y.target)));
                }
            }
        }
    }
    Ok(out)
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

/// The items of the directories `a` and `b` at `path`, sorted by key, with
/// the subdirectories a walk will enter prefetched in one batch.
async fn items<M: MetaStore>(
    work: &mut TreeWork<'_, M>,
    path: &[u8],
    a: Option<TreeId>,
    b: Option<TreeId>,
    paths: &[RepoPath],
    after: Option<&[u8]>,
) -> Result<Vec<Item>, DeriveError> {
    let mut out = Vec::new();
    for (name, ea, eb) in pairs(work, a, b).await? {
        let p = join(path, &name);
        let (fa, fb) = (file(ea), file(eb));
        if (fa.is_some() || fb.is_some()) && keep_file(&p, paths) {
            out.push(Item {
                key: p.clone(),
                path: p.clone(),
                file: Some((fa, fb)),
                dir: None,
            });
        }
        let (da, db) = (dir(ea), dir(eb));
        if (da.is_some() || db.is_some()) && enter_dir(&p, paths) {
            let mut key = p.clone();
            key.push(b'/');
            out.push(Item {
                key,
                path: p,
                file: None,
                dir: Some((da, db)),
            });
        }
    }
    out.sort_by(|x, y| x.key.cmp(&y.key));
    let entered = out
        .iter()
        .filter_map(|i| match (&i.dir, resume(&i.key, after)) {
            (Some((da, db)), Resume::All | Resume::Partly) => Some([*da, *db]),
            _ => None,
        });
    work.prefetch(entered.flatten().flatten()).await?;
    Ok(out)
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
        let mut files = Vec::new();
        // One frame per directory being walked: its items, next first.
        let mut stack: Vec<std::vec::IntoIter<Item>> = Vec::new();
        if ra != rb {
            stack.push(
                items(&mut cx.trees, b"", Some(ra), Some(rb), paths, after)
                    .await?
                    .into_iter(),
            );
        }
        // Walk until one file past the page, which proves there is a next
        // page.
        'walk: while let Some(frame) = stack.last_mut() {
            let Some(item) = frame.next() else {
                stack.pop();
                continue;
            };
            if let Some((old, new)) = item.file {
                if after.is_some_and(|after| item.path.as_slice() <= after) {
                    continue;
                }
                files.push(FileDiff {
                    path: item.path,
                    old,
                    new,
                    ..FileDiff::default()
                });
                if files.len() > limit {
                    break 'walk;
                }
            } else if let Some((da, db)) = item.dir
                && da != db
            {
                let within = match resume(&item.key, after) {
                    Resume::All => None,
                    Resume::Partly => after,
                    Resume::None => continue,
                };
                let children = items(&mut cx.trees, &item.path, da, db, paths, within).await?;
                stack.push(children.into_iter());
            }
        }
        let next = if files.len() > limit {
            files.truncate(limit);
            files.last().map(|f| DiffCursor(f.path.clone()))
        } else {
            None
        };
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
                let (old, new) = (f.old.map(|s| s.blob), f.new.map(|s| s.blob));
                let blobs = d.blobs().clone();
                reads.spawn(async move {
                    let read = async |id: Option<BlobId>| match id {
                        Some(id) => read_blob(&*blobs, repo, id).await,
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
    fn filters_select_files_and_the_dirs_leading_to_them() {
        let f = [RepoPath::new(&b"a/b"[..]).unwrap()];
        assert!(keep_file(b"a/b", &f));
        assert!(keep_file(b"a/b/c", &f));
        assert!(!keep_file(b"a/bc", &f));
        assert!(enter_dir(b"a", &f));
        assert!(enter_dir(b"a/b/c", &f));
        assert!(!enter_dir(b"x", &f));
        assert!(!enter_dir(b"a/c", &f));
    }
}
