//! Diff between two changesets' trees: the changed paths, their line
//! counts, or a unified patch. Equal subtrees are skipped by id, so the walk
//! costs O(changed paths × path depth) node reads plus the size of each
//! changed directory.

use derived::{DeriveError, RootTree, TreeWork};
use imara_diff::{Algorithm, Diff, InternedInput};
use scion_core::{BlobId, ChangesetId, EntryTarget, FileKind, RepoPath, TreeId};
use store::{BlobStore, MetaStore, RepoId};

use crate::blob::{is_text, read_blob};
use crate::{OpError, Ops};

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
        Some(id) => work.flatten(id).await?,
        None => Vec::new(),
    };
    let eb = match b {
        Some(id) => work.flatten(id).await?,
        None => Vec::new(),
    };
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < ea.len() || j < eb.len() {
        let ord = match (ea.get(i), eb.get(j)) {
            (Some(x), Some(y)) => x.name.cmp(&y.name),
            (Some(_), None) => std::cmp::Ordering::Less,
            _ => std::cmp::Ordering::Greater,
        };
        match ord {
            std::cmp::Ordering::Less => {
                out.push((ea[i].name.clone(), Some(ea[i].target), None));
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push((eb[j].name.clone(), None, Some(eb[j].target)));
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                if ea[i].target != eb[j].target {
                    out.push((ea[i].name.clone(), Some(ea[i].target), Some(eb[j].target)));
                }
                i += 1;
                j += 1;
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

impl<M: MetaStore, B: BlobStore> Ops<M, B> {
    /// The files that differ between `a` and `b`, in path byte order,
    /// limited to paths equal to or under one of `paths` when it is not
    /// empty.
    pub async fn diff(
        &self,
        repo: RepoId,
        a: ChangesetId,
        b: ChangesetId,
        mode: DiffMode,
        paths: &[RepoPath],
    ) -> Result<Vec<FileDiff>, OpError> {
        let d = self.deriver();
        let ra = RootTree::decode(&d.get(repo, &RootTree, a).await?)?;
        let rb = RootTree::decode(&d.get(repo, &RootTree, b).await?)?;
        let mut cx = d.cx(repo);
        let mut out = Vec::new();
        let mut stack = vec![(Vec::new(), Some(ra), Some(rb))];
        while let Some((path, ta, tb)) = stack.pop() {
            if ta == tb || !enter_dir(&path, paths) {
                continue;
            }
            for (name, ea, eb) in pairs(&mut cx.trees, ta, tb).await? {
                let p = join(&path, &name);
                let (fa, fb) = (file(ea), file(eb));
                if (fa.is_some() || fb.is_some()) && keep_file(&p, paths) {
                    out.push(FileDiff {
                        path: p.clone(),
                        old: fa,
                        new: fb,
                        ..FileDiff::default()
                    });
                }
                let (da, db) = (dir(ea), dir(eb));
                if da.is_some() || db.is_some() {
                    stack.push((p, da, db));
                }
            }
        }
        out.sort_by(|x, y| x.path.cmp(&y.path));
        if mode != DiffMode::Names {
            let blobs = d.blobs();
            for f in &mut out {
                let old = match f.old {
                    Some(s) => read_blob(&**blobs, repo, s.blob).await?,
                    None => Vec::new(),
                };
                let new = match f.new {
                    Some(s) => read_blob(&**blobs, repo, s.blob).await?,
                    None => Vec::new(),
                };
                if !is_text(&old) || !is_text(&new) {
                    f.binary = true;
                    continue;
                }
                let mut patch = Vec::new();
                let (added, removed) = unified(&old, &new, &mut patch);
                f.added_lines = added;
                f.removed_lines = removed;
                if mode == DiffMode::Patch {
                    f.patch = patch;
                }
            }
        }
        Ok(out)
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
