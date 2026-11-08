<project>
Scion is a centralized version-control system written in Rust, for organizations that run their own Scion servers. It fully replaces git for one huge monorepo whose users are mostly agents and CI jobs. A web platform that replaces GitHub will later be built on its public API.

The server does all history computation and all writes. Clients are thin, always online, and fetch only the objects they read.

Scale the design must hold:
- More than 30,000 lands per day.
- About 25 million CI checkouts per day.
- About 100 GB of history.
- A compressed working copy of more than 25 GB.
- Millions of files.
</project>

<principles>
1. **Clients are always online.** No client, server, or cache holds a full copy of the repo.
2. **The server does everything.**
   - It answers history, blame, diff, ancestry, and merge-base queries.
   - It performs commit, rebase, merge, and land.
   - The client lays out files, watches the filesystem, and keeps a bounded object cache. It never computes history.
3. **Everything the client can do is a public API.** CI and tools can work with no checkout.
4. **Every component scales horizontally.** There are no packfiles, no repository maintenance, no GC, no repacking, and no compaction.
5. **Commits are cheap, incremental, and lock-free.**
   - A commit costs O(changed files × path depth), never O(repo). (INV-1)
   - A commit takes no locks, needs no cross-server coordination, and touches no refs. (INV-2)
6. **Refs are rare.** Only `main`, `release/*`, and `tags/*` exist.
7. **The working copy is optional, and sparse or virtual by design.** A client pays only for what it reads.
</principles>

<working_rules>
- CLAUDE.md is the only design document. Never create other design, notes, or progress files.
- Work one milestone at a time, in order. A milestone is done only when its acceptance tests pass on Postgres plus an object store, `cargo xtask ci-full` passes, and you have shown the output.
- Never weaken, skip, ignore, or delete a test to make it pass. Never add `#[allow]` without a one-line justification in the code.
- When this file leaves a choice open, choose the option that satisfies every INV and the forbidden list, rejects or fails closed when in doubt, and is smallest to implement. Keep the choice in one place in the code and list it in the session report. Never edit this file to record it.
- When this file makes an acceptance criterion impossible, complete the rest of the milestone, leave that test in place and failing (never ignored or deleted), and report the conflict.
- Never record progress, decisions, or session notes in this file; the tests are the record of progress.
- All code is Rust, including tests, test tools, checkers, generators, and project tooling. Project tooling lives in the internal `xtask` crate, run with `cargo xtask`. No Python and no shell scripts. Apart from checkout, Swatinem/rust-cache, and installing prebuilt tools, CI steps call cargo commands only.
- A crate not in the dependency list may be added when the current milestone needs it, no listed crate covers the need, it is actively maintained, and it passes the license allowlist and the MSRV. Add it to the Dependencies line in the same change. That line is the only part of this file an implementation session may edit.
- Acceptance tests run against the Postgres and object store started by the dev environment in the workspace root.
- Checks run in two tiers. `cargo xtask ci` is the fast tier: `cargo fmt --all --check`, `cargo build --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`, tests, doc tests, docs, `cargo deny check`, `cargo semver-checks` for each public crate, `buf breaking` on the `scion.v1` proto, and the generated-code check. It runs before any change is complete and on every push. `cargo xtask ci-full` adds the slow test group, fuzz targets at `FUZZ_BUDGET`, the MSRV build, and the macOS and Windows tests. It runs before a milestone is complete, on every merge, and nightly. `cargo semver-checks` and `buf breaking` compare against the last published release and are skipped for a crate or proto that has never been released.
- Tests run with cargo-nextest, plus `cargo test --doc` for doc tests. A test that takes longer than `SLOW_TEST_SECONDS` (30 s) belongs in the slow group. Tests are never retried automatically; a flaky test is a defect.
- Each test that uses Postgres gets its own database created from a template, and each test that uses the object store gets its own key prefix, so tests run in parallel.
- rust-toolchain.toml pins an exact stable release for development; a separate build checks the MSRV. Fuzz targets alone use a pinned, dated nightly.
- CI caches the cargo registry and target directory with Swatinem/rust-cache, pinned to a full commit SHA. `cargo xtask` commands use sccache as the compiler wrapper when it is installed and build normally when it is not. Never clear a cache or the target directory to get past a build failure; fix the cause.
- CI installs tools from prebuilt binaries, never by compiling them with `cargo install`.
- scion-proto commits its generated code and has no build script. `cargo xtask proto` regenerates it with protox, and the fast tier fails if the committed code is stale.
</working_rules>

<architecture>
```
all clients ── control plane (refs, history, diff, commit, land) ─────────────▶ scion-server ─▶ metadata DB: changesets, trees, refs, derived data
dev machines ── object plane (trees, blobs) ─▶ scion-cache ── misses ─▶  (stateless fleet)        └▶ blob storage: file contents
CI / cluster jobs ─▶ scion-daemon (per node) ── misses ─▶ (regional, RAM only)
                          ▲ seeded from snapshot
```
- **scion-server**
  - A stateless fleet; any instance serves any request.
  - Holds a bounded RAM cache of frames, changesets, trees, and derived values. It is a large cache that also computes derived data.
  - A changeset is readable on every server as soon as its create call returns. On a replica miss, the server reads from the leader. (INV-3)
  - Ref reads are always strong. (INV-4)
- **scion-cache**
  - Regional, RAM-only, and stateless. A restart starts cold. (INV-5)
  - Streams stored frames straight from memory.
- **scion-daemon**
  - One per node, serving every job on that node over a Unix socket.
  - Keeps a disk store bounded by size, with LRU eviction.
  - Talks only to its regional caches.
- **scn** is the CLI. In the fleet it gets objects only from the node daemon. On dev machines it talks to scion-cache directly and keeps a bounded per-user store in the same layout as the daemon.
- **One object RPC.** Daemon, cache, and server answer the identical object RPC, so clients never know the topology.
- **Strict tiering.** The object-plane call order is fixed: client → daemon → cache → server, or dev client → cache → server.
  - A tier never skips or bypasses its upstream. If the cache is down, daemons return `UNAVAILABLE` and never call the server. (INV-6)
  - Each tier rejects callers from the wrong tier, identified by the tier in their mTLS identity or token. (INV-7)
- **Single-node mode:** one process runs scion-server and scion-cache against Postgres and a local-filesystem object store, on at least 8 vCPU, 32 GB RAM, and 500 GB NVMe. That process is both the cache tier and the server tier, which relaxes only INV-7: it accepts daemon and client callers. Every other rule applies unchanged. The cluster topology is the production design.
</architecture>

<data_model>
- **Ids**
  - An id is BLAKE3 `derive_key` with a per-kind context (`scion v1 blob`, `scion v1 chunk`, `scion v1 tree`, `scion v1 changeset`), computed over the object's canonical bytes. (INV-8)
  - A blob id is computed over the raw, uncompressed bytes.
  - Each kind has its own type: `BlobId`, `ChunkId`, `TreeId`, `ChangesetId`.
  - Objects are immutable. Any tier may cache any object forever. (INV-9)
- **Encoding**
  - Changesets and trees use a hand-written, versioned, canonical binary encoding: sorted keys, minimal varints, no trailing bytes.
  - Primitives: `uvarint` is minimal unsigned LEB128; `svarint` is zigzag then `uvarint`; `bytes` is a `uvarint` length then raw bytes; `opt<T>` is `0x00`, or `0x01` then `T`; `timestamp` is `svarint` Unix seconds then `svarint` UTC offset in seconds east, with |offset| < 86,400.
  - The decoder rejects anything non-canonical. (INV-10)
  - Golden vectors lock the format. Any change needs a version bump.
- **Changeset (the commit)**
  - Fields: `parents` (ordered; first parent first), `author`, `author_date` (with timezone), `committer?`, `committer_date?`, `message`, `signature?`, `git_headers?`, `file_changes`.
  - `git_headers` is present only on imported changesets: the git commit object's bytes after its `tree` and `parent` lines, verbatim.
  - `file_changes` is a sorted map `path → Deleted | (Regular|Executable|Symlink, BlobId, size)`, relative to the **first parent only**, including for merges.
  - A changeset has no tree pointer.
  - Changeset bytes, in order: `u8` version `0x01`; `uvarint` parent count, then each parent's 32-byte id; `author` as `bytes` (`Name <email>` verbatim; 1–1,024 bytes except on imported changesets); `author_date` as `timestamp`; `committer` as `opt<bytes>`; `committer_date` as `opt<timestamp>`, present exactly when `committer` is; `message` as `bytes`; `signature` as `opt<bytes>`; `git_headers` as `opt<bytes>`; `uvarint` change count, then each change in path byte order as `bytes` path, `u8` kind (0 Deleted, 1 Regular, 2 Executable, 3 Symlink), and, unless Deleted, the 32-byte `BlobId` and `uvarint` size.
  - No-op entries are invalid. (INV-11)
  - No path in `file_changes` may be a prefix of another, except a `Deleted` file whose path becomes a directory. (INV-12)
  - When the server creates or rewrites a changeset, it sets `committer` to itself and `committer_date` to now, and drops `signature` and `git_headers`.
  - Copies and renames are not recorded; they are derived.
- **Tree**
  - Trees are derived by the server and stored in the metadata DB.
  - A tree is a sorted list of `(name, kind Tree|Shard|Regular|Executable|Symlink, id, size, sha256)` entries. The size and SHA-256 let stat and digest queries work without fetching the blob.
  - A tree node with more than `MAX_TREE_ENTRIES` (4,096) names is split: with P the longest common prefix of its names, a name equal to P stays an entry, and the rest are grouped by their first byte after P into one `Shard` entry (name P + that byte → `TreeId`) per group; each shard node holds full names and is split by the same rule.
  - Tree node bytes: `u8` version `0x01`; `uvarint` entry count, then each entry in name byte order as `bytes` name, `u8` kind (0 Tree, 1 Shard, 2 Regular, 3 Executable, 4 Symlink), and the 32-byte id, followed for Regular, Executable, and Symlink entries by `uvarint` size and the 32-byte SHA-256.
  - The trie depends only on the directory's entry set. (INV-13) Each trie level counts toward path depth. `Tree` and `Shard` entries omit `size` and `sha256`.
  - Only the root tree may be empty.
- **Blob**
  - Raw file bytes, stored verbatim with no end-of-line or encoding conversion.
  - A symlink is a blob holding the link target.
  - A blob of at most `MAX_TEXT_BYTES` (16 MiB) with no NUL byte in its first 8 KiB is text; every other blob is binary.
  - Stored zstd-compressed and deduplicated by id within a repo.
  - A blob larger than `CHUNK_BYTES` (4 MiB) is stored as consecutive `CHUNK_BYTES` chunks, each its own object and frame; the blob's frame is a chunk-list frame with codec raw and raw length equal to the blob's, whose payload is a `uvarint` count then the `ChunkId`s in order. The `BlobId` still hashes the full raw bytes.
  - Each blob records its SHA-256 and, when imported, its git SHA-1.
- **Frame**
  - The single byte format for the wire, cache RAM, daemon and client disk, and blob storage.
  - Layout: header, payload, then the 32-byte BLAKE3 digest of every preceding byte. Header bytes: magic `SCF1`; `u8` kind (1 blob, 2 chunk, 3 tree, 4 chunk list); the 32-byte id; `u8` codec (0 raw, 1 zstd); `uvarint` raw length; `uvarint` payload length.
  - Only scion-server creates frames. Every frame, tree frames included, is created once and stored in blob storage put-if-absent; servers serve stored frame bytes and never rebuild a frame for an existing id, so every frame for a given kind and id is byte-identical at every tier and over time. (INV-44)
  - Cache and daemon store and forward the exact bytes. They check the digest incrementally while forwarding, and never decompress, re-encode, or hold a whole object in memory. (INV-14)
  - The raw-byte id is verified on upload (by the server) and on materialization (by the client). (INV-15)
- **Refs**
  - Each ref is one row, updated by single-row compare-and-swap. (INV-16)
  - `tags/*` refs can be created but never moved. (INV-17)
  - Every CAS writes a `ref_log(repo, ref, seq, old, new, actor, time, reason land|backout|manual)` row in the same transaction. `seq` is gap-free. (INV-18)
- **Paths**
  - Paths are byte strings. No `.` or `..` components, no NUL bytes, no empty components.
  - Limits, except on imported changesets: path ≤ 4,096 bytes, path component ≤ 255 bytes, message ≤ 1 MiB, parents ≤ 64, signature ≤ 64 KiB. Decoders reject any `bytes` field longer than `MAX_FIELD_BYTES` (64 MiB) before allocating.
  - CreateChangeset rejects a changeset that adds a path whose name conflicts with another name in the same directory of its resulting tree. Two names conflict when they are equal after NFC normalization followed by Unicode simple case folding, both per Unicode 18.0 as implemented by `icu_normalizer` and `icu_casemap` pinned at `=2.4`; names that are not valid UTF-8 compare by bytes. A test fails if the Unicode version these crates implement differs from Unicode 18.0. This is a repo setting, on by default. Imported changesets are exempt. (INV-19)
</data_model>

<derived_data>
Everything that is not a changeset is derived data, and all of it goes through **one generic mechanism**.

- **How a value is computed:** the value for a changeset is a pure function of the changeset and the same type's values for its parents.
- **Storage:** values are keyed by `(type, version, changeset)` and are deterministic. Writes are insert-if-absent. Concurrent derivations of the same key are coalesced on each server. (INV-20)
- **Mode:** each type is either eager (derived inside create) or deferred.
- **Visibility:** clients never see derived formats.
- **New versions:** a new version gets a new key space and is backfilled before reads switch over.
- **Background derivation:** a background deriver derives every type for every new `main`, `release/*`, or `tags/*` tip. A request that finds an underived ancestor derives the parents first, in bounded batches.

| Type | Mode | Purpose |
|---|---|---|
| RootTree | eager | Rewrites only the trees on each changed path's spine. |
| CommitGraph | eager | Generation number and two skew-binary skip pointers: one over the first-parent chain, one over the skip tree (a commit's skip-tree parent is the closest common ancestor of its parents). `is_ancestor`, `merge_base`, nth ancestor, and ranges `ancestors(b) − ancestors(a)` cost O(log n) jumps, plus output size. Never a full-graph walk. (INV-21) |
| RestrictedRoots | eager | Keyed additionally by ACL config version. Records, per changeset, the tree id of each restricted directory root: a directory whose path prefix maps to a non-default ACL domain. |
| HistoryManifest | deferred | Per-path history for files and directories, including deleted paths, following merges. |
| Blame | deferred | Line blame, computed from the parents' blame plus a line diff. Binary files and files over `MAX_TEXT_BYTES` return `Rejected(Binary\|TooBig)`. |
| CopyIndex | deferred | Detects copies and renames from content. Powers `log --follow` and blame across renames. A file added in a changeset copies a source if their blobs are equal, or if both are text and line similarity (2 × common lines ÷ total lines, by line diff) is at least `COPY_SIMILARITY` (50%). Sources are only files deleted or modified in the same changeset; at most `COPY_MAX_CANDIDATES` (1,000) similarity comparisons per changeset, with pairs ordered by size difference, then added path, then source path, in byte order. Each added file's source is the qualifying source with the highest similarity, exact matches first, ties by source path byte order. Detection is deterministic and never exceeds `COPY_MAX_CANDIDATES` comparisons. (INV-42) |
| PathSearch | deferred | `PathSearch(rev, prefix, pattern)` returns the file paths at `rev` under `prefix` whose basename matches `pattern` (`*` matches any bytes, `?` one byte, case-sensitive), in path byte order, paginated. |

All seven types are required.
</derived_data>

<server>
The public API is gRPC package `scion.v1`. Changes to it are additive only: an RPC or field may be deprecated but is never removed or repurposed within `scion.v1`, and removal requires `scion.v2`. (INV-38) The CLI and the platform use only this API. Every RPC carries `repo_id`. Every list or stream response paginates with opaque tokens.
- `CreateChangeset`, `UpdateRef`, `Land`, `CommitAndLand`, `Rebase`, `Merge`, `Backout`, `Graft`, `Redact`, `CreateRepo`, `SetAclConfig`, `GrantRole`, and `RevokeRole` take a client-supplied idempotency key scoped to (repo, principal); a retry with the same key returns the original result without repeating the operation, reusing a key with different arguments fails, and keys are retained for `IDEMPOTENCY_TTL` (7 days). (INV-39)
- Every error carries a `google.rpc.ErrorInfo` status detail with domain `scion.v1`, a reason code, and metadata `retryable` = `true` or `false`. Clients and agents branch on the code, never on the message text. (INV-40) Codes are added, never changed: `INVALID_ARGUMENT`, `NOT_CANONICAL`, `LIMIT_EXCEEDED`, `NO_OP_CHANGE`, `PATH_CONFLICT`, `CASE_CONFLICT`, `NOT_FOUND`, `AMBIGUOUS_REV`, `REF_CAS_MISMATCH`, `TAG_IMMUTABLE`, `MERGE_CONFLICT`, `LAND_MERGE_LIMIT`, `LAND_RETRIES_EXHAUSTED`, `IDEMPOTENCY_KEY_REUSED`, `ACL_VERSION_MISMATCH`, `ACL_PENDING`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `WRONG_TIER`, `REDACTED`, `DATA_LOSS`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE`; only the last two, `LAND_RETRIES_EXHAUSTED`, and `ACL_PENDING` are retryable.
- There are no hand-written SDKs in other languages; other languages use clients generated from the `scion.v1` proto.

| Area | RPCs |
|---|---|
| Refs | `ResolveRev` (tries, in order: a ref name, a 64-hex changeset id, a 40-hex git sha1, then a hex prefix of at least 8 digits matching exactly one changeset id; otherwise `AMBIGUOUS_REV` or `NOT_FOUND`) · `ListRefs` · `UpdateRef` (CAS) · `WatchRefs(ref prefix, since_seq per ref)` (streams ref-log entries; `seq` is per ref) |
| Read | `GetChangesets` · `GetMutations(changeset)` (its predecessors and successors) · `ResolvePaths(rev, paths) → (kind, id, size, sha256, git_sha1?)` · `ListTree` · `ReadFile` (low-volume tools only; bulk content goes through the object plane) |
| History | `Diff(a, b, mode names\|stat\|patch, paths, merge_base: bool)` · `MergeBase` (returns every merge base, by generation descending, then id ascending) · `IsAncestor` · `Log(rev, path?, first_parent, range)` · `Blame` · `NthAncestor` |
| Write | `MissingBlobs` · `UploadBlobs` · `CreateChangeset` (blobs inline or pre-uploaded) · `Rebase` · `Merge` · `Backout` · `Graft` · `Land` · `CommitAndLand` (atomic, for bots) |
| Admin | `Redact` · `CreateRepo` · `SetAclConfig` · `GetAclConfig` · `GrantRole` · `RevokeRole` · `ListRoleBindings` |

**CreateChangeset:**
1. Validate the input.
2. Check that the parents and blobs exist.
3. Derive the trees.
4. Write the changeset and its eager derived values in one transaction.

There are no locks and no refs.

**Write order:** blobs → trees → changeset → ref. A crash at any point leaves only unreferenced objects, never a broken reference. (INV-22)

**Merge (3-way, per file):**
- The line diff is histogram diff over `\n`-terminated lines; Diff, Merge, Land, and Blame all use it.
- Text files get a diff3 line merge over the line diffs base→ours and base→theirs; changed regions that overlap or touch conflict unless both sides made the identical change.
- With several merge bases, the base is a virtual base built by merging them pairwise in `MergeBase` order with this same merge; a conflict while building it fails the operation.
- These are conflicts:
  - changed regions that overlap or touch, unless identical
  - kind or mode mismatch
  - delete vs. modify
  - file vs. directory
  - differing binaries
- A conflict fails the operation and returns the conflicting paths. Conflict markers are never committed. (INV-23)

**Land(ref, stack):**
1. If the stack's base is the tip, fast-forward and keep the ids.
2. Otherwise intersect the stack's changed paths with the paths changed between its base and the tip. With no overlap, rebase onto the tip without reading file contents.
3. Overlapping paths get the 3-way merge above only if there are at most `MAX_LAND_MERGE_PATHS` (100) and each is text; otherwise land fails with `LAND_MERGE_LIMIT`.
4. CAS the ref.
5. If the CAS fails, redo steps 2–3 against only the new tip commits and retry with jittered backoff, for at most `MAX_LAND_ATTEMPTS` (16) attempts, then fail with `LAND_RETRIES_EXHAUSTED`.

**Backout(cs, ref)** creates on the ref's tip the 3-way merge with base cs, ours the tip, and theirs cs's first parent, restricted to cs's changed paths, with author the caller and message `Backout <cs hex id>`, then lands it with reason `backout`. **Graft(cs, ref)** creates on the ref's tip the 3-way merge with base cs's first parent, ours the tip, and theirs cs, restricted to cs's changed paths, with cs's author and message, then lands it with reason `land`.

A server may batch concurrent lands on the same ref into one CAS. There is no merge queue and no global lock. (INV-24)

**Mutation records:** amend, rebase, land, backout, and graft each write a `predecessor → successor (op, merged_paths)` record; `merged_paths` lists the paths land resolved by 3-way merge and is also returned by `Land`. A change keeps its identity through this lineage, not through its changeset id. (INV-25)

**Redaction:**
- Changesets and objects are never deleted. Redaction is the only removal.
- Servers refuse a redacted id before `Redact` returns. (INV-26)
- **Purge stream:** servers push purged ids to caches, and caches relay them to daemons. Every cache and daemon stops serving a purged id within `PURGE_LAG_MAX` (5 s) and drops it from its store. (INV-27) Clients drop purged ids from their local store on their next server contact.
- Redacted ids are pushed on the purge stream.
- Reads of a redacted object return `REDACTED`.
</server>

<fetch_protocol>
There is one RPC: `GetObjects([(kind, id, offset)]) → stream of chunks`.
- Chunks are bounded in size, interleaved across objects, and carry a per-object status (`OK`, `NOT_FOUND`, `DENIED`, `REDACTED`, or `DATA_LOSS`) and a `cacheable` flag. Requests name kind 1 (blob), 2 (chunk), or 3 (tree); a blob request may return a chunk-list frame.
- Chunks are provisional until the object's last chunk arrives with `OK`.
- To resume, re-request an object from a byte offset.

**There is no "haves" negotiation.** The client names only the objects it wants. There are no packfiles and no delta compression.

**Fetch algorithm:**
1. Start at the root tree.
2. Walk trees breadth-first. Send each level's missing ids in batched, concurrent `GetObjects` calls, with batch size and parallelism bounded.
3. Fetch blobs, and the chunks of chunked blobs, the same way, writing each blob as it completes.

**Rules:**
- A sparse profile is a set of directory paths, the empty path meaning the whole tree; a checkout holds exactly the files under them.
- Never request a tree that is neither inside a profile directory nor on the path to one, or a blob outside the profile. (INV-28)
- A revision switch fetches and writes only the tree diff between the old and new revisions, intersected with the profile.
</fetch_protocol>

<caching>
- **Snapshots**
  - A snapshot holds the frames of every tree, blob, and chunk in the `main` tip's root tree. It contains no history.
  - A new snapshot is written to blob storage every `SNAPSHOT_INTERVAL` (4 h).
  - A daemon with an empty store loads the newest snapshot, verifying every frame, and serves requests while it loads.
- **Load control at every tier**
  - Identical concurrent misses are coalesced into one upstream fetch. (INV-29)
  - Concurrency is bounded.
  - Overload is answered immediately with `RESOURCE_EXHAUSTED` and a retry-after value, never queued unboundedly. (INV-30)
- **Pre-warming:** caches pre-warm the objects changed by each new `main` tip.
- **Metrics:** every tier exports hits, misses, bytes, and rejections. The target is a hit rate above 99% at each tier.
</caching>

<client>
`scn` is a thin CLI over `scion-client`. Every command takes `--json`, which prints the proto3 JSON mapping of a `scion.v1` message: the RPC response the command wraps, or a `scion.v1` message defined for local commands.

| Command | Behavior |
|---|---|
| `checkout <rev>` | Sparse by default. Configure with `sparse add/remove/list`. |
| `worktree add` | Creates a new sparse worktree sharing the node or user object store. |
| `status` | Lists changed and untracked files. See the status rules below. |
| `diff` | Shows working-copy changes. |
| `commit [paths]` | Hashes changed files, uploads missing blobs, then calls `CreateChangeset`. |
| `amend` | Rewrites the current changeset. |
| `restore <path>` | Discards local edits to a path. |
| `shelve` / `unshelve` | Shelves changes as a changeset on no ref. |
| `pull` | Moves to the latest `main`, rebasing the stack on the server. |
| `rebase` | Server-side rebase. |
| `merge` | Server-side merge. |
| `land` | Lands the stack on `main`. |
| `backout <cs>` | Server-side backout. |
| `graft <cs> --onto <ref>` | Server-side graft. |
| `goto <rev>` | Moves the working copy to another revision. |
| `log` / `blame` / `show <rev>` / `cat <rev>:<path>` | Read-only views served by the server. |

- **No staging area.** `commit` records every change to non-ignored files, or only the paths given. Ignore rules come from `.scnignore`, which uses gitignore syntax.
- **No local commits and no push.** A stack is your changesets that are not yet on `main`.
- **Status rules**
  - Status queries a long-lived watcher, one per worktree, started automatically, that keeps a journal and a cursor.
  - It re-stats only the journaled paths.
  - It hashes a file only if `(size, mtime_ns, ino, mode)` differs from dirstate.
  - If the watcher is missing or has overflowed, status does one parallel stat walk and restarts it.
  - On a filesystem that cannot represent a file's kind (no executable bit, or no symlink support), status and commit keep the kind from the base tree. (INV-46)
- **Diff output:** binary files show as "binary differs".
- **Virtual checkout:** a FUSE mount on Linux, an NFSv3 loopback mount on macOS, and ProjFS on Windows.
  - `readdir`, stat, and, on FUSE, the `user.scion.sha256` xattr come from trees.
  - Reads fetch the blob on first access.
  - Writes go to an overlay, so status costs O(modified files). (INV-31)
- **Local state** lives in `.scn/`: the base changeset, the sparse profile, and the dirstate. Every write is atomic, so an interrupted command can simply be rerun. (INV-32)
- **Offline:** commands that need the server fail fast with exit code 3 and leave state intact. `status`, `diff`, and `restore` work from local state.
- **Exit codes:**

  | Code | Meaning |
  |---|---|
  | 0 | OK |
  | 1 | User error |
  | 2 | Conflict |
  | 3 | Transient or network error |
  | 4 | Integrity error |
</client>

<ci>
CI gets its savings from four things: the daemon on every node, snapshot seeding, warm regional caches, and jobs that need no checkout at all.

- **What a change touched:** `Diff(base, head, merge_base=true, mode=names)` answers this with no checkout.
- **Build and test cache keys:** `ResolvePaths` returns subtree ids, and CI uses them as keys. An unchanged id means the subtree is unchanged.
- **Triggers:** CI triggers on `WatchRefs`. It never polls or scans history.
- **Bisect and revert pipelines** use `NthAncestor`, `Log`, and `Backout`/`CommitAndLand` with no checkout.
- **Reused worktrees** switch revision incrementally, as described in `<fetch_protocol>`.
</ci>

<storage>
- **Metadata database:** behind the `MetaStore` trait.
  - Production: CockroachDB through the Postgres wire protocol; single-node mode: Postgres.
  - Development: Postgres.
  - Tests only: in-memory.
- **Blob storage:** behind the `BlobStore` trait, using the `object_store` crate. Objects are written put-if-absent and never overwritten.
- **Conformance:** every backend passes one shared conformance suite.
- **Keys:** every key includes `repo_id`. Deduplication never crosses repos.
</storage>

<security>
- **ACL domains**
  - A repo's ACL config is a versioned server-side record mapping path prefixes to ACL domains, with one default domain; it is never part of a changeset. `SetAclConfig` takes the expected current version and fails with `ACL_VERSION_MISMATCH` otherwise.
  - After an ACL config change, a request that needs a RestrictedRoots value not yet derived for the new version is denied with `ACL_PENDING` until it is. (INV-45)
  - Objects below a restricted directory root are reachable only through that root's tree.
- **Object plane**
  - The server answers every cache fill as `cacheable` or `private`. Restricted-root trees are `private`. Caches and daemons never store `private` frames (INV-33), and pass requests for them to the server with the end caller's credential.
  - Restricting a path or tightening a domain pushes the affected root tree ids on the purge stream.
  - The daemon authenticates each job, mapping the peer credential to that job's token. It never serves requests on its own node identity. (INV-34)
- **Control plane**
  - Results omit paths the caller cannot read. (INV-35)
  - A write needs `writer` on every domain it touches.
  - A ref CAS needs `lander`.
- **Principals and roles**
  - Principals are users, agents, services, and groups.
  - Roles are `reader`, `writer`, `lander`, and `admin`.
  - The platform may act on behalf of a user; every such action is audited.
- **Audit log:** append-only, recording every mutation, every ACL change, and every restricted read.
- **Authentication:** clients send an OIDC JWT bearer token in the `authorization` gRPC metadata; its `sub` claim is the principal and its `name` and `email` claims form the principal's `Name <email>`. Services authenticate with mTLS.
- **Transport:** TLS everywhere, and mTLS between services.
- **Rate limits:** applied per principal, separately for the control plane and the object plane.
- **Input hardening:** every decoder is fuzzed, and size limits are enforced before allocating.
</security>

<git_import>
`scion-import` performs the migration.
- **Order:** commits are processed parents first. A run can be resumed, and the same history always produces the same ids. (INV-36)
- **Memory:** bounded, and independent of history length.
- **Conversion:** each commit's first-parent tree diff becomes its `file_changes`. File bytes are copied verbatim. Author, committer, dates, and message are parsed into their fields, and `git_headers` is filled. Imported changesets are exempt from the case-conflict check.
- **Mode mapping:**

  | git mode | Becomes |
  |---|---|
  | 100644 | Regular |
  | 100755 | Executable |
  | 120000 | Symlink |
  | any other 100xxx | Regular |
  | gitlink | Rejected |

  LFS-tracked files are imported as their real content.
- **Malformed metadata:** an author or committer line that does not parse as `<ident> <seconds> <±HHMM>`, with seconds in i64 range and |offset| < 24 h, imports with its ident field set to the line's bytes after the keyword, verbatim, and its date set to `timestamp` (0, 0). Every field that parses equals its parse of `git_headers`. (INV-43)
- **Git shas:** `git_map` maps each git commit sha1 to its changeset id and its git tree sha1, so `ResolveRev` accepts git shas.
- **Verification:** every commit's derived tree is checked against git: paths, modes, and blob contents. Every commit object rebuilt from its `git_map` tree sha1, its parents' git sha1s, and its `git_headers` must hash to its original sha1. Any mismatch halts the import. (INV-37)
- **Incremental mode:** tails the git remote until cutover.
- **Export:** `scion-export` writes git objects with gix and continuously pushes `main`, `release/*`, and `tags/*` one way to a git remote with the `git` CLI, recording each mapping in `git_map`. An imported changeset exports to its original git sha1. Any other changeset exports to the git commit built from its tree (modes 100644, 100755, 120000, 40000, git entry order), its parents' exported sha1s, its author and committer with the offset written as ±HHMM truncated to whole minutes, and its message, without `signature`. Export is deterministic and an exported import round-trips to its original sha1. (INV-41)
</git_import>

<workspace>
```
Public (published, documented, semver-stable):
crates/scion-core    ids, encoding, frame, changeset/tree types (no I/O, no async; depends on no workspace crate)
crates/scion-proto   scion.v1 gRPC definitions and generated code (depends on no workspace crate)
crates/scion-client  connect, fetch, sparse profiles, dirstate, watcher, status, commit; usable with no CLI (depends only on scion-core and scion-proto — never store/derived/ops)
crates/scion-conformance  black-box scion.v1 conformance suite runnable against any server, cache, or daemon address (depends only on public crates)
Internal (publish = false, no stability promise):
crates/store         MetaStore, BlobStore, backends, conformance suite, fault-injection wrappers
crates/derived       derived-data framework + all types
crates/ops           create, diff, merge, rebase, land, backout, graft
crates/rpc           server-side layers: auth, tier guard, rate limit, coalescing, retry
crates/vfs           virtual-checkout backends: FUSE (fuser), NFSv3 (nfsserve), ProjFS (windows)
crates/import        git → scion import and scion → git export
bins/                scion-server, scion-cache, scion-daemon, scn, scion-import, scion-export, scion-fsck, scion-bench
fuzz/                fuzz targets
tests/sim            deterministic whole-cluster simulation
xtask/               project tooling, run with `cargo xtask`
```
- The workspace root holds a compose file that starts Postgres and an S3-compatible object store for development and tests.
- Public crates follow semver. A breaking change requires a major version and is checked with cargo-semver-checks.
- Public APIs never expose types from internal crates. Only scion-proto exposes tonic or prost types; its tonic client and server code sit behind the `client` and `server` cargo features, off by default. scion-client's public API exposes no tonic or prost types.
- scion-core's crate docs specify the canonical encoding, frame format, and id rules, and scion-core ships the golden vectors as language-neutral test vectors: input bytes, expected id, accept or reject.
- Public error enums and extensible structs are `#[non_exhaustive]`.
- Public crates use `#![deny(missing_docs)]`; scion-core also uses `#![forbid(unsafe_code)]`.
- The workspace MSRV is Rust 1.88 (edition 2024).
- A public crate never uses an exact (`=`) version requirement.
- Every crate is licensed MIT OR Apache-2.0. `cargo deny` allows only MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, MIT-0, Zlib, CC0-1.0, Unlicense, Unicode-3.0, NCSA, and CDLA-Permissive-2.0.
- Heavy optional functionality in public crates sits behind cargo features, off by default.
- Public crates use rustls with the ring crypto provider (tonic `tls-ring`). jsonwebtoken uses its `aws_lc_rs` backend.
**Dependencies:** tokio, tonic, prost, tonic-prost-build, protox, tower, bytes, blake3, sha2, zstd, thiserror, tracing, serde, toml, rustls, ring, jsonwebtoken, moka, object_store, tokio-postgres, gix, notify, ignore, icu_normalizer (`=2.4`, internal crates only), icu_casemap (`=2.4`, internal crates only), imara-diff, fuser, nfsserve, windows (`Win32_Storage_ProjectedFileSystem`), clap, proptest, arbitrary, libfuzzer-sys, turmoil, criterion.
**Tools:** cargo-fuzz, cargo-nextest, cargo-deny, cargo-semver-checks, sccache, buf, git, Docker Compose.
</workspace>

<targets>
All targets are medians on the **bench repo**, built by `scion-bench gen` with seed `0x5C10_2026`: 3,000,000 files in directories of depth at most 12 and fanout at most 64; file sizes log-normal with median 2 KiB and σ 2.0, capped at 64 MiB, filled with seeded pseudo-random bytes; 1,000,000 changesets, each changing a log-normal count of files with median 3, capped at 2,000; one merge every 100 changesets. The compressed tip must be at least 25 GB.

Bench cluster: 3 scion-server (32 vCPU, 128 GB RAM), 3 scion-cache (32 vCPU, 256 GB RAM), a 3-node CockroachDB (16 vCPU, 64 GB RAM, NVMe each), and an S3-compatible object store; client and daemon nodes have 16 vCPU, 64 GB RAM, and NVMe. One region, every RTT at most 1 ms, 25 Gbps per node.
Dev rows run scn directly against scion-cache; Agent and CI rows run scn through a warm scion-daemon; the Compute job row runs on a node whose daemon has only the latest snapshot. Every checkout uses a sparse profile covering `BENCH_PROFILE` (1%) of the tip's files.

Benchmarks run on CockroachDB plus an object store. Each result is reported per cache state: cold, warm cache, and warm daemon. A target counts as met only when the harness output is attached.

| Environment | Operation | Target |
|---|---|---|
| Dev | status | 19 ms |
| Dev | diff | 67 ms |
| Dev | checkout | 1.1 s |
| Dev | commit | 1.1 s |
| Dev | rebase | 2.7 s |
| Dev | pull | 3.5 s |
| Agent | new worktree | 5.1 s |
| Agent | pull | 9.0 s |
| Agent | commit | 761 ms |
| Agent | status | <0.2 s |
| CI | checkout | 3.5 s |
| Compute job | checkout | 22 s |
| Every tier | hit rate during the warm CI checkout benchmark | >99% |
</targets>

<milestones>
Every acceptance test runs on Postgres plus an object store, never on in-memory backends alone.

| M | Deliverable | Acceptance |
|---|---|---|
| 1 | scion-core: ids, encoding, frame | Golden vectors pass; each decoder's fuzz target runs `FUZZ_BUDGET` (30 min) with no crash or panic; non-canonical input is rejected; the language-neutral vectors match the Rust golden vectors; scion-core builds its docs with no warnings; `cargo semver-checks` fails on a fixture release that removes a public scion-core item. (INV-8, INV-10) |
| 2 | store: traits, backends, conformance suite | One conformance suite passes on in-memory, Postgres, and object store, including that a put of an existing key never changes its stored bytes; ref CAS is linearizable at 64-way concurrency. (INV-9, INV-16) |
| 3 | CreateChangeset, RootTree, CommitGraph, RestrictedRoots, diff | Create cost on a 3M-file repo is within 1.5× of a 10k-file repo; a store wrapper that fails on any ref access or lock shows creation uses neither; no-op entries, path-prefix conflicts, and case conflicts are rejected, and case-conflict checks use the stated Unicode version; equal directory contents give equal tree ids regardless of history; incremental derivation equals from-scratch, and concurrent derivations of one key write it once; a crash at every write step leaves no broken reference. (INV-1, INV-2, INV-11, INV-12, INV-13, INV-19, INV-20, INV-22) |
| 4 | Ancestry, merge base, nth ancestor | Results equal a naive model on random DAGs of at least 10k nodes, and on a 1M-commit history with a merge every 100 commits, a node-visit counter shows each query visits at most 64 × ⌈log2 n⌉ nodes plus output size. (INV-21) |
| 5 | Server, scion-proto, auth, ACL, ref_log, WatchRefs | ACL leakage suite passes on every RPC; a changeset is readable from every server as soon as its create call returns, and ref reads return the latest CAS under replica lag; moving a `tags/*` ref fails; every CAS writes exactly one `ref_log` row in its transaction, with gap-free `seq`; every server refuses a redacted id once `Redact` returns; scion-proto builds its docs with no warnings; `buf breaking` fails on a fixture that removes or retypes a `scion.v1` field; a write RPC retried with the same idempotency key returns the original result and runs once, and reusing a key with different arguments fails; every error path returns a reason code and retryable flag; after `SetAclConfig`, a request needing an underived RestrictedRoots value is denied with `ACL_PENDING` until it is derived; after `RevokeRole`, the principal's next request is denied; admin writes are idempotent. (INV-3, INV-4, INV-17, INV-18, INV-26, INV-35, INV-38, INV-39, INV-40, INV-45) |
| 6 | Object plane on the server; scion-client; scn checkout, status, commit, diff, pull, worktree | A 1% sparse checkout fetches 0 objects outside its profile; an interrupted checkout resumes; corrupted frames are rejected, and bytes that do not hash to their id are rejected on upload and on materialization; scion-client, with no CLI, drives checkout, status, and commit end to end, and builds its docs with no warnings. (INV-15, INV-28, INV-32) |
| 7 | scion-cache, scion-daemon, snapshots, tier guard | Identical RPC conformance across all three tiers, and wrong-tier callers are rejected; 10k identical misses cause 1 upstream fetch; with the cache tier down, server object QPS stays at baseline; overload returns `RESOURCE_EXHAUSTED` with queue depth bounded; cache and daemon forward frames byte-identical with per-object memory independent of object size, and scion-cache writes no files; `private` frames are never stored; daemon upstream requests carry the job's credential; every cache and daemon stops serving a purged id within `PURGE_LAG_MAX`; the same frame fetched through different servers, caches, and daemons is byte-identical, and a resume through a different cache completes; a seeded daemon serves a warm checkout with 0 upstream fetches; scion-conformance passes against a cluster's server, cache, and daemon addresses and against a single-node process. (INV-5, INV-6, INV-7, INV-14, INV-27, INV-29, INV-30, INV-33, INV-34, INV-44) |
| 8 | Rebase, merge, land, backout, graft, mutation records | 100 concurrent landers on disjoint files all land, with no livelock and a gap-free ref_log; conflicting lands fail with the exact conflicting paths and commit nothing; every amend, rebase, land, backout, and graft writes a mutation record. (INV-18, INV-23, INV-24, INV-25) |
| 9 | HistoryManifest, Blame, CopyIndex, PathSearch | Blame and log equal a naive model; CopyIndex equals a naive model comparing every candidate pair on changesets under `COPY_MAX_CANDIDATES`, stops at exactly `COPY_MAX_CANDIDATES` comparisons above it, and derives identical bytes twice; PathSearch equals a naive filter of the full file list. (INV-42) |
| 10 | Watcher, virtual checkout (FUSE, NFS, ProjFS), remaining client commands | With the watcher, status after editing 10 files in a 1M-file worktree stats at most 10 files; every virtual-checkout backend passes lookup, getattr, readdir, open, read, mmap read, write, create, truncate, fsync, unlink, rename (including over an existing file), mkdir, rmdir, symlink, readlink, and chmod of the executable bit (except on ProjFS); reads return EIO on network loss; on a filesystem without executable bits or symlinks, status reports no change for such files and commit keeps the base tree's kind. (INV-31, INV-46) |
| 11 | git import | The Linux kernel history up to tag v6.12 imports, verifies, and produces the same ids on rerun; every parsed field equals its parse of `git_headers`; a fixture history with malformed author, committer, and date lines imports with the fallback fields and passes the sha1 round-trip. (INV-36, INV-37, INV-43) |
| 12 | Deterministic simulation: partitions, kills, slow storage | Every INV holds continuously. |
| 13 | Full benchmark suite | Every target is measured on the bench repo; every warm-cache and warm-daemon result meets its target; cold-state results are reported with no pass/fail bar. |
| 14 | git mirror (`scion-export`) | Exporting the same history twice yields identical git sha1s; importing the Linux kernel history up to tag v6.12 and exporting it reproduces every original sha1; a history created in Scion, exported, imported into a fresh repo, and exported again yields identical git sha1s. (INV-41) |
</milestones>

<forbidden>
- No git-isms: no local commits, push, packfiles, delta compression, "haves" negotiation, staging area, or client-side history.
- Never read or write refs while creating changesets or fetching objects. No global locks and no global sequence.
- No `unimplemented!`, `todo!`, or mocks in non-test code. A missing feature is a failing acceptance test, not a stub.
- In-memory backends are for tests only.
- No tier skips or falls back past its upstream. CI jobs fetch objects only through the node daemon.
- No disk, Redis, or other persistence in scion-cache.
- Never decompress, re-encode, or fully buffer an object in cache or daemon.
- No per-object serial RPCs. Never fetch everything and prune afterwards.
- Never answer history by scanning commits or walking the full graph. History comes from derived data.
- Never recompute derived data on every read.
- Hashed bytes come only from the hand-written encoder. Never use serde or bincode for them.
- No GC, repack, or compaction.
- No unbounded queues, channels, or concurrency.
- Every invariant needs a test that fails when the invariant is broken. Crash and concurrency behavior must be tested, not argued.
- Never report performance without harness output from the bench repo.
</forbidden>
