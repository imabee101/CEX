//! The Postgres metadata store, also used for CockroachDB through the
//! Postgres wire protocol.

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

use bytes::Bytes;
use scion_core::{ChangesetId, TreeId};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_postgres::{Client, Config, NoTls, Row};

use super::{
    CasOutcome, DerivedKey, MetaKey, MetaStore, RefLogEntry, RefLogReason, RefState, RefUpdate,
    check_distinct, check_limit, check_name, check_prefix, check_update,
};
use crate::{PutOutcome, RepoId, StoreError};

/// The schema. Every statement is idempotent, so applying it again is a
/// no-op. Every key includes `repo`.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS changesets (
    repo INT8 NOT NULL,
    id BYTEA NOT NULL CHECK (length(id) = 32),
    bytes BYTEA NOT NULL,
    PRIMARY KEY (repo, id)
);
CREATE TABLE IF NOT EXISTS trees (
    repo INT8 NOT NULL,
    id BYTEA NOT NULL CHECK (length(id) = 32),
    bytes BYTEA NOT NULL,
    PRIMARY KEY (repo, id)
);
CREATE TABLE IF NOT EXISTS derived (
    repo INT8 NOT NULL,
    kind INT4 NOT NULL,
    version INT4 NOT NULL,
    changeset BYTEA NOT NULL CHECK (length(changeset) = 32),
    bytes BYTEA NOT NULL,
    PRIMARY KEY (repo, kind, version, changeset)
);
CREATE TABLE IF NOT EXISTS refs (
    repo INT8 NOT NULL,
    name BYTEA NOT NULL,
    target BYTEA NOT NULL CHECK (length(target) = 32),
    seq INT8 NOT NULL CHECK (seq > 0),
    PRIMARY KEY (repo, name)
);
CREATE TABLE IF NOT EXISTS ref_log (
    repo INT8 NOT NULL,
    name BYTEA NOT NULL,
    seq INT8 NOT NULL CHECK (seq > 0),
    old BYTEA CHECK (length(old) = 32),
    new BYTEA NOT NULL CHECK (length(new) = 32),
    actor TEXT NOT NULL,
    time_micros INT8 NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (repo, name, seq)
);
";

/// A [`MetaStore`] in Postgres, or CockroachDB through the Postgres wire
/// protocol, with a bounded connection pool.
#[derive(Debug)]
pub struct PgMetaStore {
    pool: Pool,
}

impl PgMetaStore {
    /// Creates a store that opens at most `max_connections` connections.
    pub fn new(config: Config, max_connections: usize) -> Result<Self, StoreError> {
        if max_connections == 0 {
            return Err(StoreError::invalid("max_connections is 0"));
        }
        Ok(Self {
            pool: Pool {
                config,
                permits: Semaphore::new(max_connections),
                idle: Mutex::new(Vec::new()),
            },
        })
    }

    /// Creates the schema if it does not exist. Run once per database before
    /// serving.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await.map_err(pg)?;
        tx.batch_execute(SCHEMA).await.map_err(pg)?;
        tx.commit().await.map_err(pg)
    }
}

fn pg(e: tokio_postgres::Error) -> StoreError {
    // The top-level message of a server error is just "db error"; keep the
    // server's message and code.
    match e.as_db_error() {
        Some(db) => {
            StoreError::Unavailable(format!("postgres: {} ({})", db.message(), db.code().code()))
        }
        None => StoreError::Unavailable(format!("postgres: {e}")),
    }
}

fn repo_param(repo: RepoId) -> i64 {
    // Bit-preserving: every u64 has its own i64.
    repo.0 as i64
}

fn id_bytes(bytes: &[u8]) -> Result<[u8; 32], StoreError> {
    bytes
        .try_into()
        .map_err(|_| StoreError::DataLoss(format!("stored id has {} bytes", bytes.len())))
}

fn seq_from(v: i64) -> Result<u64, StoreError> {
    u64::try_from(v).map_err(|_| StoreError::DataLoss(format!("stored seq {v} is negative")))
}

fn ref_state(name: &[u8], target: &[u8], seq: i64) -> Result<RefState, StoreError> {
    Ok(RefState {
        name: String::from_utf8(name.to_vec())
            .map_err(|_| StoreError::DataLoss("stored ref name is not UTF-8".into()))?,
        target: ChangesetId::from_bytes(id_bytes(target)?),
        seq: seq_from(seq)?,
    })
}

/// The smallest byte string greater than every string starting with
/// `prefix`, or `None` when no such bound exists.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

impl MetaStore for PgMetaStore {
    async fn put(
        &self,
        repo: RepoId,
        entries: &[(MetaKey, Bytes)],
    ) -> Result<Vec<PutOutcome>, StoreError> {
        check_distinct(entries.iter().map(|(k, _)| k))?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let (mut cs_ids, mut cs_bytes) = (Vec::new(), Vec::new());
        let (mut tree_ids, mut tree_bytes) = (Vec::new(), Vec::new());
        let (mut d_kinds, mut d_versions, mut d_changesets, mut d_bytes) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        // Insert in key order (MetaKey orders by table, then by key bytes), so
        // concurrent batches over overlapping keys take their index locks in
        // the same order and cannot deadlock.
        let mut sorted: Vec<&(MetaKey, Bytes)> = entries.iter().collect();
        sorted.sort_unstable_by_key(|(key, _)| *key);
        for (key, bytes) in sorted {
            match key {
                MetaKey::Changeset(id) => {
                    cs_ids.push(id.as_bytes().as_slice());
                    cs_bytes.push(bytes.as_ref());
                }
                MetaKey::Tree(id) => {
                    tree_ids.push(id.as_bytes().as_slice());
                    tree_bytes.push(bytes.as_ref());
                }
                MetaKey::Derived(d) => {
                    d_kinds.push(i32::from(d.kind));
                    d_versions.push(i32::from(d.version));
                    d_changesets.push(d.changeset.as_bytes().as_slice());
                    d_bytes.push(bytes.as_ref());
                }
            }
        }
        let repo_p = repo_param(repo);
        let mut created = HashSet::new();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await.map_err(pg)?;
        if !cs_ids.is_empty() {
            for row in tx
                .query(
                    "INSERT INTO changesets (repo, id, bytes) \
                     SELECT $1, i, b FROM UNNEST($2::bytea[], $3::bytea[]) WITH ORDINALITY AS t(i, b, n) \
                     ORDER BY n ON CONFLICT DO NOTHING RETURNING id",
                    &[&repo_p, &cs_ids, &cs_bytes],
                )
                .await
                .map_err(pg)?
            {
                let id = ChangesetId::from_bytes(id_bytes(row.get(0))?);
                created.insert(MetaKey::Changeset(id));
            }
        }
        if !tree_ids.is_empty() {
            for row in tx
                .query(
                    "INSERT INTO trees (repo, id, bytes) \
                     SELECT $1, i, b FROM UNNEST($2::bytea[], $3::bytea[]) WITH ORDINALITY AS t(i, b, n) \
                     ORDER BY n ON CONFLICT DO NOTHING RETURNING id",
                    &[&repo_p, &tree_ids, &tree_bytes],
                )
                .await
                .map_err(pg)?
            {
                let id = TreeId::from_bytes(id_bytes(row.get(0))?);
                created.insert(MetaKey::Tree(id));
            }
        }
        if !d_kinds.is_empty() {
            for row in tx
                .query(
                    "INSERT INTO derived (repo, kind, version, changeset, bytes) \
                     SELECT $1, k, v, c, b \
                     FROM UNNEST($2::int4[], $3::int4[], $4::bytea[], $5::bytea[]) \
                     WITH ORDINALITY AS t(k, v, c, b, n) ORDER BY n ON CONFLICT DO NOTHING RETURNING kind, version, changeset",
                    &[&repo_p, &d_kinds, &d_versions, &d_changesets, &d_bytes],
                )
                .await
                .map_err(pg)?
            {
                created.insert(MetaKey::Derived(derived_key(&row)?));
            }
        }
        tx.commit().await.map_err(pg)?;
        Ok(entries
            .iter()
            .map(|(key, _)| {
                if created.contains(key) {
                    PutOutcome::Created
                } else {
                    PutOutcome::Existed
                }
            })
            .collect())
    }

    async fn get(&self, repo: RepoId, keys: &[MetaKey]) -> Result<Vec<Option<Bytes>>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let (mut cs_ids, mut tree_ids) = (Vec::new(), Vec::new());
        let (mut d_kinds, mut d_versions, mut d_changesets) = (Vec::new(), Vec::new(), Vec::new());
        for key in keys {
            match key {
                MetaKey::Changeset(id) => cs_ids.push(id.as_bytes().as_slice()),
                MetaKey::Tree(id) => tree_ids.push(id.as_bytes().as_slice()),
                MetaKey::Derived(d) => {
                    d_kinds.push(i32::from(d.kind));
                    d_versions.push(i32::from(d.version));
                    d_changesets.push(d.changeset.as_bytes().as_slice());
                }
            }
        }
        let repo_p = repo_param(repo);
        let mut found = std::collections::HashMap::new();
        let conn = self.pool.get().await?;
        if !cs_ids.is_empty() {
            for row in conn
                .query(
                    "SELECT id, bytes FROM changesets WHERE repo = $1 AND id = ANY($2)",
                    &[&repo_p, &cs_ids],
                )
                .await
                .map_err(pg)?
            {
                let id = ChangesetId::from_bytes(id_bytes(row.get(0))?);
                found.insert(
                    MetaKey::Changeset(id),
                    Bytes::from(row.get::<_, Vec<u8>>(1)),
                );
            }
        }
        if !tree_ids.is_empty() {
            for row in conn
                .query(
                    "SELECT id, bytes FROM trees WHERE repo = $1 AND id = ANY($2)",
                    &[&repo_p, &tree_ids],
                )
                .await
                .map_err(pg)?
            {
                let id = TreeId::from_bytes(id_bytes(row.get(0))?);
                found.insert(MetaKey::Tree(id), Bytes::from(row.get::<_, Vec<u8>>(1)));
            }
        }
        if !d_kinds.is_empty() {
            for row in conn
                .query(
                    "SELECT d.kind, d.version, d.changeset, d.bytes FROM derived d \
                     JOIN UNNEST($2::int4[], $3::int4[], $4::bytea[]) AS t(k, v, c) \
                     ON d.kind = t.k AND d.version = t.v AND d.changeset = t.c \
                     WHERE d.repo = $1",
                    &[&repo_p, &d_kinds, &d_versions, &d_changesets],
                )
                .await
                .map_err(pg)?
            {
                found.insert(
                    MetaKey::Derived(derived_key(&row)?),
                    Bytes::from(row.get::<_, Vec<u8>>(3)),
                );
            }
        }
        Ok(keys.iter().map(|k| found.get(k).cloned()).collect())
    }

    async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefState>, StoreError> {
        check_name("ref name", name)?;
        let conn = self.pool.get().await?;
        let row = conn
            .query_opt(
                "SELECT name, target, seq FROM refs WHERE repo = $1 AND name = $2",
                &[&repo_param(repo), &name.as_bytes()],
            )
            .await
            .map_err(pg)?;
        row.map(|r| ref_state(r.get(0), r.get(1), r.get(2)))
            .transpose()
    }

    async fn list_refs(
        &self,
        repo: RepoId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RefState>, StoreError> {
        check_prefix("ref prefix", prefix)?;
        if let Some(after) = after {
            check_prefix("ref cursor", after)?;
        }
        check_limit(limit)?;
        let end = prefix_end(prefix.as_bytes());
        let after = after.map(str::as_bytes);
        let limit = i64::try_from(limit).map_err(|_| StoreError::invalid("limit overflows"))?;
        let conn = self.pool.get().await?;
        let rows = conn
            .query(
                "SELECT name, target, seq FROM refs \
                 WHERE repo = $1 AND name >= $2 \
                 AND ($3::bytea IS NULL OR name < $3) \
                 AND ($4::bytea IS NULL OR name > $4) \
                 ORDER BY name LIMIT $5",
                &[
                    &repo_param(repo),
                    &prefix.as_bytes(),
                    &end.as_deref(),
                    &after,
                    &limit,
                ],
            )
            .await
            .map_err(pg)?;
        rows.iter()
            .map(|r| ref_state(r.get(0), r.get(1), r.get(2)))
            .collect()
    }

    async fn update_ref(
        &self,
        repo: RepoId,
        update: &RefUpdate<'_>,
    ) -> Result<CasOutcome, StoreError> {
        check_update(update)?;
        let repo_p = repo_param(repo);
        let name = update.name.as_bytes();
        let new = update.new.as_bytes().as_slice();
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await.map_err(pg)?;
        // Lock the row, so the comparison and the write see the same state.
        let current = tx
            .query_opt(
                "SELECT name, target, seq FROM refs WHERE repo = $1 AND name = $2 FOR UPDATE",
                &[&repo_p, &name],
            )
            .await
            .map_err(pg)?
            .map(|r| ref_state(r.get(0), r.get(1), r.get(2)))
            .transpose()?;
        if current.as_ref().map(|c| c.target) != update.expected {
            return Ok(CasOutcome::Mismatch { current });
        }
        let seq = match current {
            Some(_) => tx
                .query_one(
                    "UPDATE refs SET target = $3, seq = seq + 1 \
                     WHERE repo = $1 AND name = $2 RETURNING seq",
                    &[&repo_p, &name, &new],
                )
                .await
                .map_err(pg)?
                .get::<_, i64>(0),
            None => {
                let inserted = tx
                    .query_opt(
                        "INSERT INTO refs (repo, name, target, seq) VALUES ($1, $2, $3, 1) \
                         ON CONFLICT DO NOTHING RETURNING seq",
                        &[&repo_p, &name, &new],
                    )
                    .await
                    .map_err(pg)?;
                match inserted {
                    Some(row) => row.get::<_, i64>(0),
                    None => {
                        // A concurrent CAS created the ref after our read and
                        // has committed; refs are never deleted, so it still
                        // exists and the expected absence is false.
                        let current = tx
                            .query_one(
                                "SELECT name, target, seq FROM refs WHERE repo = $1 AND name = $2",
                                &[&repo_p, &name],
                            )
                            .await
                            .map_err(pg)?;
                        return Ok(CasOutcome::Mismatch {
                            current: Some(ref_state(
                                current.get(0),
                                current.get(1),
                                current.get(2),
                            )?),
                        });
                    }
                }
            }
        };
        let old = update.expected.map(|o| *o.as_bytes());
        tx.execute(
            "INSERT INTO ref_log (repo, name, seq, old, new, actor, time_micros, reason) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &repo_p,
                &name,
                &seq,
                &old.as_ref().map(<[u8; 32]>::as_slice),
                &new,
                &update.actor,
                &update.time_micros,
                &update.reason.as_str(),
            ],
        )
        .await
        .map_err(pg)?;
        tx.commit().await.map_err(pg)?;
        Ok(CasOutcome::Updated {
            seq: seq_from(seq)?,
        })
    }

    async fn ref_log(
        &self,
        repo: RepoId,
        name: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>, StoreError> {
        check_name("ref name", name)?;
        check_limit(limit)?;
        let Ok(after) = i64::try_from(after_seq) else {
            // Stored seqs fit in an i64, so none is greater.
            return Ok(Vec::new());
        };
        let limit = i64::try_from(limit).map_err(|_| StoreError::invalid("limit overflows"))?;
        let conn = self.pool.get().await?;
        let rows = conn
            .query(
                "SELECT seq, old, new, actor, time_micros, reason FROM ref_log \
                 WHERE repo = $1 AND name = $2 AND seq > $3 ORDER BY seq LIMIT $4",
                &[&repo_param(repo), &name.as_bytes(), &after, &limit],
            )
            .await
            .map_err(pg)?;
        rows.iter()
            .map(|r| {
                Ok(RefLogEntry {
                    seq: seq_from(r.get(0))?,
                    old: r
                        .get::<_, Option<&[u8]>>(1)
                        .map(|o| id_bytes(o).map(ChangesetId::from_bytes))
                        .transpose()?,
                    new: ChangesetId::from_bytes(id_bytes(r.get(2))?),
                    actor: r.get(3),
                    time_micros: r.get(4),
                    reason: RefLogReason::parse(r.get(5))?,
                })
            })
            .collect()
    }
}

fn derived_key(row: &Row) -> Result<DerivedKey, StoreError> {
    let narrow = |v: i32| {
        u16::try_from(v).map_err(|_| StoreError::DataLoss(format!("stored derived code {v}")))
    };
    Ok(DerivedKey {
        kind: narrow(row.get(0))?,
        version: narrow(row.get(1))?,
        changeset: ChangesetId::from_bytes(id_bytes(row.get(2))?),
    })
}

/// A bounded pool: at most `permits` connections exist at once.
#[derive(Debug)]
struct Pool {
    config: Config,
    permits: Semaphore,
    idle: Mutex<Vec<Client>>,
}

impl Pool {
    async fn get(&self) -> Result<Conn<'_>, StoreError> {
        let permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable("connection pool closed".into()))?;
        let reused = loop {
            let next = self
                .idle
                .lock()
                .map_err(|_| StoreError::Unavailable("connection pool lock poisoned".into()))?
                .pop();
            match next {
                Some(c) if c.is_closed() => continue,
                other => break other,
            }
        };
        let client = match reused {
            Some(c) => c,
            None => {
                let (client, connection) = self.config.connect(NoTls).await.map_err(pg)?;
                tokio::spawn(connection);
                client
            }
        };
        Ok(Conn {
            client: Some(client),
            pool: self,
            _permit: permit,
        })
    }
}

/// A pooled connection, returned to the pool when dropped unless it closed.
struct Conn<'a> {
    client: Option<Client>,
    pool: &'a Pool,
    _permit: SemaphorePermit<'a>,
}

impl Deref for Conn<'_> {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.client.as_ref().expect("present until drop")
    }
}

impl DerefMut for Conn<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client.as_mut().expect("present until drop")
    }
}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take()
            && !client.is_closed()
            && let Ok(mut idle) = self.pool.idle.lock()
        {
            idle.push(client);
        }
    }
}
