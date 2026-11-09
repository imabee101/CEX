//! The Postgres metadata store, also used for CockroachDB through the
//! Postgres wire protocol.

use std::collections::HashSet;

use bytes::Bytes;
use scion_core::{BlobId, ChangesetId, TreeId};
use tokio_postgres::{Config, Row};

use super::pg_pool::{PgTls, Pool, PoolLimits, with_conn};

use super::{
    CasOutcome, DerivedKey, MetaKey, MetaStore, RefLogEntry, RefLogReason, RefState, RefUpdate,
    check_distinct, check_limit, check_name, check_prefix, check_update,
};
use crate::{PutOutcome, RepoId, StoreError};

/// The schema version [`PgMetaStore::migrate`] brings a database to.
pub const SCHEMA_VERSION: usize = MIGRATIONS.len();

/// The schema migrations, in order; migration `i` brings the schema to
/// version `i + 1`. A migration, once released, never changes: a change to
/// the schema is a new migration. Every key includes `repo`.
const MIGRATIONS: &[&str] = &["
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
CREATE TABLE IF NOT EXISTS blobs (
    repo INT8 NOT NULL,
    id BYTEA NOT NULL CHECK (length(id) = 32),
    bytes BYTEA NOT NULL,
    PRIMARY KEY (repo, id)
);
CREATE TABLE IF NOT EXISTS case_nodes (
    repo INT8 NOT NULL,
    id BYTEA NOT NULL CHECK (length(id) = 32),
    bytes BYTEA NOT NULL,
    PRIMARY KEY (repo, id)
);
CREATE TABLE IF NOT EXISTS derived (
    repo INT8 NOT NULL,
    kind INT4 NOT NULL,
    version INT4 NOT NULL,
    param INT8 NOT NULL,
    changeset BYTEA NOT NULL CHECK (length(changeset) = 32),
    bytes BYTEA NOT NULL,
    PRIMARY KEY (repo, kind, version, param, changeset)
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
"];

/// A [`MetaStore`] in Postgres, or CockroachDB through the Postgres wire
/// protocol, with a bounded connection pool.
///
/// Every transaction runs at `READ COMMITTED`, set explicitly. CockroachDB
/// may run it at `SERIALIZABLE` instead; a serialization failure is then
/// reported as retryable.
#[derive(Debug)]
pub struct PgMetaStore {
    pool: Pool,
}

impl PgMetaStore {
    /// Creates a store that connects with `config`, secured by `tls`, within
    /// `limits`.
    pub fn new(config: Config, tls: PgTls, limits: PoolLimits) -> Result<Self, StoreError> {
        Ok(Self {
            pool: Pool::new(config, tls, limits)?,
        })
    }

    /// Brings the schema up to date by applying the migrations it lacks, in
    /// one transaction. Concurrent calls, from any number of servers, are
    /// serialized by a row lock, so each migration is applied exactly once.
    /// Fails if the database's schema is newer than this code knows.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        // Creating the version table races with other servers doing the
        // same; Postgres reports the loser as a duplicate, which is success.
        let (conn, ()) = with_conn!(&self.pool, |c| async {
            match c
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS schema_version (version INT8 PRIMARY KEY)",
                )
                .await
            {
                Err(e) if is_duplicate_object(&e) => Ok(()),
                other => other,
            }
        });
        drop(conn);
        let conn = self.pool.begin().await?;
        // Row 0 is the lock every migrator takes, so only one applies
        // migrations at a time.
        conn.execute(
            "INSERT INTO schema_version VALUES (0) ON CONFLICT DO NOTHING",
            &[],
        )
        .await
        .map_err(pg)?;
        conn.execute(
            "SELECT version FROM schema_version WHERE version = 0 FOR UPDATE",
            &[],
        )
        .await
        .map_err(pg)?;
        let current: i64 = conn
            .query_one("SELECT max(version) FROM schema_version", &[])
            .await
            .map_err(pg)?
            .get(0);
        let known = MIGRATIONS.len() as i64;
        if current > known {
            return Err(StoreError::Internal(format!(
                "the database schema is at version {current}, newer than this server's {known}"
            )));
        }
        for (i, migration) in MIGRATIONS.iter().enumerate().skip(current as usize) {
            conn.batch_execute(migration).await.map_err(pg)?;
            conn.execute("INSERT INTO schema_version VALUES ($1)", &[&(i as i64 + 1)])
                .await
                .map_err(pg)?;
        }
        conn.commit().await
    }
}

/// Whether `e` reports that an object being created already exists, the
/// outcome of racing `CREATE ... IF NOT EXISTS` statements.
fn is_duplicate_object(e: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    e.code().is_some_and(|c| {
        *c == SqlState::UNIQUE_VIOLATION
            || *c == SqlState::DUPLICATE_TABLE
            || *c == SqlState::DUPLICATE_OBJECT
    })
}

/// Whether a server error with SQLSTATE `code` may succeed on retry:
/// connection exceptions (08), insufficient resources (53), serialization
/// failures, CockroachDB's ambiguous result, deadlocks, cancelled statements
/// (the statement timeout), and a server shutting down or starting.
fn retryable_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        // 40003 is CockroachDB's ambiguous result: the statement may or may
        // not have committed. A retried ref CAS that had committed reports a
        // mismatch whose current target is its own; its caller must treat
        // that as success.
        || matches!(
            code,
            "40001" | "40003" | "40P01" | "57014" | "57P01" | "57P02" | "57P03" | "57P05"
        )
}

/// Classifies a Postgres error by what a retry can fix.
///
/// Connection failures, serialization failures, ambiguous results, and
/// deadlocks, a server out of resources or shutting down, and cancelled
/// statements (the statement timeout) are retryable. Everything else the
/// server reports (bad data, constraint violations, undefined objects,
/// permissions, program limits) means this code or its configuration is
/// wrong, and a retry cannot help. An error without a SQLSTATE is
/// retryable only when the connection itself failed.
pub(crate) fn pg(e: tokio_postgres::Error) -> StoreError {
    // The top-level message of a server error is just "db error"; keep the
    // server's message and code.
    let Some(db) = e.as_db_error() else {
        // A failure of the connection is retryable; one of encoding or
        // decoding values, or of the protocol, is a defect.
        return if e.is_closed() || super::pg_pool::connection_failed(&e) {
            StoreError::Unavailable(format!("postgres: {e}"))
        } else {
            StoreError::Internal(format!("postgres: {e}"))
        };
    };
    let code = db.code().code();
    let message = format!("postgres: {} ({code})", db.message());
    if retryable_sqlstate(code) {
        StoreError::Unavailable(message)
    } else {
        StoreError::Internal(message)
    }
}

/// Rolls back a transaction whose outcome is already known. If the rollback
/// fails, the connection is closed (it is still marked as in a transaction),
/// which rolls back too, so the known outcome stands.
async fn release(tx: super::pg_pool::Conn<'_>) {
    if let Err(e) = tx.rollback().await {
        tracing::warn!(error = %e, "rolling back failed; the connection is closed instead");
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
        // One (ids, bytes) column pair per id-keyed table, in `ID_TABLES` order.
        let mut by_table: [IdColumns<'_>; ID_TABLES.len()] = Default::default();
        let (mut d_kinds, mut d_versions, mut d_params, mut d_changesets, mut d_bytes) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        // Insert in key order (MetaKey orders by table, then by key bytes), so
        // concurrent batches over overlapping keys take their index locks in
        // the same order and cannot deadlock.
        let mut sorted: Vec<&(MetaKey, Bytes)> = entries.iter().collect();
        sorted.sort_unstable_by_key(|(key, _)| *key);
        for (key, bytes) in sorted {
            match key {
                MetaKey::Derived(d) => {
                    d_kinds.push(i32::from(d.kind));
                    d_versions.push(i32::from(d.version));
                    d_params.push(d.param as i64);
                    d_changesets.push(d.changeset.as_bytes().as_slice());
                    d_bytes.push(bytes.as_ref());
                }
                _ => {
                    let Some((table, id)) = id_key(key) else {
                        continue;
                    };
                    by_table[table].0.push(id);
                    by_table[table].1.push(bytes.as_ref());
                }
            }
        }
        let repo_p = repo_param(repo);
        let mut created = HashSet::new();
        let conn = self.pool.begin().await?;
        for (table, (ids, values)) in by_table.iter().enumerate() {
            if ids.is_empty() {
                continue;
            }
            let (name, make) = ID_TABLES[table];
            for row in conn
                .query(
                    &format!(
                        "INSERT INTO {name} (repo, id, bytes) \
                         SELECT $1, i, b FROM UNNEST($2::bytea[], $3::bytea[]) WITH ORDINALITY AS t(i, b, n) \
                         ORDER BY n ON CONFLICT DO NOTHING RETURNING id"
                    ),
                    &[&repo_p, ids, values],
                )
                .await
                .map_err(pg)?
            {
                created.insert(make(id_bytes(row.get(0))?));
            }
        }
        if !d_kinds.is_empty() {
            for row in conn
                .query(
                    "INSERT INTO derived (repo, kind, version, param, changeset, bytes) \
                     SELECT $1, k, v, p, c, b \
                     FROM UNNEST($2::int4[], $3::int4[], $4::int8[], $5::bytea[], $6::bytea[]) \
                     WITH ORDINALITY AS t(k, v, p, c, b, n) ORDER BY n ON CONFLICT DO NOTHING \
                     RETURNING kind, version, param, changeset",
                    &[
                        &repo_p,
                        &d_kinds,
                        &d_versions,
                        &d_params,
                        &d_changesets,
                        &d_bytes,
                    ],
                )
                .await
                .map_err(pg)?
            {
                created.insert(MetaKey::Derived(derived_key(&row)?));
            }
        }
        conn.commit().await?;
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
        let mut by_table: [Vec<&[u8]>; ID_TABLES.len()] = Default::default();
        let (mut d_kinds, mut d_versions, mut d_params, mut d_changesets) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for key in keys {
            match key {
                MetaKey::Derived(d) => {
                    d_kinds.push(i32::from(d.kind));
                    d_versions.push(i32::from(d.version));
                    d_params.push(d.param as i64);
                    d_changesets.push(d.changeset.as_bytes().as_slice());
                }
                _ => {
                    let Some((table, id)) = id_key(key) else {
                        continue;
                    };
                    by_table[table].push(id);
                }
            }
        }
        let repo_p = repo_param(repo);
        let (_conn, (id_rows, derived_rows)) = with_conn!(&self.pool, |c| async {
            let mut id_rows = Vec::new();
            for (table, ids) in by_table.iter().enumerate() {
                if ids.is_empty() {
                    continue;
                }
                let name = ID_TABLES[table].0;
                let rows = c
                    .query(
                        &format!("SELECT id, bytes FROM {name} WHERE repo = $1 AND id = ANY($2)"),
                        &[&repo_p, ids],
                    )
                    .await?;
                id_rows.push((table, rows));
            }
            let derived_rows = if d_kinds.is_empty() {
                Vec::new()
            } else {
                c.query(
                        "SELECT d.kind, d.version, d.param, d.changeset, d.bytes FROM derived d \
                         JOIN UNNEST($2::int4[], $3::int4[], $4::int8[], $5::bytea[]) AS t(k, v, p, c) \
                         ON d.kind = t.k AND d.version = t.v AND d.param = t.p AND d.changeset = t.c \
                         WHERE d.repo = $1",
                        &[&repo_p, &d_kinds, &d_versions, &d_params, &d_changesets],
                    )
                    .await?
            };
            Ok::<_, tokio_postgres::Error>((id_rows, derived_rows))
        });
        let mut found = std::collections::HashMap::new();
        for (table, rows) in id_rows {
            let make = ID_TABLES[table].1;
            for row in rows {
                found.insert(
                    make(id_bytes(row.get(0))?),
                    Bytes::from(row.get::<_, Vec<u8>>(1)),
                );
            }
        }
        for row in derived_rows {
            found.insert(
                MetaKey::Derived(derived_key(&row)?),
                Bytes::from(row.get::<_, Vec<u8>>(4)),
            );
        }
        Ok(keys.iter().map(|k| found.get(k).cloned()).collect())
    }

    async fn get_ref(&self, repo: RepoId, name: &str) -> Result<Option<RefState>, StoreError> {
        check_name("ref name", name)?;
        let (_conn, row) = with_conn!(&self.pool, |c| async {
            c.query_opt(
                "SELECT name, target, seq FROM refs WHERE repo = $1 AND name = $2",
                &[&repo_param(repo), &name.as_bytes()],
            )
            .await
        });
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
        let (_conn, rows) = with_conn!(&self.pool, |c| async {
            c.query(
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
        });
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
        let tx = self.pool.begin().await?;
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
            // Release the row lock and keep the connection for the pool.
            release(tx).await;
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
                        let current = ref_state(current.get(0), current.get(1), current.get(2))?;
                        release(tx).await;
                        return Ok(CasOutcome::Mismatch {
                            current: Some(current),
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
        tx.commit().await?;
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
        let (_conn, rows) = with_conn!(&self.pool, |c| async {
            c.query(
                "SELECT seq, old, new, actor, time_micros, reason FROM ref_log \
                 WHERE repo = $1 AND name = $2 AND seq > $3 ORDER BY seq LIMIT $4",
                &[&repo_param(repo), &name.as_bytes(), &after, &limit],
            )
            .await
        });
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
        // Bit-preserving, as written.
        param: row.get::<_, i64>(2) as u64,
        changeset: ChangesetId::from_bytes(id_bytes(row.get(3))?),
    })
}

/// The id and bytes columns bound for one id-keyed table.
type IdColumns<'a> = (Vec<&'a [u8]>, Vec<&'a [u8]>);

/// Makes the key for a row id of one id-keyed table.
type IdToKey = fn([u8; 32]) -> MetaKey;

/// The tables keyed by `(repo, id)`, with the key each row id makes.
const ID_TABLES: [(&str, IdToKey); 4] = [
    ("blobs", |id| MetaKey::Blob(BlobId::from_bytes(id))),
    ("changesets", |id| {
        MetaKey::Changeset(ChangesetId::from_bytes(id))
    }),
    ("trees", |id| MetaKey::Tree(TreeId::from_bytes(id))),
    ("case_nodes", |id| MetaKey::CaseNode(TreeId::from_bytes(id))),
];

/// The index into [`ID_TABLES`] and the id bytes of a key, or `None` for
/// [`MetaKey::Derived`], which has its own table.
fn id_key(key: &MetaKey) -> Option<(usize, &[u8])> {
    match key {
        MetaKey::Blob(id) => Some((0, id.as_bytes())),
        MetaKey::Changeset(id) => Some((1, id.as_bytes())),
        MetaKey::Tree(id) => Some((2, id.as_bytes())),
        MetaKey::CaseNode(id) => Some((3, id.as_bytes())),
        MetaKey::Derived(_) => None,
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;
    use crate::meta::pg_pool::died_idle;
    use crate::testing::TestDatabase;

    #[test]
    fn sqlstates_are_retryable_only_when_a_retry_can_succeed() {
        for code in [
            "08006", "08001", "53300", "53200", "40001", "40003", "40P01", "57014", "57P01",
            "57P02", "57P03", "57P05",
        ] {
            assert!(retryable_sqlstate(code), "{code}");
        }
        for code in [
            "23505", "23514", "22001", "42P01", "42501", "54000", "28P01", "XX000",
        ] {
            assert!(!retryable_sqlstate(code), "{code}");
        }
    }

    #[tokio::test]
    async fn a_client_side_error_is_internal_and_never_replayed() {
        let db = TestDatabase::create().await;
        let client = db.connect().await;
        // A text value for an integer parameter fails to encode, client side,
        // with no SQLSTATE.
        let e = client
            .query("SELECT $1::int4", &[&"not a number"])
            .await
            .unwrap_err();
        assert!(e.code().is_none());
        assert!(!died_idle(&e));
        assert!(matches!(pg(e), StoreError::Internal(_)));
    }

    #[tokio::test]
    async fn a_dead_connection_is_retryable_and_replayable() {
        let db = TestDatabase::create().await;
        let client = db.connect().await;
        let pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        db.connect()
            .await
            .query_one("SELECT pg_terminate_backend($1, 10000)", &[&pid])
            .await
            .unwrap();
        let e = client.query("SELECT 1", &[]).await.unwrap_err();
        assert!(died_idle(&e), "{e:?}");
        assert!(matches!(pg(e), StoreError::Unavailable(_)));
    }
}

#[cfg(test)]
mod release_tests {
    use super::*;
    use crate::testing::TestDatabase;

    #[tokio::test]
    async fn a_failed_rollback_closes_the_connection_and_the_pool_recovers() {
        let db = TestDatabase::create().await;
        let pool = Pool::new(
            db.config().clone(),
            PgTls::LoopbackPlaintext,
            PoolLimits::new(1),
        )
        .unwrap();
        let tx = pool.begin().await.unwrap();
        let pid: i32 = tx
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        db.connect()
            .await
            .query_one("SELECT pg_terminate_backend($1, 10000)", &[&pid])
            .await
            .unwrap();
        // The rollback fails on the dead connection; release reports nothing,
        // because the outcome it releases is already known.
        release(tx).await;
        // The connection, still marked as in a transaction, was closed, not
        // returned to the pool.
        assert_eq!(pool.idle_len(), 0);
        // The next connection works and is not inside a transaction: in a
        // transaction's first statement the two timestamps are equal.
        let conn = pool.checkout().await.unwrap();
        let rows = conn
            .simple_query("SELECT pg_backend_pid(), now() = statement_timestamp()")
            .await
            .unwrap();
        let row = rows
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .unwrap();
        assert_ne!(row.get(0), Some(pid.to_string().as_str()));
        assert_eq!(row.get(1), Some("t"));
    }
}
