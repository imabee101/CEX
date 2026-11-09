//! The Postgres store's bounded connection pool.

use std::ops::Deref;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::{Client, Config};

use super::postgres::pg;
use crate::StoreError;

/// Limits on a [`PgMetaStore`](crate::PgMetaStore)'s connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolLimits {
    /// The most connections open at once.
    pub max_connections: usize,
    /// The most callers waiting for a connection at once. A caller beyond
    /// this is refused at once with a retryable overload error, never
    /// queued.
    pub max_waiters: usize,
    /// How long a caller waits for a free connection before it is refused
    /// with a retryable overload error.
    pub acquire_timeout: Duration,
    /// How long opening a connection may take.
    pub connect_timeout: Duration,
    /// How long one statement may run on the server before it is cancelled.
    pub statement_timeout: Duration,
}

impl PoolLimits {
    /// The default limits for a pool of `max_connections` connections.
    pub fn new(max_connections: usize) -> Self {
        Self {
            max_connections,
            max_waiters: max_connections.saturating_mul(8),
            acquire_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(30),
        }
    }
}

/// How a [`PgMetaStore`](crate::PgMetaStore) secures its connections.
#[derive(Clone)]
pub enum PgTls {
    /// TLS with rustls, using this client configuration (built on the ring
    /// crypto provider, with the roots the deployment trusts).
    Rustls(std::sync::Arc<rustls::ClientConfig>),
    /// No TLS. Accepted only when every host in the configuration is a
    /// loopback address, as in the dev environment.
    LoopbackPlaintext,
}

impl std::fmt::Debug for PgTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Rustls(_) => "PgTls::Rustls",
            Self::LoopbackPlaintext => "PgTls::LoopbackPlaintext",
        })
    }
}

/// Opens connections and starts the task that drives each.
enum Connector {
    Rustls(tokio_postgres_rustls::MakeRustlsConnect),
    Plain,
}

impl Connector {
    /// The connector for `tls`, adjusting `config` to it: TLS is required,
    /// never merely preferred, so a server or an attacker on the path cannot
    /// downgrade a connection to plaintext.
    fn new(config: &mut Config, tls: PgTls) -> Result<Self, StoreError> {
        match tls {
            PgTls::Rustls(c) => {
                if config.get_ssl_mode() == SslMode::Disable {
                    return Err(StoreError::invalid(
                        "the Postgres configuration disables TLS, but TLS is required",
                    ));
                }
                config.ssl_mode(SslMode::Require);
                Ok(Self::Rustls(tokio_postgres_rustls::MakeRustlsConnect::new(
                    (*c).clone(),
                )))
            }
            PgTls::LoopbackPlaintext => {
                let loopback = |h: &Host| match h {
                    Host::Tcp(name) => {
                        name == "localhost"
                            || name
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                    }
                    #[cfg(unix)]
                    Host::Unix(_) => true,
                };
                // A `hostaddr` overrides the address a host name resolves to,
                // so it must be loopback too.
                if config.get_hosts().is_empty()
                    || !config.get_hosts().iter().all(loopback)
                    || !config
                        .get_hostaddrs()
                        .iter()
                        .all(std::net::IpAddr::is_loopback)
                {
                    return Err(StoreError::invalid(
                        "plaintext Postgres connections are allowed only to loopback hosts",
                    ));
                }
                Ok(Self::Plain)
            }
        }
    }

    async fn connect(&self, config: &Config) -> Result<Client, tokio_postgres::Error> {
        fn drive<S, T>(connection: tokio_postgres::Connection<S, T>)
        where
            S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
            T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "a Postgres connection failed");
                }
            });
        }
        match self {
            Self::Rustls(tls) => {
                let (client, connection) = config.connect(tls.clone()).await?;
                drive(connection);
                Ok(client)
            }
            Self::Plain => {
                let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
                drive(connection);
                Ok(client)
            }
        }
    }
}

/// A bounded pool: at most `max_connections` connections exist, and at most
/// `max_waiters` callers wait for one.
pub(crate) struct Pool {
    config: Config,
    connector: Connector,
    limits: PoolLimits,
    permits: Semaphore,
    waiting: AtomicUsize,
    idle: Mutex<Vec<Client>>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

/// Runs `$body`, an expression using the connection `$c: &Client`, on a
/// connection from `$pool`, and evaluates to `(Conn, value)`. If a reused
/// connection had died while idle (a server restart or failover), its first
/// round trip fails before anything reaches the server, and `$body` runs
/// again on a new connection. Use it only where running `$body` twice is
/// safe: a read, or a transaction's `BEGIN`.
macro_rules! with_conn {
    ($pool:expr, |$c:ident| $body:expr) => {{
        let pool = $pool;
        let mut conn = pool.checkout().await?;
        let first = {
            let $c: &tokio_postgres::Client = &conn;
            $body.await
        };
        match first {
            Ok(v) => (conn, v),
            Err(e) if conn.reused && $crate::meta::pg_pool::died_idle(&e) => {
                conn = pool.replace(conn).await?;
                let $c: &tokio_postgres::Client = &conn;
                let v = $body.await.map_err($crate::meta::postgres::pg)?;
                (conn, v)
            }
            Err(e) => return Err($crate::meta::postgres::pg(e)),
        }
    }};
}
pub(crate) use with_conn;

/// Whether a failed first round trip on a reused connection means the
/// connection had died while idle, so nothing reached the server and a fresh
/// connection may try again.
pub(crate) fn died_idle(e: &tokio_postgres::Error) -> bool {
    e.is_closed() || connection_failed(e)
}

/// Whether `e` is a failure of the connection itself (an I/O error), rather
/// than of a statement or of encoding or decoding values.
pub(crate) fn connection_failed(e: &tokio_postgres::Error) -> bool {
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        if s.is::<std::io::Error>() {
            return true;
        }
        source = s.source();
    }
    false
}

impl Pool {
    pub(crate) fn new(
        mut config: Config,
        tls: PgTls,
        limits: PoolLimits,
    ) -> Result<Self, StoreError> {
        if limits.max_connections == 0 {
            return Err(StoreError::invalid("max_connections is 0"));
        }
        config.connect_timeout(limits.connect_timeout);
        let connector = Connector::new(&mut config, tls)?;
        Ok(Self {
            config,
            connector,
            limits,
            permits: Semaphore::new(limits.max_connections),
            waiting: AtomicUsize::new(0),
            idle: Mutex::new(Vec::new()),
        })
    }

    async fn permit(&self) -> Result<SemaphorePermit<'_>, StoreError> {
        if let Ok(permit) = self.permits.try_acquire() {
            return Ok(permit);
        }
        let overloaded = || StoreError::Overloaded {
            retry_after: self.limits.acquire_timeout,
        };
        // Counted while waiting, and released however the wait ends,
        // including when the caller is cancelled.
        struct Waiting<'a>(&'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let previous = self.waiting.fetch_add(1, Ordering::AcqRel);
        let _waiting = Waiting(&self.waiting);
        if previous >= self.limits.max_waiters {
            return Err(overloaded());
        }
        let acquired =
            tokio::time::timeout(self.limits.acquire_timeout, self.permits.acquire()).await;
        match acquired {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(StoreError::Internal("the connection pool is closed".into())),
            Err(_) => Err(overloaded()),
        }
    }

    fn reuse(&self) -> Result<Option<Client>, StoreError> {
        let mut idle = self
            .idle
            .lock()
            .map_err(|_| StoreError::Internal("connection pool lock poisoned".into()))?;
        while let Some(c) = idle.pop() {
            if !c.is_closed() {
                return Ok(Some(c));
            }
        }
        Ok(None)
    }

    async fn open(&self) -> Result<Client, StoreError> {
        let client = self.connector.connect(&self.config).await.map_err(pg)?;
        let ms = self.limits.statement_timeout.as_millis();
        client
            .batch_execute(&format!("SET statement_timeout = {ms}"))
            .await
            .map_err(pg)?;
        Ok(client)
    }

    /// A connection: an idle one if there is one, else a new one.
    pub(crate) async fn checkout(&self) -> Result<Conn<'_>, StoreError> {
        let permit = self.permit().await?;
        let (client, reused) = match self.reuse()? {
            Some(c) => (c, true),
            None => (self.open().await?, false),
        };
        let mut conn = Conn::new(client, self, permit);
        conn.reused = reused;
        Ok(conn)
    }

    /// Replaces `conn`, a reused connection whose first round trip failed
    /// because it had died while idle, with a new connection.
    pub(crate) async fn replace<'a>(&'a self, mut conn: Conn<'a>) -> Result<Conn<'a>, StoreError> {
        let client = self.open().await?;
        conn.client = Some(client);
        conn.reused = false;
        Ok(conn)
    }

    /// A connection in a new read-committed transaction. Dropping the
    /// connection before [`Conn::commit`] closes it, so the server rolls the
    /// transaction back.
    pub(crate) async fn begin(&self) -> Result<Conn<'_>, StoreError> {
        const BEGIN: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";
        let mut conn = self.checkout().await?;
        // Marked before BEGIN is sent: if the caller is cancelled while it is
        // in flight, the connection is closed, never returned to the pool
        // inside a transaction.
        conn.in_transaction = true;
        match conn.batch_execute(BEGIN).await {
            Ok(()) => Ok(conn),
            // A reused connection that died while idle: BEGIN never reached
            // the server.
            Err(e) if conn.reused && died_idle(&e) => {
                let conn = self.replace(conn).await?;
                conn.batch_execute(BEGIN).await.map_err(pg)?;
                Ok(conn)
            }
            Err(e) => Err(pg(e)),
        }
    }
}

/// A pooled connection, returned to the pool when dropped unless it closed
/// or is inside a transaction.
pub(crate) struct Conn<'a> {
    client: Option<Client>,
    pool: &'a Pool,
    in_transaction: bool,
    pub(crate) reused: bool,
    _permit: SemaphorePermit<'a>,
}

impl<'a> Conn<'a> {
    fn new(client: Client, pool: &'a Pool, permit: SemaphorePermit<'a>) -> Self {
        Self {
            client: Some(client),
            pool,
            in_transaction: false,
            reused: false,
            _permit: permit,
        }
    }

    /// Commits the transaction [`Pool::begin`] started.
    pub(crate) async fn commit(mut self) -> Result<(), StoreError> {
        self.batch_execute("COMMIT").await.map_err(pg)?;
        self.in_transaction = false;
        Ok(())
    }

    /// Rolls back the transaction [`Pool::begin`] started, so the connection
    /// returns to the pool.
    pub(crate) async fn rollback(mut self) -> Result<(), StoreError> {
        self.batch_execute("ROLLBACK").await.map_err(pg)?;
        self.in_transaction = false;
        Ok(())
    }
}

impl Deref for Conn<'_> {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.client.as_ref().expect("present until drop")
    }
}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take()
            && !client.is_closed()
            && !self.in_transaction
            && let Ok(mut idle) = self.pool.idle.lock()
        {
            idle.push(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback() -> Config {
        let mut c = Config::new();
        c.host("127.0.0.1").user("scion");
        c
    }

    #[tokio::test]
    async fn a_cancelled_waiter_releases_its_place() {
        let mut limits = PoolLimits::new(1);
        limits.acquire_timeout = Duration::from_secs(60);
        let pool = Pool::new(loopback(), PgTls::LoopbackPlaintext, limits).unwrap();
        let _held = pool.permits.try_acquire().unwrap();
        // A caller that waits, then is cancelled while still waiting.
        let waited = tokio::time::timeout(Duration::from_millis(20), pool.permit()).await;
        assert!(waited.is_err(), "the pool had no free connection");
        assert_eq!(pool.waiting.load(Ordering::Acquire), 0);
    }

    #[test]
    fn tls_is_required_never_preferred() {
        let tls = || {
            PgTls::Rustls(std::sync::Arc::new(
                rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
            ))
        };
        let mut disabled = loopback();
        disabled.ssl_mode(SslMode::Disable);
        assert!(matches!(
            Pool::new(disabled, tls(), PoolLimits::new(1)),
            Err(StoreError::InvalidArgument(_))
        ));
        // The default, `prefer`, becomes `require`.
        let pool = Pool::new(loopback(), tls(), PoolLimits::new(1)).unwrap();
        assert_eq!(pool.config.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn plaintext_requires_every_address_to_be_loopback() {
        let mut c = loopback();
        c.hostaddr(std::net::IpAddr::from([10, 0, 0, 5]));
        assert!(matches!(
            Pool::new(c, PgTls::LoopbackPlaintext, PoolLimits::new(1)),
            Err(StoreError::InvalidArgument(_))
        ));
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use crate::testing::TestDatabase;

    /// Whether `client`'s next statement runs inside a transaction that
    /// began before it.
    async fn in_transaction(client: &Client) -> bool {
        // In a transaction's first statement, sent as one simple-protocol
        // message, the transaction and statement timestamps are equal.
        let rows = client
            .simple_query("SELECT now() <> statement_timestamp()")
            .await
            .unwrap();
        let row = rows.iter().find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        });
        row.expect("one row").get(0) == Some("t")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_never_returns_to_the_pool_inside_a_transaction() {
        let db = TestDatabase::create().await;
        let pool = Pool::new(
            db.config().clone(),
            PgTls::LoopbackPlaintext,
            PoolLimits::new(1),
        )
        .unwrap();
        let mut abandoned_inside = 0;
        for i in 0..60u64 {
            // Abandon a transaction at a growing delay: before, during, and
            // after its BEGIN and its statement.
            let work = async {
                let conn = pool.begin().await?;
                conn.batch_execute("SELECT pg_sleep(0.002)")
                    .await
                    .map_err(super::super::postgres::pg)?;
                Ok::<_, StoreError>(conn)
            };
            match tokio::time::timeout(Duration::from_micros(i * i * 3), work).await {
                Ok(Ok(conn)) => drop(conn),
                Ok(Err(e)) => panic!("{e}"),
                Err(_) => abandoned_inside += 1,
            }
            let conn = pool.checkout().await.unwrap();
            assert!(
                !in_transaction(&conn).await,
                "delay {i}: reused a connection in a transaction"
            );
        }
        assert!(abandoned_inside > 0, "no call was abandoned");
    }
}
