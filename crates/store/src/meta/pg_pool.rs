//! The Postgres store's bounded connection pool.

use std::ops::Deref;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_postgres::config::Host;
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
    fn new(config: &Config, tls: PgTls) -> Result<Self, StoreError> {
        match tls {
            PgTls::Rustls(c) => Ok(Self::Rustls(tokio_postgres_rustls::MakeRustlsConnect::new(
                (*c).clone(),
            ))),
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
                if config.get_hosts().is_empty() || !config.get_hosts().iter().all(loopback) {
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
    e.is_closed() || e.as_db_error().is_none() && e.code().is_none()
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
        let connector = Connector::new(&config, tls)?;
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
        if self.waiting.fetch_add(1, Ordering::AcqRel) >= self.limits.max_waiters {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return Err(overloaded());
        }
        let acquired =
            tokio::time::timeout(self.limits.acquire_timeout, self.permits.acquire()).await;
        self.waiting.fetch_sub(1, Ordering::AcqRel);
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
        let (mut conn, ()) = with_conn!(self, |c| c
            .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED"));
        conn.in_transaction = true;
        Ok(conn)
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
