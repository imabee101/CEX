//! A loopback proxy in front of a Postgres server that forwards the wire
//! protocol message by message and can stop either direction at an exact
//! message boundary, so a test can abandon a call at a chosen point of its
//! exchange with the server instead of after a guessed delay.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_postgres::config::Host;

/// A direction of a proxied connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Direction {
    /// Messages from the client to the server.
    ToServer,
    /// Messages from the server to the client.
    ToClient,
}

/// What a hold found when it stopped a message.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Stopped {
    /// Whether the stopped connection's client had sent `BEGIN` and not yet
    /// `COMMIT` or `ROLLBACK` (counting only messages that reached the
    /// server).
    pub in_transaction: bool,
}

#[derive(Debug, Default)]
struct State {
    /// The armed hold: its direction and how many messages it still lets
    /// through.
    armed: Option<(Direction, usize)>,
    /// What the armed hold found, once it stopped a message.
    stopped: Option<Stopped>,
    /// Bumped by every release; a message stopped under an older value
    /// proceeds.
    generation: u64,
}

#[derive(Debug, Default)]
struct Control {
    state: Mutex<State>,
    /// Notified when the armed hold stops a message.
    stopped: Notify,
    /// Notified when a hold is released.
    released: Notify,
    /// How many proxied connections are open.
    open: Mutex<usize>,
    /// Notified when a proxied connection closes.
    closed: Notify,
}

impl Control {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Waits until a message travelling in `direction` may proceed.
    async fn pass(&self, direction: Direction, in_transaction: bool) {
        let mut held_at = None;
        loop {
            let mut released = std::pin::pin!(self.released.notified());
            released.as_mut().enable();
            {
                let mut s = self.lock();
                if let Some(generation) = held_at {
                    if s.generation != generation {
                        return;
                    }
                } else {
                    match &mut s.armed {
                        Some((d, 0)) if *d == direction => {
                            s.stopped = Some(Stopped { in_transaction });
                            held_at = Some(s.generation);
                            self.stopped.notify_one();
                        }
                        Some((d, left)) if *d == direction => {
                            *left -= 1;
                            return;
                        }
                        _ => return,
                    }
                }
            }
            released.await;
        }
    }
}

/// A proxy to the Postgres server of a configuration, listening on a
/// loopback port, with at most one hold armed at a time. Dropping it stops
/// it and closes every proxied connection.
#[derive(Debug)]
pub struct PgProxy {
    addr: SocketAddr,
    control: Arc<Control>,
    accept: tokio::task::JoinHandle<()>,
    connections: Arc<Mutex<tokio::task::JoinSet<()>>>,
}

impl Drop for PgProxy {
    fn drop(&mut self) {
        self.accept.abort();
        self.connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .abort_all();
    }
}

impl PgProxy {
    /// Starts a proxy, on the current runtime, to the first host and port of
    /// `upstream`. Clients must not request TLS.
    pub async fn start(upstream: &tokio_postgres::Config) -> Self {
        let host = match upstream.get_hosts().first() {
            Some(Host::Tcp(h)) => h.clone(),
            other => panic!("the proxy needs a TCP host, not {other:?}"),
        };
        let port = upstream.get_ports().first().copied().unwrap_or(5432);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the proxy failed");
        let addr = listener.local_addr().expect("the proxy has an address");
        let control = Arc::new(Control::default());
        let connections = Arc::new(Mutex::new(tokio::task::JoinSet::new()));
        let accept = {
            let (control, connections) = (control.clone(), connections.clone());
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = TcpStream::connect((host.as_str(), port))
                        .await
                        .expect("the proxy could not reach Postgres");
                    // Forward each message as soon as it is written, as the
                    // client and server send it.
                    client
                        .set_nodelay(true)
                        .expect("setting TCP_NODELAY failed");
                    server
                        .set_nodelay(true)
                        .expect("setting TCP_NODELAY failed");
                    *control
                        .open
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
                    let control = control.clone();
                    connections
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .spawn(async move {
                            proxy(client, server, control.clone()).await;
                            *control
                                .open
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) -= 1;
                            control.closed.notify_waiters();
                        });
                }
            })
        };
        Self {
            addr,
            control,
            accept,
            connections,
        }
    }

    /// `base` with its host and port replaced by the proxy's.
    pub fn config(&self, base: &tokio_postgres::Config) -> tokio_postgres::Config {
        let mut c = tokio_postgres::Config::new();
        c.host(self.addr.ip().to_string()).port(self.addr.port());
        if let Some(u) = base.get_user() {
            c.user(u);
        }
        if let Some(p) = base.get_password() {
            c.password(p);
        }
        if let Some(d) = base.get_dbname() {
            c.dbname(d);
        }
        c
    }

    /// Arms a hold: the next `pass` messages in `direction`, on any
    /// connection, go through, and the one after them stops until
    /// [`release`](Self::release).
    pub fn hold(&self, direction: Direction, pass: usize) {
        let mut s = self.control.lock();
        assert!(s.armed.is_none(), "a hold is already armed");
        s.armed = Some((direction, pass));
        s.stopped = None;
    }

    /// Waits until the armed hold stops a message.
    async fn stopped(&self) -> Stopped {
        loop {
            let notified = self.control.stopped.notified();
            if let Some(stopped) = self.control.lock().stopped {
                return stopped;
            }
            // A notification left by an earlier hold finds nothing stopped.
            notified.await;
        }
    }

    /// Runs `call` until it completes or the armed hold stops a message; in
    /// that case `call` is dropped, abandoned while it waits for that
    /// message, and what the hold found is returned.
    pub async fn run_or_abandon<T>(&self, call: impl Future<Output = T>) -> Result<T, Stopped> {
        let mut call = std::pin::pin!(call);
        let mut stopped = std::pin::pin!(self.stopped());
        std::future::poll_fn(|cx| {
            if let std::task::Poll::Ready(s) = stopped.as_mut().poll(cx) {
                return std::task::Poll::Ready(Err(s));
            }
            call.as_mut().poll(cx).map(Ok)
        })
        .await
    }

    /// Waits until at most `n` proxied connections are open, so that every
    /// other connection has closed on both sides.
    pub async fn wait_until_open(&self, n: usize) {
        loop {
            let mut closed = std::pin::pin!(self.control.closed.notified());
            closed.as_mut().enable();
            if *self
                .control
                .open
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                <= n
            {
                return;
            }
            closed.await;
        }
    }

    /// Disarms the hold and lets a stopped message proceed.
    pub fn release(&self) {
        let mut s = self.control.lock();
        s.armed = None;
        s.generation += 1;
        if s.stopped.take().is_none() {
            // Nothing stopped, so no notification is pending; one stored
            // now would end the next wait at once.
            drop(s);
            return;
        }
        drop(s);
        self.control.released.notify_waiters();
    }
}

/// Forwards one connection both ways, message by message, until either side
/// closes.
async fn proxy(client: TcpStream, server: TcpStream, control: Arc<Control>) {
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let in_transaction = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let up = pump(
        client_read,
        server_write,
        Direction::ToServer,
        control.clone(),
        in_transaction.clone(),
    );
    let down = pump(
        server_read,
        client_write,
        Direction::ToClient,
        control,
        in_transaction,
    );
    tokio::join!(up, down);
}

/// Forwards messages from `from` to `to`. The first message to the server is
/// the untyped startup message; every other message is a type byte and a
/// length-prefixed body.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    direction: Direction,
    control: Arc<Control>,
    in_transaction: Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let mut typed = direction == Direction::ToClient;
    loop {
        let mut message = Vec::new();
        if typed {
            let mut tag = [0u8; 1];
            if from.read_exact(&mut tag).await.is_err() {
                break;
            }
            message.push(tag[0]);
        }
        let mut len = [0u8; 4];
        if from.read_exact(&mut len).await.is_err() {
            break;
        }
        message.extend_from_slice(&len);
        let body_len = usize::try_from(i32::from_be_bytes(len))
            .ok()
            .and_then(|l| l.checked_sub(4))
            .expect("a Postgres message length is at least 4");
        let start = message.len();
        message.resize(start + body_len, 0);
        if from.read_exact(&mut message[start..]).await.is_err() {
            break;
        }
        if !typed {
            const SSL_REQUEST: [u8; 4] = 80_877_103i32.to_be_bytes();
            assert!(
                message[4..8] != SSL_REQUEST,
                "the proxy does not carry TLS; connect without it"
            );
            typed = true;
        }
        control.pass(direction, in_transaction.load(SeqCst)).await;
        if direction == Direction::ToServer && message[0] == b'Q' {
            let query = String::from_utf8_lossy(&message[5..]).to_ascii_uppercase();
            if query.starts_with("BEGIN") {
                in_transaction.store(true, SeqCst);
            } else if query.starts_with("COMMIT") || query.starts_with("ROLLBACK") {
                in_transaction.store(false, SeqCst);
            }
        }
        if to.write_all(&message).await.is_err() {
            break;
        }
    }
    // Pass the close on, so the other side sees the connection end.
    let _ = to.shutdown().await;
}
