//! Explicit persistent input server. One worker owns command ordering and cleanup.

mod held;
pub mod protocol;
pub mod socket;

use std::future::Future;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use wdotool_core::detector::{self, BackendKind, Environment};
use wdotool_core::{Backend, DynBackend};

use crate::{DispatchCtx, ExitCode};
use held::HeldBackend;
use protocol::{Request, Response};
use socket::BoundSocket;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_INTERVAL: Duration = Duration::from_millis(100);
const OUTPUT_LIMIT: usize = 1024 * 1024;

/// A ready connection, enqueued only after its complete frame has been validated.
struct Job {
    stream: UnixStream,
    request: Request,
}

/// Reserve the socket before authorizing, preventing duplicate session startup.
pub async fn launch(path: &Path, forced: Option<BackendKind>) -> Result<()> {
    let bound = BoundSocket::bind(path)?;
    let env = Environment::detect();
    // Choose once, not detector's fallback loop: failed authorization must not
    // open another backend/session. Users can explicitly choose a different one.
    let kind = forced
        .or_else(|| detector::priority(&env).first().copied())
        .context("no backend available; select one with --backend")?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    };
    tokio::pin!(shutdown);
    let backend = {
        let build = detector::build(&env, Some(kind));
        tokio::pin!(build);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                backend = &mut build => break backend?,
                accepted = bound.listener.accept() => {
                    let (mut stream, _) = accepted?;
                    socket::check_peer(&stream)?;
                    let request = tokio::time::timeout(IO_TIMEOUT, protocol::read_frame::<Request>(&mut stream)).await;
                    let response = match request {
                        Ok(Ok(Request::Status)) => Response::message("starting: awaiting backend authorization\n".into()),
                        Ok(Ok(Request::Stop)) => {
                            let _ = send(&mut stream, &Response::message("startup cancelled\n".into())).await;
                            return Ok(());
                        }
                        _ => Response::error("server not ready; command not executed"),
                    };
                    let _ = send(&mut stream, &response).await;
                }
            }
        }
    };
    println!("ready");
    io::stdout().flush()?;
    run(bound, backend, env, shutdown).await
}

/// Exchange one request; leaving the write half open lets EOF mean cancellation.
pub async fn request(path: &Path, request: Request) -> Result<Response> {
    let mut stream = socket::connect(path).await?;
    tokio::time::timeout(IO_TIMEOUT, protocol::write_frame(&mut stream, &request)).await??;
    // No command timeout: type/replay can intentionally run for a long time.
    protocol::read_frame(&mut stream)
        .await
        .context("server disconnected; command may have partially executed (not retried)")
}

/// Replay the server's byte streams and preserve its command's numeric status.
pub fn print_response(response: Response) -> Result<ExitCode> {
    io::stdout().write_all(&response.stdout)?;
    io::stderr().write_all(&response.stderr)?;
    Ok(ExitCode(response.status))
}

/// Run a preconstructed backend, also allowing offline mock-backed lifecycle tests.
pub async fn run(
    bound: BoundSocket,
    backend: DynBackend,
    env: Environment,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let backend = Arc::new(HeldBackend::new(backend));
    let bound = Arc::new(bound);
    let (jobs_tx, mut jobs) = mpsc::channel::<Job>(32);
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel::<Option<std::result::Result<(), String>>>(None);
    let mut listener = tokio::spawn(listen(
        bound.clone(),
        jobs_tx,
        stop_tx,
        done_rx,
        backend.clone(),
    ));
    let mut health = tokio::time::interval(HEALTH_INTERVAL);
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            _ = stop_rx.changed() => break Ok(()),
            _ = health.tick() => {
                if let Err(err) = backend.check_health() { break Err(err.into()); }
            }
            result = &mut listener => break match result {
                Ok(result) => result,
                Err(err) => Err(err.into()),
            },
            Some(mut job) = jobs.recv() => {
                // Stop and backend-loss monitoring remain active during long commands.
                let mut extra = [0u8; 1];
                let mut shutting_down = false;
                let response = {
                    let execute = execute(&*backend, &env, job.request);
                    tokio::pin!(execute);
                    loop {
                        tokio::select! {
                            biased;
                            _ = &mut shutdown => { shutting_down = true; break Some(Response::error("server shutting down; command cancelled")); },
                            _ = stop_rx.changed() => { shutting_down = true; break Some(Response::error("server shutting down; command cancelled")); },
                            _ = health.tick() => {
                                if let Err(err) = backend.check_health() {
                                    break Some(Response::error(err));
                                }
                            }
                            // EOF or extra data invalidates this one-request connection.
                            _ = job.stream.read(&mut extra) => break None,
                            response = &mut execute => break Some(response),
                        }
                    }
                };
                let failed = response.as_ref().is_none_or(|r| r.status != 0);
                // Release before waiting on a slow reader of a failed command.
                let released = if failed { backend.release().await } else { Ok(()) };
                let sent = if let Some(response) = response {
                    send(&mut job.stream, &response).await.is_ok()
                } else { false };
                if let Err(err) = released { break Err(err.into()); }
                if !failed && !sent {
                    if let Err(err) = backend.release().await { break Err(err.into()); }
                }
                if shutting_down || *stop_rx.borrow() { break Ok(()); }
                if let Err(err) = backend.check_health() { break Err(err.into()); }
            }
        }
    };
    // Release input before responding to queued clients, including slow readers.
    jobs.close();
    let cleanup = backend
        .shutdown()
        .await
        .context("server input/session cleanup failed");
    done_tx.send_replace(Some(
        cleanup.as_ref().map(|_| ()).map_err(|e| e.to_string()),
    ));
    let mut rejected = JoinSet::new();
    while let Ok(mut job) = jobs.try_recv() {
        rejected.spawn(async move {
            let _ = send(
                &mut job.stream,
                &Response::error("server shutting down; command not executed"),
            )
            .await;
        });
    }
    while rejected.join_next().await.is_some() {}
    if !listener.is_finished() {
        let _ = tokio::time::timeout(IO_TIMEOUT + Duration::from_secs(1), &mut listener).await;
    }
    listener.abort();
    // Aborting is asynchronous; wait for the listener's socket guard to drop.
    if !listener.is_finished() {
        let _ = listener.await;
    }
    drop(bound);
    result.and(cleanup)
}

/// Accept independently of the worker so stop/status remain usable during replay.
async fn listen(
    bound: Arc<BoundSocket>,
    jobs: mpsc::Sender<Job>,
    stop: watch::Sender<bool>,
    mut done: watch::Receiver<Option<std::result::Result<(), String>>>,
    backend: Arc<HeldBackend>,
) -> Result<()> {
    let mut clients = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = done.changed() => break,
            Some(_) = clients.join_next(), if !clients.is_empty() => {},
            accepted = bound.listener.accept(), if clients.len() < 64 => {
                let (stream, _) = accepted?;
                clients.spawn(accept(stream, jobs.clone(), stop.clone(), done.clone(), backend.clone()));
            }
        }
    }
    let _ = tokio::time::timeout(IO_TIMEOUT, async {
        while clients.join_next().await.is_some() {}
    })
    .await;
    clients.abort_all();
    Ok(())
}

/// Slow or malformed clients cannot occupy the command worker while framing.
async fn accept(
    mut stream: UnixStream,
    jobs: mpsc::Sender<Job>,
    stop: watch::Sender<bool>,
    mut done: watch::Receiver<Option<std::result::Result<(), String>>>,
    backend: Arc<HeldBackend>,
) {
    let request = async {
        socket::check_peer(&stream)?;
        protocol::read_frame::<Request>(&mut stream).await
    };
    let request = match tokio::time::timeout(IO_TIMEOUT, request).await {
        Ok(Ok(request)) => request,
        result => {
            let _ = send(
                &mut stream,
                &Response::error(format!("invalid or incomplete request: {result:?}")),
            )
            .await;
            return;
        }
    };
    match request {
        Request::Status => {
            let response = match backend.check_health() {
                Ok(()) => Response::message(format!("running: {}\n", backend.name())),
                Err(err) => Response::error(err),
            };
            let _ = send(&mut stream, &response).await;
        }
        Request::Stop => {
            stop.send_replace(true);
            if done.borrow().is_none() {
                let _ = done.changed().await;
            }
            let response = match done.borrow().as_ref() {
                Some(Ok(())) => Response::message("stopped\n".into()),
                Some(Err(err)) => Response::error(err),
                None => Response::error("server ended before confirming cleanup"),
            };
            let _ = send(&mut stream, &response).await;
        }
        request => {
            if let Err(err) = jobs.try_send(Job { stream, request }) {
                let mut job = err.into_inner();
                let _ = send(
                    &mut job.stream,
                    &Response::error("server queue full or shutting down; command not executed"),
                )
                .await;
            }
        }
    }
}

/// Bound output at the writer, not only after a command has allocated it all.
#[derive(Default)]
struct Output(Vec<u8>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len() + bytes.len() > OUTPUT_LIMIT {
            return Err(io::Error::other(
                "server command output exceeds 1 MiB per stream",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reuse the normal dispatcher and writers, including nonzero success-path exits.
async fn execute(backend: &dyn Backend, env: &Environment, request: Request) -> Response {
    let mut stdout = Output::default();
    let mut stderr = Output::default();
    let result: Result<ExitCode> = async {
        match request {
            Request::Run(command) => {
                protocol::validate(&command)?;
                let mut ctx = DispatchCtx {
                    backend,
                    env,
                    stdout: &mut stdout,
                    stderr: &mut stderr,
                };
                Ok(crate::dispatch(&mut ctx, command).await?)
            }
            #[cfg(feature = "recorder")]
            Request::Replay { trace, speed } => {
                crate::replay::run_trace(backend, &trace, speed).await?;
                Ok(ExitCode::SUCCESS)
            }
            _ => bail!("management command cannot be dispatched"),
        }
    }
    .await;
    let status = match result {
        Ok(status) => status.0,
        Err(err) => {
            if writeln!(&mut stderr, "Error: {err}").is_err() {
                let suffix = b"\nError: server error output truncated\n";
                stderr.0.truncate(OUTPUT_LIMIT - suffix.len());
                stderr.0.extend_from_slice(suffix);
            }
            1
        }
    };
    Response {
        stdout: stdout.0,
        stderr: stderr.0,
        status,
    }
}

/// Bound slow-reader time; a failed delivery triggers conservative input release.
async fn send(stream: &mut UnixStream, response: &Response) -> Result<()> {
    tokio::time::timeout(IO_TIMEOUT, protocol::write_frame(stream, response)).await??;
    Ok(())
}
