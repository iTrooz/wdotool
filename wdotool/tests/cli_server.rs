//! Real Unix IPC and separate CLI processes against one offline backend.

mod server_support;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use clap::Parser;
use tokio::net::UnixStream;
use wdotool::{
    server::{
        self,
        protocol::{self, Request, Response},
        socket::BoundSocket,
    },
    Cli,
};
use wdotool_core::{backend::mock::BackendCall, detector::Environment, KeyDirection, MouseButton};

use server_support::TestBackend;

/// Run the server and scenario together, bounding failures without a desktop.
async fn with_server<F, Fut>(backend: TestBackend, scenario: F) -> anyhow::Result<()>
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    with_shutdown(backend, std::future::pending(), scenario).await
}

/// Inject the signal future without delivering a process-wide signal to tests.
async fn with_shutdown<F, Fut>(
    backend: TestBackend,
    shutdown: impl Future<Output = ()>,
    scenario: F,
) -> anyhow::Result<()>
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    let directory = tempfile::Builder::new().prefix("input-ipc-").tempdir()?;
    let path = directory.path().join("control.sock");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let bound = BoundSocket::bind(&path)?;
    let run = server::run(bound, Box::new(backend), Environment::default(), shutdown);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(run, scenario(path.clone()))
    })
    .await
    .expect("IPC scenario timed out");
    assert!(!path.exists(), "server socket removed on exit");
    result
}

/// Parse exactly like the CLI, then perform its client-side preparation.
async fn command(path: &Path, args: &[&str]) -> Response {
    let cli = Cli::try_parse_from(std::iter::once("wdotool").chain(args.iter().copied())).unwrap();
    server::request(path, protocol::prepare(cli.command).unwrap())
        .await
        .unwrap()
}

/// A subprocess blocks on IPC while the test runtime keeps the server running.
async fn process(path: &Path, args: &[&str]) -> std::process::Output {
    process_input(path, args, None).await
}

/// Give stdin to the client process, never to the server's own input stream.
async fn process_input(path: &Path, args: &[&str], input: Option<Vec<u8>>) -> std::process::Output {
    let path = path.to_owned();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        use std::process::Stdio;
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_wdotool"))
            .arg("--socket")
            .arg(path)
            .args(args)
            .env(
                "DBUS_SESSION_BUS_ADDRESS",
                "unix:path=/nonexistent-session-bus",
            )
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        if let Some(input) = input {
            child.stdin.take().unwrap().write_all(&input).unwrap();
        }
        child.wait_with_output().unwrap()
    })
    .await
    .unwrap()
}

/// Remote resources are loaded by the client; they remain usable after unlinking.
#[tokio::test]
async fn client_prepares_files_and_stdin_before_dispatch() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let file = path.with_file_name("text");
        std::fs::write(&file, "from file").unwrap();
        let cli =
            Cli::try_parse_from(["wdotool", "type", "--file", file.to_str().unwrap()]).unwrap();
        let request = protocol::prepare(cli.command).unwrap();
        std::fs::remove_file(file).unwrap();
        assert_eq!(server::request(&path, request).await.unwrap().status, 0);
        let response = process_input(
            &path,
            &["--remote", "type", "--file", "-"],
            Some(b"from stdin".to_vec()),
        )
        .await;
        assert!(response.status.success());
        let calls = observed.mock.calls();
        assert!(calls
            .iter()
            .any(|call| matches!(call, BackendCall::TypeText { text, .. } if text == "from file")));
        assert!(calls.iter().any(
            |call| matches!(call, BackendCall::TypeText { text, .. } if text == "from stdin")
        ));
        server::request(&path, Request::Stop).await.unwrap();
    })
    .await
    .unwrap();
}

/// Replay stdin follows the same preparation path and existing key dispatcher.
#[cfg(feature = "recorder")]
#[tokio::test]
async fn remote_replay_reads_client_stdin() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let trace = br#"[{"kind":"key","t_ms":0,"chord":"ctrl+b"}]"#.to_vec();
        let response = process_input(&path, &["--remote", "replay", "-"], Some(trace)).await;
        assert!(response.status.success(), "{:?}", response);
        assert_eq!(observed.mock.calls().len(), 3);
        server::request(&path, Request::Stop).await.unwrap();
    })
    .await
    .unwrap();
}

/// The same signal path used in production releases holds even while idle.
#[tokio::test]
async fn shutdown_signal_releases_idle_holds() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    let (signal, shutdown) = tokio::sync::oneshot::channel();
    with_shutdown(
        backend,
        async {
            let _ = shutdown.await;
        },
        |path| async move {
            assert_eq!(command(&path, &["keydown", "Control_L"]).await.status, 0);
            signal.send(()).unwrap();
        },
    )
    .await
    .unwrap();
    assert!(observed.mock.calls().contains(&BackendCall::Key {
        keysym: "Control_L".into(),
        dir: KeyDirection::Release
    }));
    assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
}

/// Revocation is observed without needing a new command to touch the backend.
#[tokio::test]
async fn idle_revocation_ends_server() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    let result = with_server(backend, |path| async move {
        assert_eq!(
            server::request(&path, Request::Status)
                .await
                .unwrap()
                .status,
            0
        );
        observed.lost.store(true, Ordering::SeqCst);
    })
    .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("authorization revoked"));
}

/// A stop acknowledgement must not claim success if backend cleanup failed.
#[tokio::test]
async fn stop_reports_cleanup_failure() {
    let mut backend = TestBackend::new();
    backend.fail_shutdown = true;
    let result = with_server(backend, |path| async move {
        let stopped = server::request(&path, Request::Stop).await.unwrap();
        assert_eq!(stopped.status, 1);
        assert!(String::from_utf8(stopped.stderr)
            .unwrap()
            .contains("cleanup failed"));
    })
    .await;
    assert!(result.unwrap_err().to_string().contains("cleanup failed"));
}

/// Every subprocess must use the same backend, including its output/status paths.
#[tokio::test]
async fn separate_cli_calls_reuse_backend_and_preserve_streams_and_status() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let status = process(&path, &["server", "status"]).await;
        assert!(status.status.success());
        assert_eq!(status.stdout, b"running: test-persistent\n");
        assert!(status.stderr.is_empty());
        let info = process(&path, &["--remote", "info"]).await;
        assert!(info.status.success());
        assert!(String::from_utf8(info.stdout)
            .unwrap()
            .contains("backend:  test-persistent"));
        assert!(process(&path, &["--remote", "key", "ctrl+b"])
            .await
            .status
            .success());
        assert!(process(&path, &["--remote", "mousemove", "10", "20"])
            .await
            .status
            .success());
        let search = process(&path, &["--remote", "search"]).await;
        assert_eq!(search.status.code(), Some(1));
        assert!(search.stdout.is_empty() && search.stderr.is_empty());
        let pointer = process(&path, &["--remote", "getmouselocation"]).await;
        assert_eq!(pointer.status.code(), Some(1));
        assert!(String::from_utf8(pointer.stderr)
            .unwrap()
            .contains("pointer position is unreadable"));
        let stopped = process(&path, &["server", "stop"]).await;
        assert!(stopped.status.success());
        assert_eq!(stopped.stdout, b"stopped\n");
    })
    .await
    .unwrap();
    assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
    assert!(observed.mock.calls().contains(&BackendCall::MouseMove {
        x: 10,
        y: 20,
        absolute: true
    }));
}

/// Hold the first command mid-chain and prove another cannot interleave its calls.
#[tokio::test]
async fn concurrent_requests_are_serialized_while_status_remains_responsive() {
    let mut backend = TestBackend::new();
    backend.block_a = true;
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let first = command(&path, &["key", "ctrl+a"]);
        let others = async {
            observed.started.notified().await;
            let second = command(&path, &["key", "b"]);
            let release = async {
                let status = server::request(&path, Request::Status).await.unwrap();
                assert_eq!(status.status, 0);
                assert_eq!(observed.mock.calls().len(), 2);
                observed.gate.add_permits(1);
            };
            let (second, ()) = tokio::join!(second, release);
            assert_eq!(second.status, 0);
        };
        let (first, ()) = tokio::join!(first, others);
        assert_eq!(first.status, 0);
        assert_eq!(
            observed.mock.calls(),
            vec![
                BackendCall::Key {
                    keysym: "Control_L".into(),
                    dir: KeyDirection::Press
                },
                BackendCall::Key {
                    keysym: "a".into(),
                    dir: KeyDirection::PressRelease
                },
                BackendCall::Key {
                    keysym: "Control_L".into(),
                    dir: KeyDirection::Release
                },
                BackendCall::Key {
                    keysym: "b".into(),
                    dir: KeyDirection::PressRelease
                },
            ]
        );
        server::request(&path, Request::Stop).await.unwrap();
    })
    .await
    .unwrap();
}

/// Explicit holds survive successful calls and are released on explicit stop.
#[tokio::test]
async fn shutdown_releases_cross_invocation_keys_and_buttons() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    let during = observed.clone();
    with_server(backend, |path| async move {
        assert_eq!(command(&path, &["keydown", "Control_L"]).await.status, 0);
        assert_eq!(command(&path, &["mousedown", "1"]).await.status, 0);
        assert_eq!(during.mock.calls().len(), 2);
        assert_eq!(
            server::request(&path, Request::Stop).await.unwrap().status,
            0
        );
    })
    .await
    .unwrap();
    assert!(observed.mock.calls().contains(&BackendCall::Key {
        keysym: "Control_L".into(),
        dir: KeyDirection::Release
    }));
    assert!(observed.mock.calls().contains(&BackendCall::MouseButton {
        btn: MouseButton::Left,
        dir: KeyDirection::Release
    }));
    assert_eq!(observed.shutdowns.load(Ordering::SeqCst), 1);
}

/// A partial chain failure returns its error and releases already pressed modifiers.
#[tokio::test]
async fn partial_failure_releases_input_and_server_accepts_next_command() {
    let backend = TestBackend::new();
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let failed = command(&path, &["key", "ctrl+a"]).await;
        assert_eq!(failed.status, 1);
        assert!(String::from_utf8(failed.stderr)
            .unwrap()
            .contains("injected partial-chain failure"));
        assert_eq!(command(&path, &["click", "1"]).await.status, 0);
        assert!(observed.mock.calls().contains(&BackendCall::Key {
            keysym: "Control_L".into(),
            dir: KeyDirection::Release
        }));
        server::request(&path, Request::Stop).await.unwrap();
    })
    .await
    .unwrap();
}

/// EOF cancels an active command; Stop can interrupt one without accepting more input.
#[tokio::test]
async fn client_disconnect_cancels_and_releases_then_stop_interrupts_next_command() {
    let mut backend = TestBackend::new();
    backend.block_a = true;
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let mut client = server::socket::connect(&path).await.unwrap();
        let request = protocol::prepare(
            Cli::try_parse_from(["wdotool", "key", "ctrl+a"])
                .unwrap()
                .command,
        )
        .unwrap();
        protocol::write_frame(&mut client, &request).await.unwrap();
        observed.started.notified().await;
        drop(client);
        assert_eq!(command(&path, &["click", "1"]).await.status, 0);
        assert!(observed.mock.calls().contains(&BackendCall::Key {
            keysym: "Control_L".into(),
            dir: KeyDirection::Release
        }));
        let active = command(&path, &["key", "ctrl+a"]);
        let stop = async {
            observed.started.notified().await;
            assert_eq!(
                server::request(&path, Request::Stop).await.unwrap().status,
                0
            );
        };
        let (active, ()) = tokio::join!(active, stop);
        assert_eq!(active.status, 1);
        assert!(String::from_utf8(active.stderr)
            .unwrap()
            .contains("cancelled"));
    })
    .await
    .unwrap();
}

/// Terminal backend loss interrupts a command and ends the server without reconnecting.
#[tokio::test]
async fn revocation_is_reported_and_cleans_up_backend() {
    let mut backend = TestBackend::new();
    backend.block_a = true;
    let observed = backend.clone();
    let result = with_server(backend, |path| async move {
        let active = command(&path, &["key", "ctrl+a"]);
        let revoke = async {
            observed.started.notified().await;
            observed.lost.store(true, Ordering::SeqCst);
        };
        let (active, ()) = tokio::join!(active, revoke);
        assert_eq!(active.status, 1);
        assert!(String::from_utf8(active.stderr)
            .unwrap()
            .contains("authorization revoked"));
    })
    .await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("authorization revoked"));
}

/// Wire clients cannot make the server read its own stdin or run management commands.
#[tokio::test]
async fn unprepared_and_malformed_requests_do_not_reach_backend() {
    use tokio::io::AsyncWriteExt;
    let backend = TestBackend::new();
    let observed = backend.clone();
    with_server(backend, |path| async move {
        let request = Request::Run(
            Cli::try_parse_from(["wdotool", "type", "--file", "-"])
                .unwrap()
                .command,
        );
        let response = server::request(&path, request).await.unwrap();
        assert_eq!(response.status, 1);
        let mut stream = UnixStream::connect(&path).await.unwrap();
        stream
            .write_u32((protocol::MAX_FRAME + 1) as u32)
            .await
            .unwrap();
        let response: Response = protocol::read_frame(&mut stream).await.unwrap();
        assert_eq!(response.status, 1);
        assert!(observed.mock.calls().is_empty());
        assert_eq!(command(&path, &["click", "1"]).await.status, 0);
        server::request(&path, Request::Stop).await.unwrap();
    })
    .await
    .unwrap();
}
