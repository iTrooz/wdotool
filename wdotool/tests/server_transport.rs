//! Socket ownership and framing tests that never construct a desktop backend.

use std::fs;
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
use std::path::Path;

use tokio::io::{duplex, AsyncWriteExt};
use wdotool::server::{
    protocol::{self, Request},
    socket::BoundSocket,
};

/// Make the same private directory required of custom socket users.
fn directory() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

/// Refusal must leave the unrelated entry intact, not merely return an error.
fn refuses(path: &Path) {
    assert!(BoundSocket::bind(path).is_err());
    assert!(fs::symlink_metadata(path).is_ok());
}

/// Launchers must not replace files, symlinks, insecure paths, or active listeners.
#[tokio::test]
async fn unsafe_paths_and_duplicate_launch_are_rejected_without_deleting_files() {
    let dir = directory();
    let path = dir.path().join("control.sock");
    fs::write(&path, "keep me").unwrap();
    refuses(&path);
    assert_eq!(fs::read_to_string(&path).unwrap(), "keep me");
    fs::remove_file(&path).unwrap();
    let target = dir.path().join("target");
    fs::write(&target, "untouched").unwrap();
    symlink(&target, &path).unwrap();
    refuses(&path);
    assert_eq!(fs::read_to_string(&target).unwrap(), "untouched");
    fs::remove_file(&path).unwrap();
    let bound = BoundSocket::bind(&path).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
    refuses(&path);
    drop(bound);
    assert!(!path.exists());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(BoundSocket::bind(&path).is_err());
}

/// Only a disconnected Unix socket is eligible for stale cleanup.
#[tokio::test]
async fn stale_socket_is_replaced_but_drop_preserves_a_replacement_file() {
    let dir = directory();
    let path = dir.path().join("control.sock");
    let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
    drop(stale);
    let bound = BoundSocket::bind(&path).unwrap();
    fs::remove_file(&path).unwrap();
    fs::write(&path, "replacement").unwrap();
    drop(bound);
    assert_eq!(fs::read_to_string(&path).unwrap(), "replacement");
}

/// A symlink lock must not redirect writes or be removed by failed startup.
#[tokio::test]
async fn lock_symlink_is_rejected() {
    let dir = directory();
    let path = dir.path().join("control.sock");
    let target = dir.path().join("target");
    fs::write(&target, "keep").unwrap();
    symlink(&target, dir.path().join("control.sock.lock")).unwrap();
    assert!(BoundSocket::bind(&path).is_err());
    assert_eq!(fs::read_to_string(target).unwrap(), "keep");
}

/// Bound allocation, require a complete frame, and reject unsupported versions.
#[tokio::test]
async fn framing_rejects_oversize_truncated_and_wrong_version() {
    for data in [
        ((protocol::MAX_FRAME + 1) as u32).to_be_bytes().to_vec(),
        [20u32.to_be_bytes().as_slice(), b"[1,"].concat(),
        [13u32.to_be_bytes().as_slice(), br#"[99,"Status"]"#].concat(),
    ] {
        let (mut writer, mut reader) = duplex(128);
        writer.write_all(&data).await.unwrap();
        drop(writer);
        assert!(protocol::read_frame::<Request>(&mut reader).await.is_err());
    }
}

/// Neither help nor an unavailable remote server should construct a local backend.
#[test]
fn help_and_missing_server_are_safe_without_a_session_bus() {
    let dir = directory();
    for args in [
        vec!["server", "launch", "--help"],
        vec!["--remote", "key", "b"],
    ] {
        let help = args.contains(&"--help");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_wdotool"))
            .arg("--socket")
            .arg(dir.path().join("missing.sock"))
            .args(args)
            .env(
                "DBUS_SESSION_BUS_ADDRESS",
                "unix:path=/nonexistent-session-bus",
            )
            .output()
            .unwrap();
        assert_eq!(output.status.success(), help);
        if !help {
            assert!(String::from_utf8(output.stderr)
                .unwrap()
                .contains("server socket unavailable"));
        }
    }
}
