//! User-private socket paths, exclusive startup, and identity-checked cleanup.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rustix::fs::{flock, FlockOperation, OFlags};
use tokio::net::{UnixListener, UnixStream};

/// Resolve without touching any portal, requiring an explicitly private runtime.
pub fn path(override_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = override_path {
        return Ok(path);
    }
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .context("XDG_RUNTIME_DIR is not set; pass --socket in a private directory")?;
    let runtime = PathBuf::from(runtime);
    private_directory(&runtime)?;
    let directory = runtime.join("wdotool");
    match fs::DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    private_directory(&directory)?;
    Ok(directory.join("control.sock"))
}

/// Refuse symlinks and directories writable or searchable by other users.
fn private_directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)
        .with_context(|| format!("checking private directory {}", path.display()))?;
    if !meta.is_dir()
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o777 != 0o700
    {
        bail!(
            "{} must be a user-owned, non-symlink directory with mode 0700",
            path.display()
        );
    }
    Ok(())
}

/// Pin the pathname identity and keep the startup lock until after unlinking.
pub struct BoundSocket {
    pub listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
    _lock: File,
}

impl BoundSocket {
    /// Lock before probing so two launchers cannot remove each other's socket.
    pub fn bind(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        private_directory(parent)?;
        let mut lock_name = path.as_os_str().to_owned();
        lock_name.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(OFlags::NOFOLLOW.bits() as i32)
            .open(PathBuf::from(lock_name))?;
        let meta = lock.metadata()?;
        if !meta.is_file()
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.mode() & 0o777 != 0o600
            || meta.nlink() != 1
        {
            bail!("unsafe server lock file");
        }
        flock(&lock, FlockOperation::NonBlockingLockExclusive)
            .context("another server owns this socket (or is starting)")?;

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                validate_socket(path, &meta)?;
                match std::os::unix::net::UnixStream::connect(path) {
                    Ok(_) => bail!("a server is already listening at {}", path.display()),
                    Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {}
                    Err(e) => return Err(e).context("cannot verify stale socket"),
                }
                remove_owned(path, (meta.dev(), meta.ino()))?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        // The enclosing 0700 directory protects the socket even before chmod.
        let listener = UnixListener::bind(path)?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let meta = fs::symlink_metadata(path)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
            identity: (meta.dev(), meta.ino()),
            _lock: lock,
        })
    }
}

impl Drop for BoundSocket {
    /// Never unlink a replacement file, symlink, or another listener's socket.
    fn drop(&mut self) {
        let _ = remove_owned(&self.path, self.identity);
    }
}

/// Only an owned Unix socket is a candidate for connection or stale cleanup.
fn validate_socket(path: &Path, meta: &fs::Metadata) -> Result<()> {
    if !meta.file_type().is_socket() || meta.uid() != rustix::process::geteuid().as_raw() {
        bail!(
            "refusing non-socket, symlink or foreign-owned path {}",
            path.display()
        );
    }
    Ok(())
}

/// Check the inode again immediately before removing a socket we previously saw.
fn remove_owned(path: &Path, identity: (u64, u64)) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    validate_socket(path, &meta)?;
    if (meta.dev(), meta.ino()) != identity {
        bail!("socket path changed; refusing to remove it");
    }
    fs::remove_file(path)?;
    Ok(())
}

/// Verify both filesystem permissions and the connected server's credentials.
pub async fn connect(path: &Path) -> Result<UnixStream> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    private_directory(parent)?;
    let meta = fs::symlink_metadata(path)
        .context("server socket unavailable; run `wdotool server launch` first")?;
    validate_socket(path, &meta)?;
    if meta.mode() & 0o777 != 0o600 {
        bail!("server socket must have mode 0600");
    }
    let stream = UnixStream::connect(path)
        .await
        .context("server unavailable (no direct-mode fallback)")?;
    check_peer(&stream)?;
    Ok(stream)
}

/// Filesystem restrictions are supplemented by kernel-reported peer identity.
pub fn check_peer(stream: &UnixStream) -> Result<()> {
    if stream.peer_cred()?.uid() != rustix::process::geteuid().as_raw() {
        bail!("server IPC peer belongs to a different user");
    }
    Ok(())
}
