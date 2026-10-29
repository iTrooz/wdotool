//! Bounded, versioned request/response frames, one command per connection.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Command, ExitCode};

const VERSION: u32 = 1;
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Management never constructs a backend; Run is dispatched on the retained one.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Run(Command),
    #[cfg(feature = "recorder")]
    Replay {
        trace: String,
        speed: f64,
    },
    Status,
    Stop,
}

/// Keep command output separate from transport and backend errors.
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub status: i32,
}

impl Response {
    /// Render errors exactly once on the client's stderr, with failure status.
    pub fn error(error: impl std::fmt::Display) -> Self {
        Self {
            stdout: Vec::new(),
            stderr: format!("Error: {error}\n").into_bytes(),
            status: 1,
        }
    }

    /// Construct successful management replies without special client formatting.
    pub fn message(message: String) -> Self {
        Self {
            stdout: message.into_bytes(),
            stderr: Vec::new(),
            status: ExitCode::SUCCESS.0,
        }
    }
}

/// Normalize client-side resources; the server must never consume its own stdin.
pub fn prepare(command: Command) -> Result<Request> {
    Ok(match command {
        Command::Type {
            delay,
            file,
            clearmodifiers,
            text,
        } => Request::Run(Command::Type {
            delay,
            file: None,
            clearmodifiers,
            text: Some(crate::resolve_type_input(file, text)?),
        }),
        #[cfg(feature = "recorder")]
        Command::Replay { file, speed } => Request::Replay {
            trace: crate::replay::read_trace(&file)?,
            speed,
        },
        command => {
            validate(&command)?;
            Request::Run(command)
        }
    })
}

/// Validate again server-side: serialized commands do not pass through clap.
pub fn validate(command: &Command) -> Result<()> {
    match command {
        Command::Server { .. } | Command::Prime | Command::Diag { .. } => {
            bail!("command is local-only")
        }
        #[cfg(feature = "recorder")]
        Command::Record { .. } | Command::Replay { .. } => {
            bail!("command requires client-side preparation")
        }
        Command::Type { file: Some(_), .. } => bail!("type files must be read by the client"),
        Command::Type { text: None, .. } => bail!("type requires text"),
        Command::Mousemove {
            relative: true,
            output: Some(_),
            ..
        } => bail!("--output conflicts with --relative"),
        _ => Ok(()),
    }
}

/// Encode first so an oversized response never leaves a partial frame behind.
pub async fn write_frame<T: Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> Result<()> {
    let data = serde_json::to_vec(&(VERSION, value))?;
    if data.len() > MAX_FRAME {
        bail!("IPC frame exceeds {MAX_FRAME} bytes");
    }
    stream.write_u32(data.len() as u32).await?;
    stream.write_all(&data).await?;
    stream.flush().await?;
    Ok(())
}

/// Bound allocation before parsing and reject incompatible protocol versions.
pub async fn read_frame<T: DeserializeOwned>(stream: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    let length = stream.read_u32().await? as usize;
    if length > MAX_FRAME {
        bail!("IPC frame exceeds {MAX_FRAME} bytes");
    }
    let mut data = vec![0; length];
    stream.read_exact(&mut data).await?;
    let (version, value): (u32, T) = serde_json::from_slice(&data).context("invalid IPC frame")?;
    if version != VERSION {
        bail!("unsupported IPC version {version} (expected {VERSION})");
    }
    Ok(value)
}
