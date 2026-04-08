use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::paths::resolve_paths;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RuntimeCapabilities {
    pub managed_launch: bool,
    pub live_account_sync: bool,
    pub quota_watch: bool,
    pub thread_recovery: bool,
    pub create_automation: bool,
}

impl RuntimeCapabilities {
    pub fn current() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self {
                managed_launch: true,
                live_account_sync: true,
                quota_watch: true,
                thread_recovery: true,
                create_automation: true,
            }
        }

        #[cfg(not(target_os = "macos"))]
        {
            Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct StatusSnapshot {
    pub current_email: Option<String>,
    pub inventory_count: Option<usize>,
    pub inventory_active_slot: Option<usize>,
    pub current_plan: Option<String>,
    pub current_quota: Option<String>,
    pub current_quota_percent: Option<u8>,
    pub last_rotation_from_email: Option<String>,
    pub last_rotation_to_email: Option<String>,
    pub last_rotation_reason: Option<String>,
    pub last_message: Option<String>,
    pub next_tick_at: Option<String>,
    pub capabilities: RuntimeCapabilities,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateInvocation {
    pub alias: Option<String>,
    pub profile_name: Option<String>,
    pub base_email: Option<String>,
    pub force: bool,
    pub ignore_current: bool,
    pub restore_previous_auth_after_create: bool,
    pub require_usable_quota: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReloginInvocation {
    pub selector: String,
    pub allow_email_change: bool,
    pub logout_first: bool,
    pub manual_login: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum InvokeAction {
    Status,
    List,
    Add { alias: Option<String> },
    Next,
    Prev,
    Create { options: CreateInvocation },
    Relogin { options: ReloginInvocation },
    Remove { selector: String },
    Refresh,
    OpenManaged,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientRequest {
    Subscribe,
    Invoke { action: InvokeAction },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerMessage {
    Snapshot {
        snapshot: StatusSnapshot,
    },
    Result {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

#[cfg(unix)]
type LocalStream = std::os::unix::net::UnixStream;

pub fn daemon_socket_path() -> Result<PathBuf> {
    Ok(resolve_paths()?.daemon_socket)
}

pub fn daemon_is_reachable() -> bool {
    #[cfg(unix)]
    {
        daemon_socket_path()
            .ok()
            .and_then(|path| LocalStream::connect(path).ok())
            .is_some()
    }

    #[cfg(not(unix))]
    {
        false
    }
}

pub struct SnapshotSubscription {
    #[cfg(unix)]
    reader: BufReader<LocalStream>,
}

impl SnapshotSubscription {
    pub fn recv(&mut self) -> Result<StatusSnapshot> {
        #[cfg(unix)]
        {
            match read_message(&mut self.reader)? {
                ServerMessage::Snapshot { snapshot } => Ok(snapshot),
                ServerMessage::Result { error, .. } => Err(anyhow!(
                    "{}",
                    error.unwrap_or_else(|| "Daemon returned an unexpected response.".to_string())
                )),
            }
        }

        #[cfg(not(unix))]
        {
            Err(anyhow!(
                "Local daemon transport is unavailable on this platform."
            ))
        }
    }

    #[cfg(unix)]
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<StatusSnapshot>> {
        self.reader
            .get_mut()
            .set_read_timeout(Some(timeout))
            .context("Failed to configure daemon subscription timeout.")?;
        match read_message(&mut self.reader) {
            Ok(ServerMessage::Snapshot { snapshot }) => Ok(Some(snapshot)),
            Ok(ServerMessage::Result { error, .. }) => Err(anyhow!(
                "{}",
                error.unwrap_or_else(|| "Daemon returned an unexpected response.".to_string())
            )),
            Err(error) if is_timeout_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

pub fn subscribe() -> Result<SnapshotSubscription> {
    #[cfg(unix)]
    {
        let mut stream = connect()?;
        write_message(&mut stream, &ClientRequest::Subscribe)?;
        Ok(SnapshotSubscription {
            reader: BufReader::new(stream),
        })
    }

    #[cfg(not(unix))]
    {
        Err(anyhow!(
            "Local daemon transport is unavailable on this platform."
        ))
    }
}

pub fn invoke(action: InvokeAction) -> Result<String> {
    #[cfg(unix)]
    {
        let mut stream = connect()?;
        write_message(&mut stream, &ClientRequest::Invoke { action })?;
        let mut reader = BufReader::new(stream);
        match read_message(&mut reader)? {
            ServerMessage::Result {
                ok: true, output, ..
            } => Ok(output.unwrap_or_default()),
            ServerMessage::Result {
                ok: false, error, ..
            } => Err(anyhow!(
                "{}",
                error.unwrap_or_else(|| "Daemon request failed.".to_string())
            )),
            ServerMessage::Snapshot { .. } => Err(anyhow!(
                "Daemon returned a snapshot when a command response was expected."
            )),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = action;
        Err(anyhow!(
            "Local daemon transport is unavailable on this platform."
        ))
    }
}

#[cfg(unix)]
fn connect() -> Result<LocalStream> {
    let socket = daemon_socket_path()?;
    LocalStream::connect(&socket)
        .with_context(|| format!("Failed to connect to {}.", socket.display()))
}

#[cfg(unix)]
pub fn write_message(stream: &mut LocalStream, message: &impl Serialize) -> Result<()> {
    let raw = serde_json::to_string(message)?;
    stream.write_all(raw.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

#[cfg(unix)]
pub fn read_request(reader: &mut BufReader<LocalStream>) -> Result<ClientRequest> {
    let mut line = String::new();
    let read = reader.read_line(&mut line)?;
    if read == 0 {
        return Err(anyhow!("Daemon client closed the connection."));
    }
    serde_json::from_str(line.trim()).context("Failed to decode daemon request.")
}

#[cfg(unix)]
pub fn read_message(reader: &mut BufReader<LocalStream>) -> Result<ServerMessage> {
    let mut line = String::new();
    let read = reader.read_line(&mut line)?;
    if read == 0 {
        return Err(anyhow!("Daemon closed the connection."));
    }
    serde_json::from_str(line.trim()).context("Failed to decode daemon response.")
}

#[cfg(unix)]
fn is_timeout_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|value| value.downcast_ref::<std::io::Error>())
        .any(|value| {
            matches!(
                value.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            )
        })
}
