#![cfg(unix)]

use codex_rotate_runtime::ipc::{daemon_is_reachable, invoke, InvokeAction, RuntimeCapabilities};
use codex_rotate_tray::{ensure_daemon_running, spawn_subscription_loop_controlled};
use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static BUILD_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static BUILT_CLI: OnceLock<PathBuf> = OnceLock::new();

fn env_mutex() -> &'static Mutex<()> {
    ENV_MUTEX.get_or_init(|| Mutex::new(()))
}

fn build_mutex() -> &'static Mutex<()> {
    BUILD_MUTEX.get_or_init(|| Mutex::new(()))
}

fn lock_unpoisoned<T>(mutex: &'static Mutex<T>) -> std::sync::MutexGuard<'static, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    PathBuf::from("/tmp").join(format!("{prefix}-{stamp}"))
}

fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn built_cli_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = BUILT_CLI.get() {
        return Ok(path.clone());
    }

    let _guard = build_mutex().lock().expect("build mutex");
    if let Some(path) = BUILT_CLI.get() {
        return Ok(path.clone());
    }

    let workspace_root = workspace_root();
    let manifest_path = workspace_root.join("Cargo.toml");
    let status = Command::new("cargo")
        .arg("build")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("-p")
        .arg("codex-rotate-cli")
        .arg("--bin")
        .arg("codex-rotate")
        .status()?;
    if !status.success() {
        return Err(format!(
            "Failed to build codex-rotate via {}.",
            manifest_path.display()
        )
        .into());
    }

    let path = workspace_root.join("target/debug/codex-rotate");
    BUILT_CLI
        .set(path.clone())
        .map_err(|_| "Failed to cache built CLI path.")?;
    Ok(path)
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn write_cli_wrapper(
    path: &Path,
    cli_binary: &Path,
    pid_file: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let script = format!(
        "#!/bin/sh\nset -eu\nif [ \"${{1-}}\" = \"daemon\" ] && [ \"${{2-}}\" = \"run\" ]; then\n  {cli} \"$@\" &\n  echo $! > {pid}\n  wait $!\nelse\n  exec {cli} \"$@\"\nfi\n",
        cli = shell_quote(cli_binary),
        pid = shell_quote(pid_file),
    );
    fs::write(path, script)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

fn wait_for<F>(timeout: Duration, mut condition: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err("Timed out waiting for condition.".into())
}

fn recv_snapshot_with_capabilities(
    receiver: &mpsc::Receiver<codex_rotate_runtime::ipc::StatusSnapshot>,
    expected: &RuntimeCapabilities,
    timeout: Duration,
) -> Result<codex_rotate_runtime::ipc::StatusSnapshot, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let snapshot = receiver.recv_timeout(remaining)?;
        if &snapshot.capabilities == expected {
            return Ok(snapshot);
        }
    }
    Err("Timed out waiting for a daemon-backed snapshot.".into())
}

struct EnvGuard {
    home: Option<OsString>,
    rotate_home: Option<OsString>,
    codex_home: Option<OsString>,
    cli_bin: Option<OsString>,
    debug_port: Option<OsString>,
}

impl EnvGuard {
    fn set(rotate_home: &Path, codex_home: &Path, cli_bin: &Path, debug_port: u16) -> Self {
        let previous = Self {
            home: std::env::var_os("HOME"),
            rotate_home: std::env::var_os("CODEX_ROTATE_HOME"),
            codex_home: std::env::var_os("CODEX_HOME"),
            cli_bin: std::env::var_os("CODEX_ROTATE_CLI_BIN"),
            debug_port: std::env::var_os("CODEX_ROTATE_DEBUG_PORT"),
        };
        let fake_home = rotate_home
            .parent()
            .expect("rotate home parent")
            .to_path_buf();
        unsafe {
            std::env::set_var("HOME", fake_home);
            std::env::set_var("CODEX_ROTATE_HOME", rotate_home);
            std::env::set_var("CODEX_HOME", codex_home);
            std::env::set_var("CODEX_ROTATE_CLI_BIN", cli_bin);
            std::env::set_var("CODEX_ROTATE_DEBUG_PORT", debug_port.to_string());
        }
        previous
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        restore_var("HOME", self.home.take());
        restore_var("CODEX_ROTATE_HOME", self.rotate_home.take());
        restore_var("CODEX_HOME", self.codex_home.take());
        restore_var("CODEX_ROTATE_CLI_BIN", self.cli_bin.take());
        restore_var("CODEX_ROTATE_DEBUG_PORT", self.debug_port.take());
    }
}

fn restore_var(name: &str, value: Option<OsString>) {
    match value {
        Some(value) => unsafe {
            std::env::set_var(name, value);
        },
        None => unsafe {
            std::env::remove_var(name);
        },
    }
}

struct DummyCdpServer {
    shutdown: std::sync::mpsc::Sender<()>,
    handle: Option<std::thread::JoinHandle<()>>,
    port: u16,
}

impl DummyCdpServer {
    fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
        let handle = thread::spawn(move || loop {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0u8; 4096];
                    let read = stream.read(&mut buffer).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let body = if path.starts_with("/json/version") {
                        r#"{"Browser":"Dummy Codex","Protocol-Version":"1.3"}"#
                    } else if path.starts_with("/json/list") {
                        "[]"
                    } else {
                        "{}"
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => break,
            }
        });
        Ok(Self {
            shutdown: shutdown_tx,
            handle: Some(handle),
            port,
        })
    }
}

impl Drop for DummyCdpServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn kill_daemon_from_pid_file(pid_file: &Path) {
    let Ok(raw_pid) = fs::read_to_string(pid_file) else {
        return;
    };
    let pid = raw_pid.trim();
    if pid.is_empty() {
        return;
    }
    let _ = Command::new("kill").arg("-TERM").arg(pid).status();
}

#[test]
fn tray_shell_launches_real_daemon_binary() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = lock_unpoisoned(env_mutex());
    let sandbox = unique_temp_dir("codex-rotate-tray-launch");
    let rotate_home = sandbox.join("rotate-home");
    let codex_home = sandbox.join("codex-home");
    fs::create_dir_all(&rotate_home)?;
    fs::create_dir_all(&codex_home)?;

    let cli_binary = built_cli_binary()?;
    let wrapper = sandbox.join("codex-rotate-wrapper.sh");
    let pid_file = sandbox.join("daemon.pid");
    write_cli_wrapper(&wrapper, &cli_binary, &pid_file)?;

    let dummy_cdp = DummyCdpServer::start()?;
    let _env = EnvGuard::set(&rotate_home, &codex_home, &wrapper, dummy_cdp.port);

    ensure_daemon_running()?;
    wait_for(Duration::from_secs(10), daemon_is_reachable)?;
    assert!(rotate_home.join("daemon.sock").exists());

    kill_daemon_from_pid_file(&pid_file);
    fs::remove_dir_all(&sandbox).ok();
    Ok(())
}

#[test]
fn tray_shell_auto_starts_and_streams_real_daemon_snapshots(
) -> Result<(), Box<dyn std::error::Error>> {
    let _guard = lock_unpoisoned(env_mutex());
    let sandbox = unique_temp_dir("codex-rotate-tray-ipc");
    let rotate_home = sandbox.join("rotate-home");
    let codex_home = sandbox.join("codex-home");
    fs::create_dir_all(&rotate_home)?;
    fs::create_dir_all(&codex_home)?;

    let cli_binary = built_cli_binary()?;
    let wrapper = sandbox.join("codex-rotate-wrapper.sh");
    let pid_file = sandbox.join("daemon.pid");
    write_cli_wrapper(&wrapper, &cli_binary, &pid_file)?;

    let dummy_cdp = DummyCdpServer::start()?;
    let _env = EnvGuard::set(&rotate_home, &codex_home, &wrapper, dummy_cdp.port);

    let stop = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel();
    let handle = spawn_subscription_loop_controlled(stop.clone(), move |snapshot| {
        sender.send(snapshot).ok();
    });

    let expected_capabilities = RuntimeCapabilities::current();
    let first = recv_snapshot_with_capabilities(
        &receiver,
        &expected_capabilities,
        Duration::from_secs(20),
    )?;
    assert_eq!(first.capabilities, RuntimeCapabilities::current());
    wait_for(Duration::from_secs(10), daemon_is_reachable)?;

    let _ = invoke(InvokeAction::List)?;
    let second = recv_snapshot_with_capabilities(
        &receiver,
        &expected_capabilities,
        Duration::from_secs(10),
    )?;
    assert_eq!(second.capabilities, RuntimeCapabilities::current());

    stop.store(true, Ordering::Relaxed);
    let _ = invoke(InvokeAction::Status);
    handle.join().expect("join tray subscription loop");

    kill_daemon_from_pid_file(&pid_file);
    fs::remove_dir_all(&sandbox).ok();
    Ok(())
}
