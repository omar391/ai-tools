#![cfg(unix)]

use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use codex_rotate_runtime::ipc::{daemon_socket_path, subscribe, RuntimeCapabilities};

static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn env_mutex() -> &'static Mutex<()> {
    ENV_MUTEX.get_or_init(|| Mutex::new(()))
}

fn cli_binary() -> &'static str {
    env!("CARGO_BIN_EXE_codex-rotate")
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    PathBuf::from("/tmp").join(format!("{prefix}-{stamp}"))
}

struct EnvGuard {
    home: Option<OsString>,
    rotate_home: Option<OsString>,
    codex_home: Option<OsString>,
    debug_port: Option<OsString>,
}

impl EnvGuard {
    fn set(rotate_home: &Path, codex_home: &Path, debug_port: u16) -> Self {
        let previous = Self {
            home: std::env::var_os("HOME"),
            rotate_home: std::env::var_os("CODEX_ROTATE_HOME"),
            codex_home: std::env::var_os("CODEX_HOME"),
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
    fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind dummy cdp listener")?;
        listener
            .set_nonblocking(true)
            .context("configure dummy cdp listener")?;
        let port = listener.local_addr().context("dummy cdp local addr")?.port();
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

struct CommandResult {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run_cli(args: &[&str], rotate_home: &Path, codex_home: &Path, debug_port: u16) -> Result<CommandResult> {
    let output = Command::new(cli_binary())
        .args(args)
        .env("CODEX_ROTATE_HOME", rotate_home)
        .env("CODEX_HOME", codex_home)
        .env("CODEX_ROTATE_DEBUG_PORT", debug_port.to_string())
        .output()
        .with_context(|| format!("run {}", cli_binary()))?;
    Ok(CommandResult {
        code: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

fn spawn_daemon(rotate_home: &Path, codex_home: &Path, debug_port: u16) -> Result<Child> {
    Command::new(cli_binary())
        .arg("daemon")
        .arg("run")
        .env("CODEX_ROTATE_HOME", rotate_home)
        .env("CODEX_HOME", codex_home)
        .env("CODEX_ROTATE_DEBUG_PORT", debug_port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn {} daemon", cli_binary()))
}

fn wait_for_socket(child: &mut Child, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let socket_path = daemon_socket_path()?;
    while Instant::now() < deadline {
        if socket_path.exists() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            let mut stdout = String::new();
            let mut stderr = String::new();
            if let Some(mut stream) = child.stdout.take() {
                let _ = stream.read_to_string(&mut stdout);
            }
            if let Some(mut stream) = child.stderr.take() {
                let _ = stream.read_to_string(&mut stderr);
            }
            return Err(anyhow::anyhow!(
                "Daemon exited before creating {} (status: {}). stdout: {} stderr: {}",
                socket_path.display(),
                status,
                stdout.trim(),
                stderr.trim()
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(anyhow::anyhow!(
        "Timed out waiting for daemon socket {}.",
        socket_path.display()
    ))
}

fn terminate_child(child: &mut Child) -> Result<()> {
    let pid = child.id().to_string();
    let _ = Command::new("kill").arg("-TERM").arg(&pid).status();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(_status) = child.try_wait()? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            child.wait().ok();
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn normalized(value: &str) -> String {
    value.replace("\r\n", "\n").trim().to_string()
}

#[test]
fn empty_home_cli_matches_daemon_proxy_and_streams_snapshots() -> Result<()> {
    let _guard = env_mutex().lock().expect("env mutex");
    let sandbox = unique_temp_dir("codex-rotate-e2e");
    let rotate_home = sandbox.join("rotate-home");
    let codex_home = sandbox.join("codex-home");
    fs::create_dir_all(&rotate_home)?;
    fs::create_dir_all(&codex_home)?;

    let dummy_cdp = DummyCdpServer::start()?;
    let _env = EnvGuard::set(&rotate_home, &codex_home, dummy_cdp.port);

    let direct_status = run_cli(&["status"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let direct_list = run_cli(&["list"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let direct_next = run_cli(&["next"], &rotate_home, &codex_home, dummy_cdp.port)?;

    let mut daemon = spawn_daemon(&rotate_home, &codex_home, dummy_cdp.port)?;
    wait_for_socket(&mut daemon, Duration::from_secs(10))?;

    let mut subscription = subscribe()?;
    let snapshot = subscription.recv()?;
    assert_eq!(snapshot.capabilities, RuntimeCapabilities::current());

    let proxied_status = run_cli(&["status"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let proxied_list = run_cli(&["list"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let proxied_next = run_cli(&["next"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let second_daemon = run_cli(&["daemon", "run"], &rotate_home, &codex_home, dummy_cdp.port)?;

    assert_eq!(direct_status.code, proxied_status.code);
    assert_eq!(normalized(&direct_status.stdout), normalized(&proxied_status.stdout));
    assert_eq!(normalized(&direct_status.stderr), normalized(&proxied_status.stderr));

    assert_eq!(direct_list.code, proxied_list.code);
    assert_eq!(normalized(&direct_list.stdout), normalized(&proxied_list.stdout));
    assert_eq!(normalized(&direct_list.stderr), normalized(&proxied_list.stderr));

    assert_eq!(direct_next.code, proxied_next.code);
    assert_eq!(normalized(&direct_next.stdout), normalized(&proxied_next.stdout));
    assert_eq!(normalized(&direct_next.stderr), normalized(&proxied_next.stderr));

    assert_eq!(second_daemon.code, 0);
    assert_eq!(
        normalized(&second_daemon.stdout),
        "Codex Rotate daemon is already running."
    );

    drop(subscription);
    terminate_child(&mut daemon)?;
    fs::remove_dir_all(&sandbox).ok();
    Ok(())
}
