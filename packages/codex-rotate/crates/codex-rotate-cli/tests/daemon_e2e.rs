#![cfg(unix)]

use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
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

fn find_binary(binary_name: &str) -> Result<PathBuf> {
    let path = std::env::var_os("PATH").ok_or_else(|| anyhow::anyhow!("PATH is not set."))?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(anyhow::anyhow!("Could not find {} in PATH.", binary_name))
}

struct EnvGuard {
    home: Option<OsString>,
    rotate_home: Option<OsString>,
    codex_home: Option<OsString>,
    debug_port: Option<OsString>,
    disable_managed_launch: Option<OsString>,
    disable_local_refresh: Option<OsString>,
}

impl EnvGuard {
    fn set(rotate_home: &Path, codex_home: &Path, debug_port: u16) -> Self {
        let previous = Self {
            home: std::env::var_os("HOME"),
            rotate_home: std::env::var_os("CODEX_ROTATE_HOME"),
            codex_home: std::env::var_os("CODEX_HOME"),
            debug_port: std::env::var_os("CODEX_ROTATE_DEBUG_PORT"),
            disable_managed_launch: std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH"),
            disable_local_refresh: std::env::var_os("CODEX_ROTATE_DISABLE_LOCAL_REFRESH"),
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
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
            std::env::set_var("CODEX_ROTATE_DISABLE_LOCAL_REFRESH", "1");
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
        restore_var(
            "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
            self.disable_managed_launch.take(),
        );
        restore_var(
            "CODEX_ROTATE_DISABLE_LOCAL_REFRESH",
            self.disable_local_refresh.take(),
        );
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

struct ExtraEnvGuard {
    values: Vec<(&'static str, Option<OsString>)>,
}

impl ExtraEnvGuard {
    fn set(values: &[(&'static str, OsString)]) -> Self {
        let mut previous = Vec::with_capacity(values.len());
        for (name, value) in values {
            previous.push((*name, std::env::var_os(name)));
            unsafe {
                std::env::set_var(name, value);
            }
        }
        Self { values: previous }
    }
}

impl Drop for ExtraEnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.values.drain(..) {
            restore_var(name, value);
        }
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
        let port = listener
            .local_addr()
            .context("dummy cdp local addr")?
            .port();
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

struct DaemonCreateHarness {
    sandbox: PathBuf,
    rotate_home: PathBuf,
    codex_home: PathBuf,
    debug_port: u16,
    _dummy_cdp: DummyCdpServer,
    _env: EnvGuard,
    _extra_env: ExtraEnvGuard,
    daemon: Child,
    create: Child,
    bridge_child_pid: u32,
}

impl DaemonCreateHarness {
    fn start(prefix: &str) -> Result<Self> {
        let sandbox = unique_temp_dir(prefix);
        let rotate_home = sandbox.join("rotate-home");
        let codex_home = sandbox.join("codex-home");
        fs::create_dir_all(&rotate_home)?;
        fs::create_dir_all(&codex_home)?;

        let fast_browser_runtime = sandbox.join("fast-browser-runtime.sh");
        write_executable(
            &fast_browser_runtime,
            "#!/bin/sh\nset -eu\nif [ \"${2-}\" = \"inspect-profiles\" ]; then\n  printf '%s\\n' '{\"managedProfiles\":{\"default\":\"dev-1\",\"profiles\":[{\"name\":\"dev-1\"}]}}'\n  exit 0\nfi\nprintf 'unexpected fast-browser runtime args: %s\\n' \"$*\" >&2\nexit 1\n",
        )?;

        let automation_bridge = sandbox.join("automation-bridge.py");
        fs::write(
            &automation_bridge,
            r#"import json
import os
import subprocess
import sys
import time

def respond(payload, code=0):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()
    sys.exit(code)

def request_file():
    args = sys.argv[1:]
    index = args.index("--request-file")
    return args[index + 1]

with open(request_file(), "r", encoding="utf-8") as handle:
    request = json.load(handle)

command = request["command"]
if command == "prepare-account-secret-ref":
    respond(
        {
            "ok": True,
            "result": {
                "type": "secret_ref",
                "store": "bitwarden-cli",
                "object_id": "test-secret",
            },
        }
    )
elif command == "delete-account-secret-ref":
    respond({"ok": True, "result": True})
elif command == "reset-managed-runtime":
    respond({"ok": True, "result": {"ok": True}})
elif command == "complete-codex-login-attempt":
    pid_file = os.environ["CODEX_ROTATE_TEST_CHILD_PID_FILE"]
    subprocess.Popen(
        [
            "/bin/sh",
            "-lc",
            f"trap 'exit 0' TERM INT; echo $$ > '{pid_file}'; while true; do sleep 1; done",
        ]
    )
    while True:
        time.sleep(1)
else:
    respond(
        {
            "ok": False,
            "error": {"message": f"unsupported automation bridge command: {command}"},
        },
        1,
    )
"#,
        )?;

        let child_pid_file = sandbox.join("bridge-child.pid");
        let python3 = find_binary("python3")?;
        let dummy_cdp = DummyCdpServer::start()?;
        let debug_port = dummy_cdp.port;
        let env = EnvGuard::set(&rotate_home, &codex_home, debug_port);
        let extra_env = ExtraEnvGuard::set(&[
            (
                "CODEX_ROTATE_AUTOMATION_BRIDGE",
                automation_bridge.as_os_str().to_os_string(),
            ),
            ("NODE_BIN", python3.as_os_str().to_os_string()),
            (
                "CODEX_ROTATE_FAST_BROWSER_RUNTIME",
                fast_browser_runtime.as_os_str().to_os_string(),
            ),
            (
                "CODEX_ROTATE_TEST_CHILD_PID_FILE",
                child_pid_file.as_os_str().to_os_string(),
            ),
        ]);

        let mut daemon = spawn_daemon(&rotate_home, &codex_home, debug_port)?;
        wait_for_socket(&mut daemon, Duration::from_secs(10))?;

        let mut create = configured_command(&rotate_home, &codex_home, debug_port)
            .arg("create")
            .arg("--force")
            .arg("--profile")
            .arg("dev-1")
            .arg("--base-email")
            .arg("dev.{n}@astronlab.com")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn {} create --force", cli_binary()))?;

        let bridge_child_pid =
            wait_for_pid_file_from_child(&child_pid_file, &mut create, Duration::from_secs(10))?;
        assert!(
            process_is_running(bridge_child_pid),
            "expected bridge child {} to be running",
            bridge_child_pid
        );

        Ok(Self {
            sandbox,
            rotate_home,
            codex_home,
            debug_port,
            _dummy_cdp: dummy_cdp,
            _env: env,
            _extra_env: extra_env,
            daemon,
            create,
            bridge_child_pid,
        })
    }

    fn assert_daemon_still_running(&self) -> Result<()> {
        let daemon_check = run_cli(
            &["daemon"],
            &self.rotate_home,
            &self.codex_home,
            self.debug_port,
        )?;
        assert_eq!(daemon_check.code, 0);
        assert_eq!(
            normalized(&daemon_check.stdout),
            "Codex Rotate daemon is already running."
        );
        Ok(())
    }

    fn expect_cancel_on_signal(&mut self, signal: &str, expected_signal: i32) -> Result<()> {
        send_signal(self.create.id(), signal)?;

        let status = wait_for_exit(&mut self.create, Duration::from_secs(10))?;
        assert!(
            !status.success(),
            "{signal} should stop the CLI instead of completing successfully"
        );
        assert!(
            status.signal() == Some(expected_signal) || status.code().is_some_and(|code| code != 0),
            "expected {signal}/non-zero exit, got {status:?}"
        );

        wait_for_process_exit(self.bridge_child_pid, Duration::from_secs(10))?;
        self.assert_daemon_still_running()?;
        Ok(())
    }
}

impl Drop for DaemonCreateHarness {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-CONT", &self.create.id().to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = terminate_child(&mut self.create);
        let _ = terminate_child(&mut self.daemon);
        fs::remove_dir_all(&self.sandbox).ok();
    }
}

fn configured_command(rotate_home: &Path, codex_home: &Path, debug_port: u16) -> Command {
    let mut command = Command::new(cli_binary());
    command
        .env("CODEX_ROTATE_HOME", rotate_home)
        .env("CODEX_HOME", codex_home)
        .env("CODEX_ROTATE_DEBUG_PORT", debug_port.to_string());
    command
}

fn run_cli(
    args: &[&str],
    rotate_home: &Path,
    codex_home: &Path,
    debug_port: u16,
) -> Result<CommandResult> {
    let output = configured_command(rotate_home, codex_home, debug_port)
        .args(args)
        .output()
        .with_context(|| format!("run {}", cli_binary()))?;
    Ok(CommandResult {
        code: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

fn spawn_daemon(rotate_home: &Path, codex_home: &Path, debug_port: u16) -> Result<Child> {
    configured_command(rotate_home, codex_home, debug_port)
        .arg("daemon")
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
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(&pid)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
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

fn wait_for_exit(child: &mut Child, timeout: Duration) -> Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            child.wait().ok();
            return Err(anyhow::anyhow!(
                "Timed out waiting for child process {} to exit.",
                child.id()
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn process_is_running(process_id: u32) -> bool {
    Command::new("kill")
        .args(["-0", &process_id.to_string()])
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn send_signal(process_id: u32, signal: &str) -> Result<()> {
    let status = Command::new("kill")
        .args([signal, &process_id.to_string()])
        .status()
        .with_context(|| format!("send {signal} to process {process_id}"))?;
    if !status.success() {
        return Err(anyhow::anyhow!(
            "{signal} should succeed for process {process_id}"
        ));
    }
    Ok(())
}

fn read_child_output(child: &mut Child) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut stream) = child.stdout.take() {
        let _ = stream.read_to_string(&mut stdout);
    }
    if let Some(mut stream) = child.stderr.take() {
        let _ = stream.read_to_string(&mut stderr);
    }
    (stdout, stderr)
}

fn wait_for_pid_file_from_child(path: &Path, child: &mut Child, timeout: Duration) -> Result<u32> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(raw) = fs::read_to_string(path) {
            if let Ok(pid) = raw.trim().parse::<u32>() {
                return Ok(pid);
            }
        }
        if let Some(status) = child.try_wait()? {
            let (stdout, stderr) = read_child_output(child);
            return Err(anyhow::anyhow!(
                "Child exited before creating pid file {} (status: {}). stdout: {} stderr: {}",
                path.display(),
                status,
                stdout.trim(),
                stderr.trim()
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }

    child.kill().ok();
    child.wait().ok();
    let (stdout, stderr) = read_child_output(child);
    Err(anyhow::anyhow!(
        "Timed out waiting for pid file {}. stdout: {} stderr: {}",
        path.display(),
        stdout.trim(),
        stderr.trim()
    ))
}

fn wait_for_process_exit(process_id: u32, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_is_running(process_id) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(anyhow::anyhow!(
        "Timed out waiting for process {} to exit.",
        process_id
    ))
}

fn write_executable(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
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
    let direct_prev = run_cli(&["prev"], &rotate_home, &codex_home, dummy_cdp.port)?;

    let mut daemon = spawn_daemon(&rotate_home, &codex_home, dummy_cdp.port)?;
    wait_for_socket(&mut daemon, Duration::from_secs(10))?;

    let mut subscription = subscribe()?;
    let snapshot = subscription.recv()?;
    assert_eq!(snapshot.capabilities, RuntimeCapabilities::current());

    let proxied_status = run_cli(&["status"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let proxied_list = run_cli(&["list"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let proxied_next = run_cli(&["next"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let proxied_prev = run_cli(&["prev"], &rotate_home, &codex_home, dummy_cdp.port)?;
    let second_daemon = run_cli(&["daemon"], &rotate_home, &codex_home, dummy_cdp.port)?;

    assert_eq!(direct_status.code, proxied_status.code);
    assert_eq!(
        normalized(&direct_status.stdout),
        normalized(&proxied_status.stdout)
    );
    assert_eq!(
        normalized(&direct_status.stderr),
        normalized(&proxied_status.stderr)
    );

    assert_eq!(direct_list.code, proxied_list.code);
    assert_eq!(
        normalized(&direct_list.stdout),
        normalized(&proxied_list.stdout)
    );
    assert_eq!(
        normalized(&direct_list.stderr),
        normalized(&proxied_list.stderr)
    );

    assert_eq!(direct_next.code, proxied_next.code);
    assert_eq!(
        normalized(&direct_next.stdout),
        normalized(&proxied_next.stdout)
    );
    assert!(
        normalized(&proxied_next.stderr).ends_with(&normalized(&direct_next.stderr)),
        "expected proxied stderr to end with direct stderr.\ndirect:\n{}\n\nproxied:\n{}",
        normalized(&direct_next.stderr),
        normalized(&proxied_next.stderr),
    );

    assert_eq!(direct_prev.code, proxied_prev.code);
    assert_eq!(
        normalized(&direct_prev.stdout),
        normalized(&proxied_prev.stdout)
    );
    assert_eq!(
        normalized(&direct_prev.stderr),
        normalized(&proxied_prev.stderr)
    );

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

#[test]
fn ctrl_c_cancels_only_the_in_flight_daemon_create_request() -> Result<()> {
    let _guard = env_mutex().lock().expect("env mutex");
    let mut harness = DaemonCreateHarness::start("codex-rotate-cancel-int-e2e")?;
    harness.expect_cancel_on_signal("-INT", 2)?;
    Ok(())
}

#[test]
fn sigterm_cancels_only_the_in_flight_daemon_create_request() -> Result<()> {
    let _guard = env_mutex().lock().expect("env mutex");
    let mut harness = DaemonCreateHarness::start("codex-rotate-cancel-term-e2e")?;
    harness.expect_cancel_on_signal("-TERM", 15)?;
    Ok(())
}

#[test]
fn sigtstp_suspends_cli_without_canceling_daemon_create_request() -> Result<()> {
    let _guard = env_mutex().lock().expect("env mutex");
    let mut harness = DaemonCreateHarness::start("codex-rotate-cancel-tstp-e2e")?;

    send_signal(harness.create.id(), "-TSTP")?;
    thread::sleep(Duration::from_millis(500));

    assert!(
        harness.create.try_wait()?.is_none(),
        "SIGTSTP should suspend the CLI instead of exiting it"
    );
    assert!(
        process_is_running(harness.bridge_child_pid),
        "suspending the CLI should not cancel the in-flight bridge child {}",
        harness.bridge_child_pid
    );
    harness.assert_daemon_still_running()?;

    send_signal(harness.create.id(), "-CONT")?;
    harness.expect_cancel_on_signal("-INT", 2)?;
    Ok(())
}
