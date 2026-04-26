use super::*;
use anyhow::Context;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::BufReader;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::net::UnixListener;

#[cfg(unix)]
use codex_rotate_runtime::ipc::{
    daemon_socket_path, read_request, write_message, ClientRequest, ServerMessage,
};

static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{stamp}"))
}

fn with_rotate_home<T>(test: impl FnOnce() -> Result<T>) -> Result<T> {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let rotate_home = unique_temp_dir("codex-rotate-cli-tests");
    fs::create_dir_all(&rotate_home).expect("create rotate home");
    let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
    unsafe {
        std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
    }

    let result = test();

    match previous_rotate_home {
        Some(value) => unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", value);
        },
        None => unsafe {
            std::env::remove_var("CODEX_ROTATE_HOME");
        },
    }
    fs::remove_dir_all(&rotate_home).ok();
    result
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

fn with_env_var<T>(
    name: &str,
    value: Option<&OsStr>,
    test: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let previous = std::env::var_os(name);
    match value {
        Some(value) => unsafe { std::env::set_var(name, value) },
        None => unsafe { std::env::remove_var(name) },
    }
    let result = test();
    restore_var(name, previous);
    result
}

struct LocalCliBuildFixture {
    repo_root: PathBuf,
    binary_path: PathBuf,
    release_binary_path: PathBuf,
    tracked_source_path: PathBuf,
}

impl LocalCliBuildFixture {
    fn new(prefix: &str) -> Self {
        let repo_root = unique_temp_dir(prefix);
        let binary_path = repo_root.join("target").join("debug").join("codex-rotate");
        let release_binary_path = repo_root
            .join("target")
            .join("release")
            .join("codex-rotate");
        let tracked_source_path = repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("src")
            .join("lib.rs");

        fs::create_dir_all(binary_path.parent().expect("binary parent"))
            .expect("create debug target dir");
        fs::create_dir_all(release_binary_path.parent().expect("release binary parent"))
            .expect("create release target dir");
        fs::create_dir_all(
            tracked_source_path
                .parent()
                .expect("tracked source parent")
                .to_path_buf(),
        )
        .expect("create core src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-runtime")
                .join("src"),
        )
        .expect("create runtime src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli")
                .join("src"),
        )
        .expect("create cli src");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
        fs::write(repo_root.join("Cargo.lock"), "").expect("write lock");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-core")
                .join("Cargo.toml"),
            "",
        )
        .expect("write core cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-runtime")
                .join("Cargo.toml"),
            "",
        )
        .expect("write runtime cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli")
                .join("Cargo.toml"),
            "",
        )
        .expect("write cli cargo");
        fs::write(&binary_path, "").expect("write debug binary");

        Self {
            repo_root,
            binary_path,
            release_binary_path,
            tracked_source_path,
        }
    }

    fn mark_sources_stale(&self) {
        thread::sleep(Duration::from_secs(1) + Duration::from_millis(50));
        fs::write(&self.tracked_source_path, "pub fn changed() {}").expect("write newer source");
    }

    fn write_release_binary(&self) {
        fs::write(&self.release_binary_path, "").expect("write release binary");
    }
}

impl Drop for LocalCliBuildFixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.repo_root).ok();
    }
}

#[cfg(unix)]
fn write_executable_script(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write script");
    let mut permissions = fs::metadata(path).expect("script metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("set script permissions");
}

#[cfg(unix)]
fn spawn_proxy_server(response_output: &str) -> std::thread::JoinHandle<Result<ClientRequest>> {
    let socket_path = daemon_socket_path().expect("daemon socket path");
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent).expect("create daemon socket dir");
    }
    let listener = UnixListener::bind(&socket_path).expect("bind daemon socket");
    let response_output = response_output.to_string();
    thread::spawn(move || -> Result<ClientRequest> {
        loop {
            let (mut stream, _) = listener.accept().context("accept request")?;
            let mut reader = BufReader::new(stream.try_clone()?);
            let request = match read_request(&mut reader) {
                Ok(request) => request,
                Err(_) => continue,
            };
            match request {
                ClientRequest::Subscribe => {
                    write_message(
                        &mut stream,
                        &ServerMessage::Snapshot {
                            snapshot: Box::new(StatusSnapshot::default()),
                        },
                    )?;
                }
                ClientRequest::Invoke { .. } => {
                    write_message(
                        &mut stream,
                        &ServerMessage::Result {
                            ok: true,
                            output: Some(response_output),
                            error: None,
                        },
                    )?;
                    fs::remove_file(&socket_path).ok();
                    return Ok(request);
                }
            }
        }
    })
}

#[cfg(unix)]
fn spawn_proxy_error_server(
    response_error: &str,
) -> std::thread::JoinHandle<Result<ClientRequest>> {
    let socket_path = daemon_socket_path().expect("daemon socket path");
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent).expect("create daemon socket dir");
    }
    let listener = UnixListener::bind(&socket_path).expect("bind daemon socket");
    let response_error = response_error.to_string();
    thread::spawn(move || -> Result<ClientRequest> {
        loop {
            let (mut stream, _) = listener.accept().context("accept request")?;
            let mut reader = BufReader::new(stream.try_clone()?);
            let request = match read_request(&mut reader) {
                Ok(request) => request,
                Err(_) => continue,
            };
            match request {
                ClientRequest::Subscribe => {
                    write_message(
                        &mut stream,
                        &ServerMessage::Snapshot {
                            snapshot: Box::new(StatusSnapshot::default()),
                        },
                    )?;
                }
                ClientRequest::Invoke { .. } => {
                    write_message(
                        &mut stream,
                        &ServerMessage::Result {
                            ok: false,
                            output: None,
                            error: Some(response_error),
                        },
                    )?;
                    fs::remove_file(&socket_path).ok();
                    return Ok(request);
                }
            }
        }
    })
}

#[cfg(unix)]
fn spawn_reachable_daemon() -> std::thread::JoinHandle<Result<()>> {
    let socket_path = daemon_socket_path().expect("daemon socket path");
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent).expect("create daemon socket dir");
    }
    let listener = UnixListener::bind(&socket_path).expect("bind daemon socket");
    thread::spawn(move || -> Result<()> {
        let (_probe_stream, _) = listener.accept().context("accept probe")?;
        fs::remove_file(&socket_path).ok();
        Ok(())
    })
}

#[test]
fn add_alias_parser_accepts_trimmed_optional_alias() {
    assert_eq!(
        parse_add_alias(&["  work  ".to_string()]).expect("add alias"),
        Some("work".to_string())
    );
    assert_eq!(parse_add_alias(&[]).expect("empty alias"), None);
}

#[test]
fn run_with_timeout_returns_none_when_operation_blocks_past_deadline() {
    let result = run_with_timeout(Duration::from_millis(10), || {
        thread::sleep(Duration::from_millis(50));
        7
    });
    assert_eq!(result, None);
}

#[test]
fn create_parser_preserves_flags_and_alias() {
    let options = parse_internal_create_options(&[
        "bench".to_string(),
        "--force".to_string(),
        "--ignore-current".to_string(),
        "--restore-auth".to_string(),
        "--profile".to_string(),
        "dev-1".to_string(),
        "--template".to_string(),
        "dev.{n}@astronlab.com".to_string(),
    ])
    .expect("create options");

    assert_eq!(options.alias.as_deref(), Some("bench"));
    assert_eq!(options.profile_name.as_deref(), Some("dev-1"));
    assert_eq!(options.template.as_deref(), Some("dev.{n}@astronlab.com"));
    assert!(options.force);
    assert!(options.ignore_current);
    assert!(options.restore_previous_auth_after_create);
    assert_eq!(options.source, CreateCommandSource::Manual);
    assert!(!options.require_usable_quota);
}

#[test]
fn public_create_parser_rejects_internal_flags() {
    let error = parse_public_create_options(&[
        "--ignore-current".to_string(),
        "--restore-auth".to_string(),
    ])
    .expect_err("public create should reject internal flags");
    assert!(error.to_string().contains("Unknown create option"));
}

#[test]
fn internal_relogin_parser_supports_current_flags() {
    let (selector, options) = parse_internal_relogin_options(&[
        "acct-123".to_string(),
        "--allow-email-change".to_string(),
        "--manual-login".to_string(),
        "--keep-session".to_string(),
    ])
    .expect("relogin options");

    assert_eq!(selector, "acct-123");
    assert!(options.allow_email_change);
    assert!(options.manual_login);
    assert!(!options.logout_first);
}

#[test]
fn public_relogin_parser_rejects_internal_flags() {
    let error =
        parse_public_relogin_options(&["acct-123".to_string(), "--manual-login".to_string()])
            .expect_err("public relogin should reject internal flags");
    assert!(error.to_string().contains("Unknown relogin option"));
}

#[test]
fn set_parser_accepts_managed_window_flag_and_requires_selector() {
    assert_eq!(
        parse_set_options(&["acct-123".to_string()]).expect("set selector"),
        SetCommandOptions {
            selector: "acct-123".to_string(),
            rotation_options: RotationCommandOptions::default(),
        }
    );
    assert_eq!(
        parse_set_options(&["-mw".to_string(), "acct-123".to_string()])
            .expect("set selector with managed window"),
        SetCommandOptions {
            selector: "acct-123".to_string(),
            rotation_options: RotationCommandOptions {
                force_managed_window: true,
            },
        }
    );
    let error = parse_set_options(&[]).expect_err("missing selector should fail for set command");
    assert!(error
        .to_string()
        .contains("Usage: codex-rotate set [-mw] <selector>"));
}

#[test]
fn next_parser_accepts_optional_selector_and_managed_window_flag() {
    assert_eq!(
        parse_next_options(&[]).expect("next"),
        NextCommandOptions {
            selector: None,
            rotation_options: RotationCommandOptions::default(),
        }
    );
    assert_eq!(
        parse_next_options(&["acct-123".to_string()]).expect("next selector"),
        NextCommandOptions {
            selector: Some("acct-123".to_string()),
            rotation_options: RotationCommandOptions::default(),
        }
    );
    assert_eq!(
        parse_next_options(&["-mw".to_string(), "acct-123".to_string()])
            .expect("next selector with managed window"),
        NextCommandOptions {
            selector: Some("acct-123".to_string()),
            rotation_options: RotationCommandOptions {
                force_managed_window: true,
            },
        }
    );
    let error = parse_next_options(&["acct-123".to_string(), "extra".to_string()])
        .expect_err("next should reject extra args");
    assert!(error
        .to_string()
        .contains("Usage: codex-rotate next [-mw] [selector]"));
}

#[test]
fn prev_parser_accepts_only_managed_window_flag() {
    assert_eq!(
        parse_prev_options(&[]).expect("prev"),
        RotationCommandOptions::default()
    );
    assert_eq!(
        parse_prev_options(&["-mw".to_string()]).expect("prev -mw"),
        RotationCommandOptions {
            force_managed_window: true,
        }
    );
    let error = parse_prev_options(&["acct-123".to_string()])
        .expect_err("prev should reject positional args");
    assert!(error.to_string().contains("Usage: codex-rotate prev [-mw]"));
}

#[test]
fn repair_host_history_parser_accepts_source_targets_and_apply() {
    let options = parse_repair_host_history_options(&[
        "--source".to_string(),
        "acct-source".to_string(),
        "--target=acct-target-1".to_string(),
        "--target".to_string(),
        "acct-target-2".to_string(),
        "--apply".to_string(),
    ])
    .expect("repair options");
    assert_eq!(options.source_selector, "acct-source");
    assert_eq!(
        options.target_selectors,
        vec!["acct-target-1".to_string(), "acct-target-2".to_string()]
    );
    assert!(!options.all_targets);
    assert!(options.apply);
}

#[test]
fn repair_host_history_parser_defaults_to_all_targets() {
    let options = parse_repair_host_history_options(&[
        "--source=acct-source".to_string(),
        "--dry-run".to_string(),
    ])
    .expect("repair options");
    assert_eq!(options.source_selector, "acct-source");
    assert!(options.target_selectors.is_empty());
    assert!(options.all_targets);
    assert!(!options.apply);
}

#[test]
fn repair_host_history_parser_requires_source() {
    let error = parse_repair_host_history_options(&["--all".to_string()])
        .expect_err("repair should require source");
    assert!(error
        .to_string()
        .contains("repair-host-history --source <selector>"));
}

#[test]
fn guest_bridge_parser_accepts_bind_flag() {
    let bind = parse_guest_bridge_bind(&["--bind".to_string(), "127.0.0.1:9334".to_string()])
        .expect("guest bridge bind");
    assert_eq!(bind.as_deref(), Some("127.0.0.1:9334"));
}

#[test]
fn guest_bridge_parser_rejects_unknown_flags() {
    let error = parse_guest_bridge_bind(&["--wat".to_string()])
        .expect_err("unknown guest bridge flag should fail");
    assert!(error.to_string().contains("Unknown guest-bridge command"));
}

#[test]
fn internal_vm_bootstrap_parser_accepts_guest_root_and_bridge_override() {
    let (guest_root, bridge_root) = parse_internal_vm_bootstrap_options(&[
        "/Volumes/VMs/guest-root".to_string(),
        "--bridge-root".to_string(),
        "/Volumes/VMs/bridge".to_string(),
    ])
    .expect("vm-bootstrap options");
    assert_eq!(guest_root, PathBuf::from("/Volumes/VMs/guest-root"));
    assert_eq!(bridge_root, Some(PathBuf::from("/Volumes/VMs/bridge")));
}

#[test]
fn internal_vm_bootstrap_parser_requires_guest_root() {
    let error = parse_internal_vm_bootstrap_options(&[])
        .expect_err("vm-bootstrap should require guest root");
    assert!(error
        .to_string()
        .contains("internal vm-bootstrap <mounted-guest-root>"));
}

#[test]
fn help_text_mentions_daemon_command() {
    let help = help_text();
    assert!(help.contains("daemon"));
    assert!(help.contains("Start the background runtime daemon"));
    assert!(help.contains("guest-bridge"));
    assert!(help.contains("tray"));
    assert!(help.contains("set"));
}

#[test]
fn daemon_progress_stream_uses_explicit_message_kind() {
    let mut progress = StatusSnapshot::default();
    progress.last_message = Some("[fast-browser] 2026-04-08T00:00:00Z step: ...".to_string());
    progress.last_message_kind = Some(SnapshotMessageKind::Progress);
    assert!(snapshot_contains_progress(&progress));

    let mut status = StatusSnapshot::default();
    status.last_message = Some("watch healthy".to_string());
    status.last_message_kind = Some(SnapshotMessageKind::Status);
    assert!(!snapshot_contains_progress(&status));

    let mut missing_text = StatusSnapshot::default();
    missing_text.last_message_kind = Some(SnapshotMessageKind::Progress);
    assert!(!snapshot_contains_progress(&missing_text));
}

#[test]
fn command_matches_binary_uses_first_token_only() {
    let binary = Path::new("/tmp/codex-rotate-tray");
    assert!(command_matches_binary("/tmp/codex-rotate-tray\n", binary));
    assert!(command_matches_binary(
        "/tmp/codex-rotate-tray --flag ignored",
        binary
    ));
    assert!(!command_matches_binary("/tmp/other-tray", binary));
}

#[cfg(target_os = "macos")]
#[test]
fn stable_tray_binary_candidates_accepts_debug_binary_while_release_is_current() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let previous_disable_local_refresh = std::env::var_os("CODEX_ROTATE_DISABLE_LOCAL_REFRESH");
    unsafe {
        std::env::remove_var("CODEX_ROTATE_DISABLE_LOCAL_REFRESH");
    }
    let repo_root = unique_temp_dir("codex-rotate-stable-tray");
    let debug_binary = repo_root
        .join("target")
        .join("debug")
        .join("codex-rotate-tray");
    let release_binary = repo_root
        .join("target")
        .join("release")
        .join("codex-rotate-tray");

    fs::create_dir_all(debug_binary.parent().expect("debug parent")).expect("debug dir");
    fs::create_dir_all(release_binary.parent().expect("release parent")).expect("release dir");
    fs::create_dir_all(
        repo_root
            .join("packages")
            .join("codex-rotate-app")
            .join("src-tauri")
            .join("src"),
    )
    .expect("tray src dir");
    fs::create_dir_all(
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("src"),
    )
    .expect("core src dir");
    fs::create_dir_all(
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("src"),
    )
    .expect("runtime src dir");
    fs::write(repo_root.join("Cargo.toml"), "").expect("root cargo");
    fs::write(repo_root.join("Cargo.lock"), "").expect("root lock");
    fs::write(
        repo_root
            .join("packages")
            .join("codex-rotate-app")
            .join("src-tauri")
            .join("Cargo.toml"),
        "",
    )
    .expect("tray cargo");
    fs::write(
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("Cargo.toml"),
        "",
    )
    .expect("core cargo");
    fs::write(
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("Cargo.toml"),
        "",
    )
    .expect("runtime cargo");
    fs::write(&debug_binary, "").expect("debug binary");
    fs::write(&release_binary, "").expect("release binary");

    let candidates = stable_tray_binary_candidates(&debug_binary).expect("stable candidates");
    assert_eq!(candidates[0], debug_binary);
    assert!(candidates.contains(&release_binary));

    restore_var(
        "CODEX_ROTATE_DISABLE_LOCAL_REFRESH",
        previous_disable_local_refresh,
    );
    fs::remove_dir_all(&repo_root).ok();
}

#[cfg(target_os = "macos")]
#[test]
fn service_command_matches_binary_accepts_shell_wrapped_scripts() {
    let binary = Path::new("/tmp/codex-rotate-tray");
    assert!(service_command_matches_binary(
        "/bin/sh /tmp/codex-rotate-tray",
        binary
    ));
}

#[test]
fn stale_local_cli_binary_resolution_ignores_non_local_paths() {
    let binary = unique_temp_dir("codex-rotate-non-local").join("codex-rotate");
    fs::create_dir_all(binary.parent().expect("binary parent")).expect("create binary dir");
    fs::write(&binary, "").expect("write binary");

    let resolved = resolve_stale_local_cli_binary(&binary).expect("resolve stale binary");
    assert!(resolved.is_none());

    fs::remove_file(&binary).ok();
    if let Some(parent) = binary.parent() {
        fs::remove_dir_all(parent).ok();
    }
}

#[test]
fn stale_local_cli_binary_resolution_returns_none_when_current() {
    let fixture = LocalCliBuildFixture::new("codex-rotate-current-cli");
    let resolved =
        resolve_stale_local_cli_binary(&fixture.binary_path).expect("resolve current binary");
    assert!(resolved.is_none());
}

#[test]
fn stale_local_cli_binary_resolution_obeys_disable_env() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = LocalCliBuildFixture::new("codex-rotate-disabled-cli");
    fixture.mark_sources_stale();

    with_env_var(
        "CODEX_ROTATE_DISABLE_LOCAL_REFRESH",
        Some(OsStr::new("1")),
        || {
            let resolved = resolve_stale_local_cli_binary(&fixture.binary_path)
                .expect("resolve disabled binary");
            assert!(resolved.is_none());
            Ok(())
        },
    )
    .expect("disable env should short-circuit");
}

#[cfg(unix)]
#[test]
fn stale_local_cli_binary_resolution_rebuilds_and_requests_reexec() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = LocalCliBuildFixture::new("codex-rotate-stale-cli");
    let cargo_script = fixture.repo_root.join("fake-cargo.sh");
    let cargo_log = fixture.repo_root.join("cargo.log");
    fixture.mark_sources_stale();
    write_executable_script(
        &cargo_script,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\ntouch \"{}\"\n",
            cargo_log.display(),
            fixture.binary_path.display()
        ),
    );

    with_env_var(
        "CODEX_ROTATE_CARGO_BIN",
        Some(cargo_script.as_os_str()),
        || {
            let resolved =
                resolve_stale_local_cli_binary(&fixture.binary_path).expect("resolve stale binary");
            assert_eq!(resolved.as_deref(), Some(fixture.binary_path.as_path()));
            let cargo_args = fs::read_to_string(&cargo_log).expect("read cargo log");
            assert!(cargo_args.contains("build"));
            assert!(cargo_args.contains("--manifest-path"));
            assert!(cargo_args.contains("codex-rotate-cli/Cargo.toml"));
            assert!(cargo_args.contains("-p"));
            assert!(cargo_args.contains("codex-rotate-cli"));
            Ok(())
        },
    )
    .expect("stale binary should rebuild");
}

#[cfg(unix)]
#[test]
fn stale_local_cli_binary_resolution_surfaces_rebuild_failures() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = LocalCliBuildFixture::new("codex-rotate-failing-cli");
    let cargo_script = fixture.repo_root.join("fake-cargo-fail.sh");
    fixture.mark_sources_stale();
    write_executable_script(&cargo_script, "#!/bin/sh\nexit 23\n");

    with_env_var(
        "CODEX_ROTATE_CARGO_BIN",
        Some(cargo_script.as_os_str()),
        || {
            let error = resolve_stale_local_cli_binary(&fixture.binary_path)
                .expect_err("rebuild failure should bubble up");
            assert!(error.to_string().contains("build exited with status"));
            Ok(())
        },
    )
    .expect("failure assertion");
}

#[cfg(unix)]
#[test]
fn stale_local_cli_binary_resolution_keeps_debug_path_when_release_exists() {
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
    let fixture = LocalCliBuildFixture::new("codex-rotate-release-pref-cli");
    let cargo_script = fixture.repo_root.join("fake-cargo-release.sh");
    fixture.write_release_binary();
    fixture.mark_sources_stale();
    write_executable_script(
        &cargo_script,
        &format!("#!/bin/sh\ntouch \"{}\"\n", fixture.binary_path.display()),
    );

    with_env_var(
        "CODEX_ROTATE_CARGO_BIN",
        Some(cargo_script.as_os_str()),
        || {
            let resolved =
                resolve_stale_local_cli_binary(&fixture.binary_path).expect("resolve stale binary");
            assert_eq!(resolved.as_deref(), Some(fixture.binary_path.as_path()));
            Ok(())
        },
    )
    .expect("debug path should remain canonical");
}

#[cfg(unix)]
#[test]
fn tray_command_can_launch_report_and_stop_tray_binary() {
    with_rotate_home(|| -> Result<()> {
            let fixture_root = unique_temp_dir("codex-rotate-tray-cli");
            fs::create_dir_all(&fixture_root).expect("fixture root");
            let tray_stub_path = fixture_root.join("codex-rotate-tray");
            let started_path = fixture_root.join("started.txt");
            fs::write(
                &tray_stub_path,
                format!(
                    "#!/bin/sh\ntrap 'exit 0' TERM INT\nprintf 'started\\n' > \"{}\"\nwhile true; do\n  sleep 1\ndone\n",
                    started_path.display()
                ),
            )
            .expect("write tray stub");
            let mut permissions = fs::metadata(&tray_stub_path)
                .expect("tray stub metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&tray_stub_path, permissions).expect("set tray stub permissions");

            let previous_tray_bin = std::env::var_os("CODEX_ROTATE_TRAY_BIN");
            unsafe {
                std::env::set_var("CODEX_ROTATE_TRAY_BIN", &tray_stub_path);
            }

            let test_result = (|| -> Result<()> {
                let mut output = Vec::new();
                run_tray_command(&mut output, &["open".to_string()])?;
                assert_eq!(
                    String::from_utf8(output).expect("utf8").trim(),
                    "Started Codex Rotate tray."
                );
                assert!(codex_rotate_runtime::watch::tray_enabled()?);

                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline && !started_path.exists() {
                    thread::sleep(Duration::from_millis(50));
                }
                assert!(started_path.exists(), "tray stub should have started");

                let error = run_tray_command(&mut Vec::new(), &["status".to_string()])
                    .expect_err("tray without daemon should be unhealthy");
                assert!(
                    error.to_string().contains("daemon is unavailable"),
                    "{error}"
                );

                let mut output = Vec::new();
                run_tray_command(&mut output, &["quit".to_string()])?;
                assert_eq!(
                    String::from_utf8(output).expect("utf8").trim(),
                    "Stopped Codex Rotate tray."
                );
                assert!(!codex_rotate_runtime::watch::tray_enabled()?);

                let error = run_tray_command(&mut Vec::new(), &["status".to_string()])
                    .expect_err("tray should be stopped");
                assert!(error.to_string().contains("not running"));
                Ok(())
            })();

            match previous_tray_bin {
                Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_TRAY_BIN", value) },
                None => unsafe { std::env::remove_var("CODEX_ROTATE_TRAY_BIN") },
            }

            test_result?;

            fs::remove_dir_all(&fixture_root).ok();
            Ok(())
        })
        .expect("tray command");
}

#[test]
fn daemon_command_rejects_unknown_subcommand() {
    let mut output = Vec::new();
    let error = run_daemon_command(&mut output, &["noop".to_string()])
        .expect_err("unknown daemon subcommand should fail");
    assert!(error.to_string().contains("Unknown daemon command"));
}

#[cfg(unix)]
#[test]
fn daemon_command_reports_existing_daemon() {
    with_rotate_home(|| {
        let handle = spawn_reachable_daemon();
        let mut output = Vec::new();
        run_daemon_command(&mut output, &[])?;
        handle.join().expect("daemon probe thread")?;
        assert_eq!(
            String::from_utf8(output).expect("utf8").trim(),
            "Codex Rotate daemon is already running."
        );
        Ok(())
    })
    .expect("daemon command");
}

#[test]
fn parse_daemon_run_options_accepts_hidden_takeover_args() {
    let options = parse_daemon_run_options(&[
        "--takeover".to_string(),
        "--instance-home=/tmp/codex-home".to_string(),
    ])
    .expect("parse daemon options");
    assert_eq!(
        options,
        DaemonRunOptions {
            instance_home: Some("/tmp/codex-home".to_string()),
            takeover: true,
        }
    );
}

#[cfg(unix)]
#[test]
fn proxy_dispatch_covers_supported_cli_commands() {
    let cases = vec![
        (
            Some("create"),
            vec![
                "bench".to_string(),
                "--force".to_string(),
                "--profile".to_string(),
                "dev-1".to_string(),
                "--template".to_string(),
                "dev.{n}@astronlab.com".to_string(),
            ],
            InvokeAction::Create {
                options: CreateInvocation {
                    alias: Some("bench".to_string()),
                    profile_name: Some("dev-1".to_string()),
                    template: Some("dev.{n}@astronlab.com".to_string()),
                    force: true,
                    ignore_current: false,
                    restore_previous_auth_after_create: false,
                    require_usable_quota: false,
                },
            },
        ),
        (Some("next"), Vec::new(), InvokeAction::Next),
        (
            Some("next"),
            vec!["-mw".to_string()],
            InvokeAction::NextManagedWindow,
        ),
        (
            Some("next"),
            vec!["acct-456".to_string()],
            InvokeAction::Set {
                selector: "acct-456".to_string(),
            },
        ),
        (
            Some("next"),
            vec!["acct-456".to_string(), "-mw".to_string()],
            InvokeAction::SetManagedWindow {
                selector: "acct-456".to_string(),
            },
        ),
        (Some("prev"), Vec::new(), InvokeAction::Prev),
        (
            Some("prev"),
            vec!["-mw".to_string()],
            InvokeAction::PrevManagedWindow,
        ),
        (
            Some("set"),
            vec!["acct-456".to_string()],
            InvokeAction::Set {
                selector: "acct-456".to_string(),
            },
        ),
        (
            Some("set"),
            vec!["-mw".to_string(), "acct-456".to_string()],
            InvokeAction::SetManagedWindow {
                selector: "acct-456".to_string(),
            },
        ),
        (
            Some("relogin"),
            vec!["acct-123".to_string()],
            InvokeAction::Relogin {
                options: ReloginInvocation {
                    selector: "acct-123".to_string(),
                    allow_email_change: false,
                    logout_first: false,
                    manual_login: false,
                },
            },
        ),
        (
            Some("remove"),
            vec!["acct-123".to_string()],
            InvokeAction::Remove {
                selector: "acct-123".to_string(),
            },
        ),
    ];

    with_rotate_home(|| {
        for (command, args, expected_action) in cases {
            let handle = spawn_proxy_server("daemon-ok");
            let output = try_run_via_daemon(command, &args).expect("proxy dispatch should succeed");
            let request = handle.join().expect("proxy thread")?;
            assert_eq!(output.as_deref(), Some("daemon-ok"));
            match request {
                ClientRequest::Invoke { action, repo_root } => {
                    assert_eq!(action, expected_action);
                    assert!(repo_root.is_some());
                }
                other => panic!("unexpected request: {other:?}"),
            }
        }
        Ok(())
    })
    .expect("proxy dispatch cases");
}

#[cfg(unix)]
#[test]
fn proxy_dispatch_returns_none_without_daemon() {
    with_rotate_home(|| {
        let output = try_run_via_daemon(Some("status"), &[])?;
        assert!(output.is_none());
        Ok(())
    })
    .expect("no daemon path");
}

#[cfg(unix)]
#[test]
fn proxy_dispatch_bypasses_read_only_commands_even_with_daemon() {
    with_rotate_home(|| {
        let handle = spawn_reachable_daemon();
        let list_output = try_run_via_daemon(Some("list"), &[])?;
        let status_output = try_run_via_daemon(Some("status"), &[])?;
        let add_output = try_run_via_daemon(Some("add"), &["bench".to_string()])?;
        assert!(list_output.is_none());
        assert!(status_output.is_none());
        assert!(add_output.is_none());
        handle.join().expect("reachable daemon thread")?;
        Ok(())
    })
    .expect("read-only commands should stay local");
}

#[cfg(unix)]
#[test]
fn preflight_allows_account_creation_commands_from_any_worktree() {
    for command in [Some("create"), Some("next"), Some("status"), None] {
        ensure_account_creation_commands_allowed(command)
            .unwrap_or_else(|error| panic!("{command:?} should be allowed: {error}"));
    }

    for command in [
        "add", "relogin", "remove", "list", "prev", "set", "daemon", "tray",
    ] {
        ensure_account_creation_commands_allowed(Some(command))
            .unwrap_or_else(|error| panic!("{command} should stay allowed: {error}"));
    }
}

#[cfg(unix)]
#[test]
fn create_command_routes_via_daemon_proxy_from_any_worktree() {
    with_rotate_home(|| {
        with_env_var(
            "CODEX_ROTATE_DISABLE_LOCAL_REFRESH",
            Some(OsStr::new("1")),
            || {
                let args = vec![
                    "create".to_string(),
                    "bench".to_string(),
                    "--force".to_string(),
                    "--profile".to_string(),
                    "dev-1".to_string(),
                    "--template".to_string(),
                    "dev.{n}@astronlab.com".to_string(),
                ];
                let mut output = Vec::new();
                let handle = spawn_proxy_server("daemon-ok");
                run_with_args(&args, &mut output)?;
                let request = handle.join().expect("proxy thread")?;
                assert_eq!(String::from_utf8(output).expect("utf8").trim(), "daemon-ok");
                match request {
                    ClientRequest::Invoke { action, repo_root } => {
                        assert!(repo_root.is_some());
                        assert_eq!(
                            action,
                            InvokeAction::Create {
                                options: CreateInvocation {
                                    alias: Some("bench".to_string()),
                                    profile_name: Some("dev-1".to_string()),
                                    template: Some("dev.{n}@astronlab.com".to_string()),
                                    force: true,
                                    ignore_current: false,
                                    restore_previous_auth_after_create: false,
                                    require_usable_quota: false,
                                }
                            }
                        );
                    }
                    other => panic!("unexpected request: {other:?}"),
                }
                Ok(())
            },
        )
    })
    .expect("create should proxy through daemon");
}

#[cfg(unix)]
#[test]
fn proxy_dispatch_falls_back_to_local_when_daemon_repo_root_mismatches() {
    with_rotate_home(|| {
        let handle = spawn_proxy_error_server(
            "Daemon repo root mismatch: daemon=/tmp/main, request=/tmp/worktree",
        );
        let output = try_run_via_daemon(Some("relogin"), &["acct-123".to_string()])?;
        let request = handle.join().expect("proxy thread")?;
        assert!(output.is_none());
        match request {
            ClientRequest::Invoke { action, repo_root } => {
                assert_eq!(
                    action,
                    InvokeAction::Relogin {
                        options: ReloginInvocation {
                            selector: "acct-123".to_string(),
                            allow_email_change: false,
                            logout_first: false,
                            manual_login: false,
                        },
                    }
                );
                assert!(repo_root.is_some());
            }
            other => panic!("unexpected request: {other:?}"),
        }
        Ok(())
    })
    .expect("repo-root mismatch should fall back to local execution");
}
