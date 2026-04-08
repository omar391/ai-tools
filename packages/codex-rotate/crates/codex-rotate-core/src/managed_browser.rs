use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::paths::{cleanup_legacy_rotate_home_artifacts, resolve_paths};

pub fn ensure_managed_browser_wrapper(profile_name: &str, codex_bin: &str) -> Result<PathBuf> {
    let paths = resolve_paths()?;
    cleanup_legacy_rotate_home_artifacts(&paths.rotate_home)?;
    let opener_path = resolve_managed_browser_opener_path()?;
    ensure_existing_file(
        &opener_path,
        "Managed Codex browser opener script not found",
    )?;
    let rotate_cli_bin = resolve_rotate_cli_binary_path()?;
    ensure_existing_file(&rotate_cli_bin, "codex-rotate CLI binary not found")?;

    let bin_dir = paths.rotate_home.join("bin");
    fs::create_dir_all(&bin_dir).with_context(|| {
        format!(
            "Failed to create Codex Rotate bin directory at {}.",
            bin_dir.display()
        )
    })?;
    let shim_dir = bin_dir.join("codex-login-shims");
    ensure_managed_browser_shims(&shim_dir, &paths.node_bin, &opener_path)?;

    let wrapper_path = build_managed_browser_wrapper_path(
        &paths.rotate_home,
        profile_name,
        codex_bin,
        &opener_path,
    );
    let content = render_managed_browser_wrapper(
        codex_bin,
        profile_name,
        &shim_dir,
        &paths.node_bin,
        &opener_path,
        &rotate_cli_bin,
    );
    let current = fs::read_to_string(&wrapper_path).ok();
    if current.as_deref() != Some(content.as_str()) {
        fs::write(&wrapper_path, content).with_context(|| {
            format!(
                "Failed to write managed browser wrapper to {}.",
                wrapper_path.display()
            )
        })?;
    }
    make_executable(&wrapper_path)?;
    Ok(wrapper_path)
}

fn resolve_managed_browser_opener_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("CODEX_ROTATE_BROWSER_OPENER_BIN").map(PathBuf::from) {
        return Ok(path);
    }
    Ok(resolve_paths()?
        .asset_root
        .join("codex-login-managed-browser-opener.ts"))
}

fn resolve_rotate_cli_binary_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN")
        .map(PathBuf::from)
        .or_else(|| env::var_os("CODEX_ROTATE_CLI_BIN").map(PathBuf::from))
        .or_else(|| env::var_os("CODEX_ROTATE_BIN").map(PathBuf::from))
    {
        return Ok(path);
    }
    env::current_exe().context("Failed to resolve the codex-rotate CLI binary.")
}

fn ensure_existing_file(path: &Path, prefix: &str) -> Result<()> {
    if !path.is_file() {
        return Err(anyhow!("{prefix} at {}.", path.display()));
    }
    Ok(())
}

fn build_managed_browser_wrapper_path(
    rotate_home: &Path,
    profile_name: &str,
    codex_bin: &str,
    opener_path: &Path,
) -> PathBuf {
    let profile_token = normalize_profile_token(profile_name);
    let mut hasher = DefaultHasher::new();
    profile_name.hash(&mut hasher);
    codex_bin.hash(&mut hasher);
    opener_path.to_string_lossy().hash(&mut hasher);
    "rust-managed-login".hash(&mut hasher);
    let hash = format!("{:016x}", hasher.finish());
    rotate_home
        .join("bin")
        .join(format!("codex-login-{profile_token}-{}", &hash[..12]))
}

fn normalize_profile_token(profile_name: &str) -> String {
    let normalized = profile_name
        .chars()
        .map(|ch| {
            let ch = ch.to_ascii_lowercase();
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let shortened = normalized.chars().take(32).collect::<String>();
    if shortened.is_empty() {
        "default".to_string()
    } else {
        shortened
    }
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn render_managed_browser_wrapper(
    real_codex_bin: &str,
    profile_name: &str,
    shim_dir: &Path,
    node_bin: &str,
    opener_path: &Path,
    rotate_cli_bin: &Path,
) -> String {
    [
        "#!/bin/sh".to_string(),
        format!(
            "export FAST_BROWSER_PROFILE={}",
            shell_single_quote(profile_name)
        ),
        format!(
            "export BROWSER={}",
            shell_single_quote(&opener_path.to_string_lossy())
        ),
        format!(
            "export PATH={}:\"$PATH\"",
            shell_single_quote(&shim_dir.to_string_lossy())
        ),
        format!(
            "export CODEX_ROTATE_NODE_BIN={}",
            shell_single_quote(node_bin)
        ),
        format!(
            "export CODEX_ROTATE_REAL_CODEX={}",
            shell_single_quote(real_codex_bin)
        ),
        "if [ \"$1\" = \"login\" ]; then".to_string(),
        "  shift".to_string(),
        format!(
            "  exec {} internal managed-login \"$@\"",
            shell_single_quote(&rotate_cli_bin.to_string_lossy())
        ),
        "fi".to_string(),
        format!("exec {} \"$@\"", shell_single_quote(real_codex_bin)),
        String::new(),
    ]
    .join("\n")
}

fn ensure_managed_browser_shims(shim_dir: &Path, node_bin: &str, opener_path: &Path) -> Result<()> {
    fs::create_dir_all(shim_dir).with_context(|| {
        format!(
            "Failed to create managed browser shim directory at {}.",
            shim_dir.display()
        )
    })?;
    let shim_content = [
        "#!/bin/sh".to_string(),
        format!(
            "exec {} {} \"$@\"",
            shell_single_quote(node_bin),
            shell_single_quote(&opener_path.to_string_lossy())
        ),
        String::new(),
    ]
    .join("\n");
    for shim_name in ["open", "xdg-open"] {
        let shim_path = shim_dir.join(shim_name);
        let current = fs::read_to_string(&shim_path).ok();
        if current.as_deref() != Some(shim_content.as_str()) {
            fs::write(&shim_path, &shim_content).with_context(|| {
                format!(
                    "Failed to write managed browser shim {}.",
                    shim_path.display()
                )
            })?;
        }
        make_executable(&shim_path)?;
    }
    Ok(())
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)
            .with_context(|| format!("Failed to inspect {}.", path.display()))?
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("Failed to set permissions on {}.", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ENV_MUTEX;
    use std::process::Command;

    #[test]
    fn wrapper_path_is_stable_for_same_profile_and_codex_binary() {
        let rotate_home = PathBuf::from("/tmp/codex-rotate-home");
        let opener_path = PathBuf::from("/tmp/opener.ts");
        assert_eq!(
            build_managed_browser_wrapper_path(&rotate_home, "dev-1", "codex", &opener_path),
            build_managed_browser_wrapper_path(&rotate_home, "dev-1", "codex", &opener_path)
        );
    }

    #[test]
    fn wrapper_path_changes_when_profile_changes() {
        let rotate_home = PathBuf::from("/tmp/codex-rotate-home");
        let opener_path = PathBuf::from("/tmp/opener.ts");
        assert_ne!(
            build_managed_browser_wrapper_path(&rotate_home, "dev-1", "codex", &opener_path),
            build_managed_browser_wrapper_path(&rotate_home, "dev-2", "codex", &opener_path)
        );
    }

    #[test]
    fn wrapper_intercepts_login_through_internal_managed_login_entrypoint() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let fixture = tempfile::tempdir().expect("tempdir");
        let rotate_home = fixture.path().join("rotate-home");
        let cli_log_path = fixture.path().join("cli-log.json");
        let cli_path = fixture.path().join("fake-codex-rotate.sh");
        let codex_marker_path = fixture.path().join("codex-invoked.txt");
        let codex_path = fixture.path().join("fake-codex.sh");

        fs::write(
            &codex_path,
            format!(
                "#!/bin/sh\nprintf 'invoked' > {}\nexit 0\n",
                shell_single_quote(&codex_marker_path.to_string_lossy())
            ),
        )
        .expect("write fake codex");
        make_executable(&codex_path).expect("chmod fake codex");

        fs::write(
            &cli_path,
            [
                "#!/bin/sh".to_string(),
                "cat <<EOF > \"$CODEX_ROTATE_TEST_CLI_LOG\"".to_string(),
                "{\"argv\":[\"$1\",\"$2\"],\"profile\":\"$FAST_BROWSER_PROFILE\",\"realCodex\":\"$CODEX_ROTATE_REAL_CODEX\"}"
                    .to_string(),
                "EOF".to_string(),
                "exit 0".to_string(),
                String::new(),
            ]
            .join("\n"),
        )
        .expect("write fake cli");
        make_executable(&cli_path).expect("chmod fake cli");

        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_target_cli = std::env::var_os("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN");
        let previous_cli = std::env::var_os("CODEX_ROTATE_CLI_BIN");
        let previous_bin = std::env::var_os("CODEX_ROTATE_BIN");
        let previous_log = std::env::var_os("CODEX_ROTATE_TEST_CLI_LOG");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN", &cli_path);
            std::env::set_var("CODEX_ROTATE_CLI_BIN", &cli_path);
            std::env::set_var("CODEX_ROTATE_BIN", &cli_path);
            std::env::set_var("CODEX_ROTATE_TEST_CLI_LOG", &cli_log_path);
        }

        let wrapper_path =
            ensure_managed_browser_wrapper("managed-dev-1", &codex_path.to_string_lossy())
                .expect("wrapper path");
        let status = Command::new(&wrapper_path)
            .arg("login")
            .status()
            .expect("run wrapper");

        match previous_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }
        match previous_target_cli {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN", value)
            },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN") },
        }
        match previous_cli {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_CLI_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_CLI_BIN") },
        }
        match previous_bin {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_BIN") },
        }
        match previous_log {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_TEST_CLI_LOG", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_TEST_CLI_LOG") },
        }

        assert!(status.success());
        let logged: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&cli_log_path).expect("read cli log"))
                .expect("parse cli log");
        assert_eq!(logged["profile"], "managed-dev-1");
        assert_eq!(
            logged["argv"],
            serde_json::json!(["internal", "managed-login"])
        );
        assert_eq!(
            logged["realCodex"],
            codex_path.to_string_lossy().to_string()
        );
        assert!(!codex_marker_path.exists());
    }

    #[test]
    fn wrapper_routes_open_based_launches_through_managed_browser_opener() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let fixture = tempfile::tempdir().expect("tempdir");
        let rotate_home = fixture.path().join("rotate-home");
        let opener_log_path = fixture.path().join("opener-log.json");
        let opener_path = fixture.path().join("fake-opener.mjs");
        let cli_path = fixture.path().join("fake-codex-rotate.sh");
        let codex_path = fixture.path().join("fake-codex.sh");

        fs::write(
            &opener_path,
            [
                "#!/usr/bin/env node".to_string(),
                "import { writeFileSync } from \"node:fs\";".to_string(),
                "const logPath = process.env.CODEX_ROTATE_TEST_OPENER_LOG;".to_string(),
                "writeFileSync(logPath, JSON.stringify({".to_string(),
                "  argv: process.argv.slice(2),".to_string(),
                "  profile: process.env.FAST_BROWSER_PROFILE || null,".to_string(),
                "  browser: process.env.BROWSER || null,".to_string(),
                "}));".to_string(),
                String::new(),
            ]
            .join("\n"),
        )
        .expect("write opener");
        make_executable(&opener_path).expect("chmod opener");

        fs::write(&cli_path, "#!/bin/sh\nexit 0\n").expect("write fake cli");
        make_executable(&cli_path).expect("chmod fake cli");
        fs::write(
            &codex_path,
            "#!/bin/sh\nopen \"https://auth.openai.com/oauth/authorize?state=test-wrapper\"\nexit 0\n",
        )
        .expect("write fake codex");
        make_executable(&codex_path).expect("chmod fake codex");

        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_opener = std::env::var_os("CODEX_ROTATE_BROWSER_OPENER_BIN");
        let previous_target_cli = std::env::var_os("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN");
        let previous_cli = std::env::var_os("CODEX_ROTATE_CLI_BIN");
        let previous_bin = std::env::var_os("CODEX_ROTATE_BIN");
        let previous_log = std::env::var_os("CODEX_ROTATE_TEST_OPENER_LOG");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_ROTATE_BROWSER_OPENER_BIN", &opener_path);
            std::env::set_var("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN", &cli_path);
            std::env::set_var("CODEX_ROTATE_CLI_BIN", &cli_path);
            std::env::set_var("CODEX_ROTATE_BIN", &cli_path);
            std::env::set_var("CODEX_ROTATE_TEST_OPENER_LOG", &opener_log_path);
        }

        let wrapper_path =
            ensure_managed_browser_wrapper("managed-dev-1", &codex_path.to_string_lossy())
                .expect("wrapper path");
        let status = Command::new(&wrapper_path)
            .arg("status")
            .status()
            .expect("run wrapper");

        match previous_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }
        match previous_opener {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_BROWSER_OPENER_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_BROWSER_OPENER_BIN") },
        }
        match previous_target_cli {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN", value)
            },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_WRAPPER_TARGET_CLI_BIN") },
        }
        match previous_cli {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_CLI_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_CLI_BIN") },
        }
        match previous_bin {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_BIN") },
        }
        match previous_log {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_TEST_OPENER_LOG", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_TEST_OPENER_LOG") },
        }

        assert!(status.success());
        let logged: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&opener_log_path).expect("read opener log"))
                .expect("parse opener log");
        assert_eq!(logged["profile"], "managed-dev-1");
        assert!(logged["argv"]
            .as_array()
            .expect("argv array")
            .iter()
            .any(|value| value == "https://auth.openai.com/oauth/authorize?state=test-wrapper"));
        assert_eq!(logged["browser"], opener_path.to_string_lossy().to_string());
    }
}
