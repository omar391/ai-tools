use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

mod account_state;
mod artifact_capture;
mod managed_browser_opener;
mod watch_trigger;
mod app_server;

pub use account_state::{
    stable_test_accounts, test_account, test_auth, test_persona, test_vm_environment,
    IsolatedAccountStateFixture, IsolatedAccountStateFixtureBuilder,
};
pub use managed_browser_opener::{
    FakeManagedBrowserOpenerFixture, FakeManagedBrowserOpenerFixtureBuilder,
    FakeManagedBrowserOpenerLaunchFailure, FakeManagedBrowserOpenerInstallGuard,
};
pub use watch_trigger::{WatchSignalRow, WatchTriggerHarness};
pub use app_server::{
    FakeCodexAppServerFixture, FakeCodexAppServerFixtureBuilder, FakeCodexAppServerOutcome,
    FakeCodexAppServerRequest,
};
pub use artifact_capture::{FailureArtifactBundle, FailureArtifactCapture};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IsolatedHomePaths {
    pub sandbox_root: PathBuf,
    pub home_dir: PathBuf,
    pub rotate_home: PathBuf,
    pub codex_home: PathBuf,
    pub fast_browser_home: PathBuf,
    pub codex_app_support_dir: PathBuf,
}

#[derive(Debug)]
pub struct IsolatedHomeFixture {
    root: tempfile::TempDir,
    paths: IsolatedHomePaths,
}

#[derive(Debug)]
pub struct IsolatedHomeGuard {
    previous_home: Option<OsString>,
    previous_rotate_home: Option<OsString>,
    previous_codex_home: Option<OsString>,
    previous_fast_browser_home: Option<OsString>,
    previous_codex_app_support_dir: Option<OsString>,
}

impl IsolatedHomeFixture {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        let temp_prefix: String = prefix
            .as_ref()
            .chars()
            .filter(|character| character.is_ascii_alphanumeric())
            .take(8)
            .collect();
        let temp_prefix = if temp_prefix.is_empty() {
            "cr".to_string()
        } else {
            temp_prefix
        };
        let root = tempfile::Builder::new()
            .prefix(&temp_prefix)
            .tempdir()
            .context("create isolated home fixture root")?;
        let sandbox_root = root.path().to_path_buf();
        let home_dir = sandbox_root.clone();
        let rotate_home = home_dir.join("r");
        let codex_home = home_dir.join("c");
        let fast_browser_home = home_dir.join("f");
        let codex_app_support_dir = home_dir.join("a");

        for path in [
            &home_dir,
            &rotate_home,
            &codex_home,
            &fast_browser_home,
            &codex_app_support_dir,
        ] {
            fs::create_dir_all(path)
                .with_context(|| format!("create isolated path {}", path.display()))?;
        }

        Ok(Self {
            root,
            paths: IsolatedHomePaths {
                sandbox_root,
                home_dir,
                rotate_home,
                codex_home,
                fast_browser_home,
                codex_app_support_dir,
            },
        })
    }

    pub fn sandbox_root(&self) -> &Path {
        self.paths.sandbox_root.as_path()
    }

    pub fn home_dir(&self) -> &Path {
        self.paths.home_dir.as_path()
    }

    pub fn rotate_home(&self) -> &Path {
        self.paths.rotate_home.as_path()
    }

    pub fn codex_home(&self) -> &Path {
        self.paths.codex_home.as_path()
    }

    pub fn fast_browser_home(&self) -> &Path {
        self.paths.fast_browser_home.as_path()
    }

    pub fn codex_app_support_dir(&self) -> &Path {
        self.paths.codex_app_support_dir.as_path()
    }

    pub fn paths(&self) -> &IsolatedHomePaths {
        &self.paths
    }

    pub fn install(&self) -> IsolatedHomeGuard {
        let previous_home = std::env::var_os("HOME");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support_dir = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");

        unsafe {
            std::env::set_var("HOME", &self.paths.home_dir);
            std::env::set_var("CODEX_ROTATE_HOME", &self.paths.rotate_home);
            std::env::set_var("CODEX_HOME", &self.paths.codex_home);
            std::env::set_var("FAST_BROWSER_HOME", &self.paths.fast_browser_home);
            std::env::set_var(
                "CODEX_ROTATE_CODEX_APP_SUPPORT",
                &self.paths.codex_app_support_dir,
            );
        }

        IsolatedHomeGuard {
            previous_home,
            previous_rotate_home,
            previous_codex_home,
            previous_fast_browser_home,
            previous_codex_app_support_dir,
        }
    }
}

impl Drop for IsolatedHomeFixture {
    fn drop(&mut self) {
        let _ = &self.root;
    }
}

impl Drop for IsolatedHomeGuard {
    fn drop(&mut self) {
        restore_var("HOME", self.previous_home.take());
        restore_var("CODEX_ROTATE_HOME", self.previous_rotate_home.take());
        restore_var("CODEX_HOME", self.previous_codex_home.take());
        restore_var("FAST_BROWSER_HOME", self.previous_fast_browser_home.take());
        restore_var(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            self.previous_codex_app_support_dir.take(),
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

#[cfg(test)]
pub(crate) fn test_environment_mutex() -> &'static std::sync::Mutex<()> {
    static ENV_MUTEX: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    ENV_MUTEX.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_mutex() -> &'static std::sync::Mutex<()> {
        crate::test_environment_mutex()
    }

    fn restore_env(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe {
                std::env::set_var(name, value);
            },
            None => unsafe {
                std::env::remove_var(name);
            },
        }
    }

    #[test]
    fn fixture_provisions_isolated_home_paths() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = IsolatedHomeFixture::new("codex-rotate-test-support")?;

        assert!(fixture.sandbox_root().exists());
        assert!(fixture.home_dir().exists());
        assert!(fixture.rotate_home().exists());
        assert!(fixture.codex_home().exists());
        assert!(fixture.fast_browser_home().exists());
        assert!(fixture.codex_app_support_dir().exists());

        assert!(fixture.rotate_home().starts_with(fixture.home_dir()));
        assert!(fixture.codex_home().starts_with(fixture.home_dir()));
        assert!(fixture.fast_browser_home().starts_with(fixture.home_dir()));
        assert!(fixture
            .codex_app_support_dir()
            .starts_with(fixture.home_dir()));
        Ok(())
    }

    #[test]
    fn install_sets_and_restores_isolated_home_environment() -> Result<()> {
        let _guard = env_mutex()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

        let previous_home = std::env::var_os("HOME");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support_dir = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");

        let sentinel_home = OsString::from("sentinel-home");
        let sentinel_rotate_home = OsString::from("sentinel-rotate-home");
        let sentinel_codex_home = OsString::from("sentinel-codex-home");
        let sentinel_fast_browser_home = OsString::from("sentinel-fast-browser-home");
        let sentinel_codex_app_support_dir = OsString::from("sentinel-codex-app-support");

        unsafe {
            std::env::set_var("HOME", &sentinel_home);
            std::env::set_var("CODEX_ROTATE_HOME", &sentinel_rotate_home);
            std::env::set_var("CODEX_HOME", &sentinel_codex_home);
            std::env::set_var("FAST_BROWSER_HOME", &sentinel_fast_browser_home);
            std::env::set_var(
                "CODEX_ROTATE_CODEX_APP_SUPPORT",
                &sentinel_codex_app_support_dir,
            );
        }

        {
            let fixture = IsolatedHomeFixture::new("codex-rotate-test-support")?;
            let guard = fixture.install();

            assert_eq!(
                std::env::var_os("HOME"),
                Some(fixture.home_dir().as_os_str().to_os_string())
            );
            assert_eq!(
                std::env::var_os("CODEX_ROTATE_HOME"),
                Some(fixture.rotate_home().as_os_str().to_os_string())
            );
            assert_eq!(
                std::env::var_os("CODEX_HOME"),
                Some(fixture.codex_home().as_os_str().to_os_string())
            );
            assert_eq!(
                std::env::var_os("FAST_BROWSER_HOME"),
                Some(fixture.fast_browser_home().as_os_str().to_os_string())
            );
            assert_eq!(
                std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT"),
                Some(fixture.codex_app_support_dir().as_os_str().to_os_string())
            );

            drop(guard);
        }

        assert_eq!(std::env::var_os("HOME"), Some(sentinel_home.clone()));
        assert_eq!(
            std::env::var_os("CODEX_ROTATE_HOME"),
            Some(sentinel_rotate_home.clone())
        );
        assert_eq!(
            std::env::var_os("CODEX_HOME"),
            Some(sentinel_codex_home.clone())
        );
        assert_eq!(
            std::env::var_os("FAST_BROWSER_HOME"),
            Some(sentinel_fast_browser_home.clone())
        );
        assert_eq!(
            std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT"),
            Some(sentinel_codex_app_support_dir.clone())
        );

        restore_env("HOME", previous_home);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
        restore_env(
            "CODEX_ROTATE_CODEX_APP_SUPPORT",
            previous_codex_app_support_dir,
        );
        Ok(())
    }
}
