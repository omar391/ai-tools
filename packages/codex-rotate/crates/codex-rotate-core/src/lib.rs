pub mod auth;
pub mod bridge;
pub mod cancel;
pub mod fs_security;
pub mod managed_browser;
pub mod paths;
pub mod pool;
pub mod quota;
pub mod state;
pub mod workflow;

#[cfg(test)]
pub mod test_support {
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub static ENV_MUTEX: Mutex<()> = Mutex::new(());

    pub fn unique_temp_dir(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    pub struct RotateHomeGuard {
        _guard: MutexGuard<'static, ()>,
        previous_rotate_home: Option<OsString>,
        rotate_home: PathBuf,
    }

    impl RotateHomeGuard {
        pub fn enter(prefix: &str) -> Self {
            let guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
            let rotate_home = unique_temp_dir(prefix);
            fs::create_dir_all(&rotate_home).expect("create rotate home");
            let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
            unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            }
            Self {
                _guard: guard,
                previous_rotate_home,
                rotate_home,
            }
        }

        pub fn path(&self) -> &Path {
            &self.rotate_home
        }
    }

    impl Drop for RotateHomeGuard {
        fn drop(&mut self) {
            match self.previous_rotate_home.take() {
                Some(value) => unsafe {
                    std::env::set_var("CODEX_ROTATE_HOME", value);
                },
                None => unsafe {
                    std::env::remove_var("CODEX_ROTATE_HOME");
                },
            }
            fs::remove_dir_all(&self.rotate_home).ok();
        }
    }
}
