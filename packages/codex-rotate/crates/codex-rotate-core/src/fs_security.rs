use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub fn write_private_string(path: &Path, raw: &str) -> Result<()> {
    let parent = if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
        parent
    } else {
        Path::new(".")
    };

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("private");

    for attempt in 0..8 {
        let temp_path = parent.join(format!(
            ".{file_name}.tmp-{}-{}-{attempt}",
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let open_result = {
            #[cfg(unix)]
            {
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(&temp_path)
            }
            #[cfg(not(unix))]
            {
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temp_path)
            }
        };

        match open_result {
            Ok(mut file) => {
                let write_result = (|| -> Result<()> {
                    file.write_all(raw.as_bytes())?;
                    file.sync_all()?;
                    Ok(())
                })();
                if let Err(error) = write_result {
                    let _ = fs::remove_file(&temp_path);
                    return Err(error)
                        .with_context(|| format!("Failed to write {}.", temp_path.display()));
                }
                drop(file);
                fs::rename(&temp_path, path).with_context(|| {
                    format!(
                        "Failed to replace {} with {}.",
                        path.display(),
                        temp_path.display()
                    )
                })?;
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Failed to create {}.", temp_path.display()));
            }
        }
    }

    Err(anyhow!(
        "Failed to allocate a temporary file for {}.",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::write_private_string;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), suffix))
    }

    #[test]
    fn write_private_string_replaces_existing_contents() {
        let root = unique_temp_dir("codex-rotate-fs-security");
        fs::create_dir_all(&root).expect("create temp dir");
        let path = root.join("state.json");

        write_private_string(&path, "{\"version\": 700}\n").expect("write first contents");
        write_private_string(&path, "{\"version\": 7}\n").expect("replace contents");

        assert_eq!(
            fs::read_to_string(&path).expect("read written file"),
            "{\"version\": 7}\n"
        );

        fs::remove_dir_all(&root).ok();
    }
}
