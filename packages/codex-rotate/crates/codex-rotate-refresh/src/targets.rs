use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildProfile {
    Debug,
    Release,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetKind {
    Cli,
    Tray,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalBinaryBuild {
    pub repo_root: PathBuf,
    pub profile: BuildProfile,
    pub binary_path: PathBuf,
    pub target: TargetKind,
}

pub fn detect_local_build(binary: &Path, target: TargetKind) -> Option<LocalBinaryBuild> {
    let (repo_root, profile) = detect_local_build_layout(binary, manifest_segments(target))?;
    Some(LocalBinaryBuild {
        repo_root,
        profile,
        binary_path: binary.to_path_buf(),
        target,
    })
}

pub fn current_process_local_build(target: TargetKind) -> Option<LocalBinaryBuild> {
    std::env::current_exe()
        .ok()
        .and_then(|path| detect_local_build(&path, target))
}

pub(crate) fn tracked_source_paths(build: &LocalBinaryBuild) -> Vec<PathBuf> {
    tracked_source_paths_for(build.target, &build.repo_root)
}

pub(crate) fn manifest_path(build: &LocalBinaryBuild) -> PathBuf {
    build.repo_root.join_iter(manifest_segments(build.target))
}

pub(crate) fn package_name(target: TargetKind) -> &'static str {
    match target {
        TargetKind::Cli => "codex-rotate-cli",
        TargetKind::Tray => "codex-rotate-tray",
    }
}

fn tracked_source_paths_for(target: TargetKind, repo_root: &Path) -> Vec<PathBuf> {
    let mut paths = vec![
        repo_root.join("Cargo.toml"),
        repo_root.join("Cargo.lock"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("src"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("src"),
    ];
    match target {
        TargetKind::Cli => {
            paths.push(
                repo_root
                    .join("packages")
                    .join("codex-rotate")
                    .join("crates")
                    .join("codex-rotate-cli")
                    .join("Cargo.toml"),
            );
            paths.push(
                repo_root
                    .join("packages")
                    .join("codex-rotate")
                    .join("crates")
                    .join("codex-rotate-cli")
                    .join("src"),
            );
        }
        TargetKind::Tray => {
            paths.push(
                repo_root
                    .join("packages")
                    .join("codex-rotate-app")
                    .join("src-tauri")
                    .join("Cargo.toml"),
            );
            paths.push(
                repo_root
                    .join("packages")
                    .join("codex-rotate-app")
                    .join("src-tauri")
                    .join("src"),
            );
        }
    }
    paths
}

fn manifest_segments(target: TargetKind) -> &'static [&'static str] {
    match target {
        TargetKind::Cli => &[
            "packages",
            "codex-rotate",
            "crates",
            "codex-rotate-cli",
            "Cargo.toml",
        ],
        TargetKind::Tray => &["packages", "codex-rotate-app", "src-tauri", "Cargo.toml"],
    }
}

fn detect_local_build_layout(
    binary: &Path,
    manifest_segments: &[&str],
) -> Option<(PathBuf, BuildProfile)> {
    let profile_dir = binary.parent()?;
    let profile = match profile_dir.file_name()?.to_str()? {
        "debug" => BuildProfile::Debug,
        "release" => BuildProfile::Release,
        _ => return None,
    };
    let target_dir = profile_dir.parent()?;
    if target_dir.file_name()?.to_str()? != "target" {
        return None;
    }
    let repo_root = target_dir.parent()?.to_path_buf();
    if !repo_root.join("Cargo.toml").is_file() || !repo_root.join_iter(manifest_segments).is_file()
    {
        return None;
    }
    Some((repo_root, profile))
}

trait JoinPathExt {
    fn join_iter(&self, segments: &[&str]) -> PathBuf;
}

impl JoinPathExt for PathBuf {
    fn join_iter(&self, segments: &[&str]) -> PathBuf {
        let mut path = self.clone();
        for segment in segments {
            path.push(segment);
        }
        path
    }
}

impl JoinPathExt for Path {
    fn join_iter(&self, segments: &[&str]) -> PathBuf {
        let mut path = self.to_path_buf();
        for segment in segments {
            path.push(segment);
        }
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    #[test]
    fn detect_local_cli_build_reads_target_layout() {
        let path = PathBuf::from("/tmp/demo/target/debug/codex-rotate");
        let repo_root = PathBuf::from("/tmp/demo");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli"),
        )
        .expect("create cli crate dir");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
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

        let detected = detect_local_build(&path, TargetKind::Cli).expect("detect build");
        assert_eq!(detected.repo_root, repo_root);
        assert_eq!(detected.profile, BuildProfile::Debug);
        assert_eq!(detected.target, TargetKind::Cli);

        fs::remove_dir_all(&repo_root).ok();
    }

    #[test]
    fn detect_local_tray_build_reads_target_layout() {
        let path = PathBuf::from("/tmp/demo-tray/target/debug/codex-rotate-tray");
        let repo_root = PathBuf::from("/tmp/demo-tray");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri"),
        )
        .expect("create tray crate dir");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri")
                .join("Cargo.toml"),
            "",
        )
        .expect("write tray cargo");

        let detected = detect_local_build(&path, TargetKind::Tray).expect("detect tray build");
        assert_eq!(detected.repo_root, repo_root);
        assert_eq!(detected.profile, BuildProfile::Debug);
        assert_eq!(detected.target, TargetKind::Tray);

        fs::remove_dir_all(&repo_root).ok();
    }

    #[test]
    fn tracked_source_paths_include_target_specific_sources() {
        let repo_root = unique_temp_dir("refresh-targets");
        let cli = LocalBinaryBuild {
            repo_root: repo_root.clone(),
            profile: BuildProfile::Debug,
            binary_path: repo_root.join("target/debug/codex-rotate"),
            target: TargetKind::Cli,
        };
        let tray = LocalBinaryBuild {
            repo_root: repo_root.clone(),
            profile: BuildProfile::Debug,
            binary_path: repo_root.join("target/debug/codex-rotate-tray"),
            target: TargetKind::Tray,
        };

        let cli_paths = tracked_source_paths(&cli);
        let tray_paths = tracked_source_paths(&tray);

        assert!(cli_paths
            .iter()
            .any(|path| path.ends_with("codex-rotate-cli/src")));
        assert!(tray_paths
            .iter()
            .any(|path| path.ends_with("src-tauri/src")));
        assert!(!cli_paths
            .iter()
            .any(|path| path.ends_with("packages/codex-rotate-app/src-tauri/src")));
        assert!(!tray_paths
            .iter()
            .any(|path| path.ends_with("packages/codex-rotate/crates/codex-rotate-cli/src")));

        let _ = Duration::from_millis(0);
    }
}
