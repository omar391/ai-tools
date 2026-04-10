mod build;
mod process;
mod targets;
mod tray_service;

pub use build::{
    daemon_socket_is_older_than_binary, local_refresh_disabled,
    maybe_start_background_release_build, preferred_release_binary, rebuild_local_binary,
    sources_newer_than_binary,
};
pub use process::{
    spawn_detached_process, stop_other_local_daemons, stop_running_daemons, stop_running_trays,
    INSTANCE_HOME_ARG,
};
pub use targets::{
    current_process_local_build, detect_local_build, BuildProfile, LocalBinaryBuild, TargetKind,
};
pub use tray_service::{
    clear_tray_service_registration, ensure_tray_process_registered, launch_tray_process,
    schedule_tray_relaunch_process, tray_service_pid,
};
