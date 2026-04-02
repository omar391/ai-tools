use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tauri::{
    image::Image,
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
    ActivationPolicy, AppHandle, Manager,
};

const DEFAULT_PORT: u16 = 9333;
const DEFAULT_INTERVAL_SECONDS: u64 = 15;
const QUOTA_REFRESH_INTERVAL_SECONDS: u64 = 60;

fn build_tray_icon() -> Image<'static> {
    let width = 18u32;
    let height = 18u32;
    let mut rgba = vec![0u8; (width * height * 4) as usize];
    let center_x = 9.0f32;
    let center_y = 9.0f32;
    let outer_radius = 7.0f32;
    let inner_radius = 4.0f32;

    for y in 0..height {
        for x in 0..width {
            let dx = x as f32 + 0.5 - center_x;
            let dy = y as f32 + 0.5 - center_y;
            let distance = (dx * dx + dy * dy).sqrt();
            let angle = dy.atan2(dx).to_degrees();
            let in_gap = angle > -40.0 && angle < 40.0;
            let on_ring = distance <= outer_radius && distance >= inner_radius;
            let arrow_head =
                (x >= 11 && x <= 16) && (y >= 2 && y <= 7) && (x as i32 - y as i32 >= 8);

            if (on_ring && !in_gap) || arrow_head {
                let offset = ((y * width + x) * 4) as usize;
                rgba[offset] = 0;
                rgba[offset + 1] = 0;
                rgba[offset + 2] = 0;
                rgba[offset + 3] = 255;
            }
        }
    }

    Image::new_owned(rgba, width, height)
}

#[derive(Clone, Default)]
struct SharedStatus {
    inner: Arc<Mutex<StatusSnapshot>>,
}

#[derive(Clone)]
struct MenuHandles {
    account_item: MenuItem<tauri::Wry>,
    plan_item: MenuItem<tauri::Wry>,
    quota_item: MenuItem<tauri::Wry>,
    status_item: MenuItem<tauri::Wry>,
    last_rotation_item: MenuItem<tauri::Wry>,
}

#[derive(Clone, Default)]
struct StatusSnapshot {
    current_email: Option<String>,
    current_plan: Option<String>,
    current_quota: Option<String>,
    last_quota_checked_at_ms: Option<u64>,
    last_rotation_email: Option<String>,
    last_rotation_reason: Option<String>,
    last_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LiveAccountEnvelope {
    account: Option<LiveAccount>,
}

#[derive(Debug, Deserialize)]
struct LiveAccount {
    email: Option<String>,
    #[serde(rename = "planType")]
    plan_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QuotaSummaryEnvelope {
    summary: String,
}

#[derive(Debug, Deserialize)]
struct WatchOnceEnvelope {
    rotated: bool,
    rotation: Option<RotationEnvelope>,
    live: Option<RotationLive>,
    decision: WatchDecision,
}

#[derive(Debug, Deserialize)]
struct RotationEnvelope {
    summary: RotationSummary,
}

#[derive(Debug, Deserialize)]
struct RotationSummary {
    email: String,
}

#[derive(Debug, Deserialize)]
struct RotationLive {
    email: String,
    #[serde(rename = "planType")]
    plan_type: String,
}

#[derive(Debug, Deserialize)]
struct WatchDecision {
    reason: Option<String>,
    #[serde(rename = "assessmentError")]
    assessment_error: Option<String>,
    assessment: Option<QuotaSummaryEnvelope>,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root should resolve")
}

fn bun_bin() -> String {
    std::env::var("BUN_BIN").unwrap_or_else(|_| "bun".to_string())
}

fn run_bun_json(args: &[&str]) -> Result<serde_json::Value, String> {
    let repo_root = repo_root();
    let package_entry = repo_root
        .join("packages")
        .join("codex-rotate-app")
        .join("index.ts");
    let output = Command::new(bun_bin())
        .arg(package_entry)
        .args(args)
        .current_dir(&repo_root)
        .output()
        .map_err(|error| format!("failed to run bun command: {error}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        return Err(if detail.is_empty() {
            "bun command failed".to_string()
        } else {
            detail
        });
    }

    serde_json::from_slice::<serde_json::Value>(&output.stdout)
        .map_err(|error| format!("failed to parse bun JSON output: {error}"))
}

fn launch_codex() -> Result<(), String> {
    run_bun_json(&["launch", "--port", &DEFAULT_PORT.to_string()]).map(|_| ())
}

fn read_live_account() -> Result<LiveAccountEnvelope, String> {
    let value = run_bun_json(&["account-read", "--port", &DEFAULT_PORT.to_string()])?;
    serde_json::from_value(value).map_err(|error| format!("failed to decode live account: {error}"))
}

fn rotate_next_and_switch() -> Result<WatchOnceEnvelope, String> {
    let value = run_bun_json(&[
        "rotate-next-and-switch",
        "--port",
        &DEFAULT_PORT.to_string(),
    ])?;
    let wrapped = serde_json::json!({
        "rotated": true,
        "rotation": value.get("rotation").cloned(),
        "live": value.get("live").cloned(),
        "decision": {
            "reason": "manual rotation",
            "assessmentError": null
        }
    });
    serde_json::from_value(wrapped)
        .map_err(|error| format!("failed to decode rotation result: {error}"))
}

fn watch_once() -> Result<WatchOnceEnvelope, String> {
    let value = run_bun_json(&["watch-once", "--port", &DEFAULT_PORT.to_string()])?;
    serde_json::from_value(value).map_err(|error| format!("failed to decode watch result: {error}"))
}

fn read_quota_summary() -> Result<QuotaSummaryEnvelope, String> {
    let value = run_bun_json(&["quota-read"])?;
    serde_json::from_value(value).map_err(|error| format!("failed to decode quota result: {error}"))
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn set_quota_summary(snapshot: &mut StatusSnapshot, summary: String) {
    snapshot.current_quota = Some(summary);
    snapshot.last_quota_checked_at_ms = Some(current_timestamp_ms());
}

fn refresh_quota_summary(snapshot: &mut StatusSnapshot) {
    match read_quota_summary() {
        Ok(assessment) => set_quota_summary(snapshot, assessment.summary),
        Err(_) => {
            if snapshot.current_quota.is_none() {
                snapshot.current_quota = Some("unavailable".to_string());
            }
            snapshot.last_quota_checked_at_ms = Some(current_timestamp_ms());
        }
    }
}

fn should_refresh_quota(snapshot: &StatusSnapshot) -> bool {
    match snapshot.last_quota_checked_at_ms {
        Some(last_checked_at_ms) => {
            current_timestamp_ms().saturating_sub(last_checked_at_ms)
                >= QUOTA_REFRESH_INTERVAL_SECONDS.saturating_mul(1000)
        }
        None => true,
    }
}

fn update_snapshot(app: &AppHandle, snapshot: StatusSnapshot) {
    if let Some(menu) = app.try_state::<MenuHandles>() {
        let account_text = format!(
            "Account: {}",
            snapshot.current_email.as_deref().unwrap_or("unknown")
        );
        let plan_text = format!(
            "Plan: {}",
            snapshot.current_plan.as_deref().unwrap_or("unknown")
        );
        let quota_text = format!(
            "Quota: {}",
            snapshot.current_quota.as_deref().unwrap_or("unknown")
        );
        let status_text = format!(
            "Status: {}",
            snapshot.last_message.as_deref().unwrap_or("starting")
        );
        let rotation_text = format!(
            "Last rotation: {}",
            snapshot.last_rotation_email.as_deref().unwrap_or("none")
        );
        let _ = menu.account_item.set_text(account_text);
        let _ = menu.plan_item.set_text(plan_text);
        let _ = menu.quota_item.set_text(quota_text);
        let _ = menu.status_item.set_text(status_text);
        let _ = menu.last_rotation_item.set_text(rotation_text);
    }

    if let Some(tray) = app.tray_by_id("main") {
        let _ = tray.set_title(None::<&str>);
        let _ = tray.set_tooltip(Some("Codex Rotate\nClick for status"));
    }
}

fn run_check(app: &AppHandle, status: &SharedStatus) {
    let next = match watch_once() {
        Ok(result) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            if let Some(live) = result.live.as_ref() {
                snapshot.current_email = Some(live.email.clone());
                snapshot.current_plan = Some(live.plan_type.clone());
            }
            if result.rotated {
                if let Some(rotation) = result.rotation.as_ref() {
                    snapshot.last_rotation_email = Some(rotation.summary.email.clone());
                }
                snapshot.last_rotation_reason = result.decision.reason.clone();
                snapshot.last_message = Some(format!(
                    "rotated: {}",
                    result
                        .decision
                        .reason
                        .clone()
                        .unwrap_or_else(|| "quota exhausted".to_string())
                ));
                refresh_quota_summary(&mut snapshot);
            } else {
                if let Some(assessment) = result.decision.assessment.as_ref() {
                    set_quota_summary(&mut snapshot, assessment.summary.clone());
                } else if should_refresh_quota(&snapshot) {
                    refresh_quota_summary(&mut snapshot);
                }
                if let Some(error) = result.decision.assessment_error.as_deref() {
                    snapshot.last_message = Some(format!("quota probe failed: {}", error));
                } else {
                    snapshot.last_message = Some("watch healthy".to_string());
                }
            }
            snapshot.clone()
        }
        Err(error) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            snapshot.last_message = Some(format!("watch failed: {}", error));
            snapshot.clone()
        }
    };
    update_snapshot(app, next);
}

fn run_manual_rotation(app: &AppHandle, status: &SharedStatus) {
    let next = match rotate_next_and_switch() {
        Ok(result) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            if let Some(live) = result.live.as_ref() {
                snapshot.current_email = Some(live.email.clone());
                snapshot.current_plan = Some(live.plan_type.clone());
            }
            if let Some(rotation) = result.rotation.as_ref() {
                snapshot.last_rotation_email = Some(rotation.summary.email.clone());
            }
            snapshot.last_rotation_reason = Some("manual rotation".to_string());
            snapshot.last_message = Some("manual rotate succeeded".to_string());
            refresh_quota_summary(&mut snapshot);
            snapshot.clone()
        }
        Err(error) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            snapshot.last_message = Some(format!("manual rotate failed: {}", error));
            snapshot.clone()
        }
    };
    update_snapshot(app, next);
}

fn refresh_live_account(app: &AppHandle, status: &SharedStatus) {
    let next = match read_live_account() {
        Ok(result) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            if let Some(account) = result.account.as_ref() {
                snapshot.current_email = account.email.clone();
                snapshot.current_plan = account.plan_type.clone();
            }
            if should_refresh_quota(&snapshot) {
                refresh_quota_summary(&mut snapshot);
            }
            if snapshot.last_message.is_none() {
                snapshot.last_message = Some("launcher ready".to_string());
            }
            snapshot.clone()
        }
        Err(error) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            snapshot.last_message = Some(format!("account read failed: {}", error));
            snapshot.clone()
        }
    };
    update_snapshot(app, next);
}

fn spawn_watch_loop(app: AppHandle, status: SharedStatus) {
    thread::spawn(move || loop {
        run_check(&app, &status);
        thread::sleep(Duration::from_secs(DEFAULT_INTERVAL_SECONDS));
    });
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            app.set_activation_policy(ActivationPolicy::Accessory);

            let status = SharedStatus::default();
            app.manage(status.clone());

            let account_item =
                MenuItem::with_id(app, "account", "Account: unknown", false, None::<&str>)?;
            let plan_item = MenuItem::with_id(app, "plan", "Plan: unknown", false, None::<&str>)?;
            let quota_item =
                MenuItem::with_id(app, "quota", "Quota: unknown", false, None::<&str>)?;
            let status_item =
                MenuItem::with_id(app, "status", "Status: starting", false, None::<&str>)?;
            let last_rotation_item = MenuItem::with_id(
                app,
                "last_rotation",
                "Last rotation: none",
                false,
                None::<&str>,
            )?;
            let launch_item =
                MenuItem::with_id(app, "launch", "Open Wrapper Codex", true, None::<&str>)?;
            let check_item = MenuItem::with_id(app, "check", "Check Now", true, None::<&str>)?;
            let rotate_item = MenuItem::with_id(app, "rotate", "Rotate Now", true, None::<&str>)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
            app.manage(MenuHandles {
                account_item: account_item.clone(),
                plan_item: plan_item.clone(),
                quota_item: quota_item.clone(),
                status_item: status_item.clone(),
                last_rotation_item: last_rotation_item.clone(),
            });
            let menu = Menu::with_items(
                app,
                &[
                    &account_item,
                    &plan_item,
                    &quota_item,
                    &status_item,
                    &last_rotation_item,
                    &launch_item,
                    &check_item,
                    &rotate_item,
                    &quit_item,
                ],
            )?;

            TrayIconBuilder::with_id("main")
                .icon(build_tray_icon())
                .icon_as_template(true)
                .menu(&menu)
                .show_menu_on_left_click(true)
                .on_menu_event({
                    let app = app.handle().clone();
                    move |app_handle, event| match event.id.as_ref() {
                        "launch" => {
                            let app = app.clone();
                            let status = app.state::<SharedStatus>().inner().clone();
                            thread::spawn(move || {
                                let next = if let Err(error) = launch_codex() {
                                    let mut snapshot = status.inner.lock().expect("status mutex");
                                    snapshot.last_message =
                                        Some(format!("launch failed: {}", error));
                                    snapshot.clone()
                                } else {
                                    refresh_live_account(&app, &status);
                                    return;
                                };
                                update_snapshot(&app, next);
                            });
                        }
                        "check" => {
                            let app = app.clone();
                            let status = app.state::<SharedStatus>().inner().clone();
                            thread::spawn(move || run_check(&app, &status));
                        }
                        "rotate" => {
                            let app = app.clone();
                            let status = app.state::<SharedStatus>().inner().clone();
                            thread::spawn(move || run_manual_rotation(&app, &status));
                        }
                        "quit" => app_handle.exit(0),
                        _ => {}
                    }
                })
                .build(app)?;

            launch_codex().ok();
            refresh_live_account(&app.handle().clone(), &status);
            spawn_watch_loop(app.handle().clone(), status);

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("failed to run codex rotate tray");
}
