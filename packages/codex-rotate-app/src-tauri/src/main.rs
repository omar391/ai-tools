use codex_rotate_core::pool::{load_pool, rotate_next_internal, NextResult};
use codex_rotate_core::quota::CachedQuotaState;
use codex_rotate_tray_core::hook::{read_live_account, switch_live_account_to_current_auth};
use codex_rotate_tray_core::launcher::ensure_debug_codex_instance;
use codex_rotate_tray_core::watch::{
    refresh_quota_cache, run_watch_iteration, WatchIterationOptions,
};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tauri::{
    image::Image,
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
    ActivationPolicy, AppHandle, Manager,
};

const DEFAULT_PORT: u16 = 9333;
const DEFAULT_INTERVAL_SECONDS: u64 = 15;
const LOW_QUOTA_INTERVAL_SECONDS: u64 = 5;
const CRITICAL_QUOTA_INTERVAL_SECONDS: u64 = 2;

fn clamp_unit(value: f32) -> f32 {
    value.clamp(0.0, 1.0)
}

fn rgba_offset(width: u32, x: u32, y: u32) -> usize {
    ((y * width + x) * 4) as usize
}

fn paint_alpha(rgba: &mut [u8], width: u32, x: u32, y: u32, alpha: u8) {
    let offset = rgba_offset(width, x, y);
    rgba[offset] = 0;
    rgba[offset + 1] = 0;
    rgba[offset + 2] = 0;
    rgba[offset + 3] = rgba[offset + 3].max(alpha);
}

fn paint_rect(
    rgba: &mut [u8],
    width: u32,
    height: u32,
    x: i32,
    y: i32,
    rect_width: u32,
    rect_height: u32,
    alpha: u8,
) {
    for row in 0..rect_height {
        for col in 0..rect_width {
            let px = x + col as i32;
            let py = y + row as i32;
            if px < 0 || py < 0 || px >= width as i32 || py >= height as i32 {
                continue;
            }
            paint_alpha(rgba, width, px as u32, py as u32, alpha);
        }
    }
}

fn ring_coverage(distance: f32, inner_radius: f32, outer_radius: f32) -> f32 {
    let outer_alpha = clamp_unit(outer_radius + 0.75 - distance);
    let inner_alpha = clamp_unit(distance - inner_radius + 0.75);
    outer_alpha.min(inner_alpha)
}

fn ccw_distance_degrees(start_degrees: f32, current_degrees: f32) -> f32 {
    (current_degrees - start_degrees).rem_euclid(360.0)
}

fn point_on_circle(center: f32, radius: f32, angle_degrees: f32) -> (f32, f32) {
    let angle = angle_degrees.to_radians();
    (center + radius * angle.cos(), center - radius * angle.sin())
}

fn paint_dot(rgba: &mut [u8], width: u32, height: u32, center_x: f32, center_y: f32, radius: f32) {
    for y in 0..height {
        for x in 0..width {
            let dx = x as f32 + 0.5 - center_x;
            let dy = y as f32 + 0.5 - center_y;
            let distance = (dx * dx + dy * dy).sqrt();
            let alpha = (clamp_unit(radius + 0.75 - distance) * 255.0).round() as u8;
            if alpha > 0 {
                paint_alpha(rgba, width, x, y, alpha);
            }
        }
    }
}

fn seven_segment_mask(digit: char) -> Option<u8> {
    match digit {
        '0' => Some(0b0111111),
        '1' => Some(0b0000110),
        '2' => Some(0b1011011),
        '3' => Some(0b1001111),
        '4' => Some(0b1100110),
        '5' => Some(0b1101101),
        '6' => Some(0b1111101),
        '7' => Some(0b0000111),
        '8' => Some(0b1111111),
        '9' => Some(0b1101111),
        _ => None,
    }
}

fn paint_segment_digit(
    rgba: &mut [u8],
    width: u32,
    height: u32,
    x: i32,
    y: i32,
    digit_width: u32,
    digit_height: u32,
    digit: char,
) {
    let Some(mask) = seven_segment_mask(digit) else {
        return;
    };
    let thickness = (digit_width / 5).max(4);
    let half_height = digit_height / 2;
    let vertical_height = half_height.saturating_sub(thickness);
    let mid_y = y + half_height as i32 - (thickness as i32 / 2);
    let right_x = x + digit_width as i32 - thickness as i32;

    let segments = [
        (
            0b0000001,
            x + thickness as i32,
            y,
            digit_width.saturating_sub(thickness * 2),
            thickness,
        ),
        (
            0b0000010,
            right_x,
            y + thickness as i32,
            thickness,
            vertical_height,
        ),
        (
            0b0000100,
            right_x,
            y + half_height as i32,
            thickness,
            vertical_height,
        ),
        (
            0b0001000,
            x + thickness as i32,
            y + digit_height as i32 - thickness as i32,
            digit_width.saturating_sub(thickness * 2),
            thickness,
        ),
        (
            0b0010000,
            x,
            y + half_height as i32,
            thickness,
            vertical_height,
        ),
        (
            0b0100000,
            x,
            y + thickness as i32,
            thickness,
            vertical_height,
        ),
        (
            0b1000000,
            x + thickness as i32,
            mid_y,
            digit_width.saturating_sub(thickness * 2),
            thickness,
        ),
    ];

    for (segment_mask, seg_x, seg_y, seg_width, seg_height) in segments {
        if mask & segment_mask != 0 {
            paint_rect(
                rgba, width, height, seg_x, seg_y, seg_width, seg_height, 255,
            );
        }
    }
}

fn paint_percent_digits(rgba: &mut [u8], width: u32, height: u32, center: f32, percent: u8) {
    let text = percent.to_string();
    let (digit_width, digit_height, spacing) = match text.len() {
        1 => (28u32, 48u32, 0u32),
        2 => (22u32, 38u32, 4u32),
        _ => (18u32, 30u32, 3u32),
    };
    let total_width =
        text.len() as u32 * digit_width + (text.len().saturating_sub(1) as u32 * spacing);
    let start_x = center.round() as i32 - (total_width as i32 / 2);
    let start_y = center.round() as i32 - (digit_height as i32 / 2);

    for (index, digit) in text.chars().enumerate() {
        let digit_x = start_x + index as i32 * (digit_width + spacing) as i32;
        paint_segment_digit(
            rgba,
            width,
            height,
            digit_x,
            start_y,
            digit_width,
            digit_height,
            digit,
        );
    }
}

fn build_tray_icon(quota_percent: Option<u8>) -> Image<'static> {
    let width = 96u32;
    let height = 96u32;
    let mut rgba = vec![0u8; (width * height * 4) as usize];
    let center = 48.0f32;
    let outer_radius = 46.0f32;
    let inner_radius = 37.0f32;
    let start_degrees = 135.0f32;
    let gauge_sweep = 270.0f32;
    let progress_sweep = quota_percent
        .map(|value| gauge_sweep * (value as f32 / 100.0))
        .unwrap_or(0.0);
    let active_radius = (inner_radius + outer_radius) / 2.0;

    for y in 0..height {
        for x in 0..width {
            let dx = x as f32 + 0.5 - center;
            let dy = center - (y as f32 + 0.5);
            let distance = (dx * dx + dy * dy).sqrt();
            let coverage = ring_coverage(distance, inner_radius, outer_radius);
            if coverage <= 0.0 {
                continue;
            }

            let mut angle = dy.atan2(dx).to_degrees();
            if angle < 0.0 {
                angle += 360.0;
            }
            let angle_distance = ccw_distance_degrees(start_degrees, angle);
            if angle_distance > gauge_sweep {
                continue;
            }

            let base_alpha = (coverage * 70.0).round() as u8;
            let progress_alpha = if progress_sweep > 0.0 && angle_distance <= progress_sweep {
                (coverage * 255.0).round() as u8
            } else {
                0
            };
            let alpha = base_alpha.max(progress_alpha);
            if alpha > 0 {
                paint_alpha(&mut rgba, width, x, y, alpha);
            }
        }
    }

    if let Some(percent) = quota_percent {
        let end_angle = start_degrees + gauge_sweep * (percent as f32 / 100.0);
        let (dot_x, dot_y) = point_on_circle(center, active_radius, end_angle);
        paint_dot(&mut rgba, width, height, dot_x, dot_y, 5.0);
        paint_percent_digits(&mut rgba, width, height, center, percent);
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
    inventory_item: MenuItem<tauri::Wry>,
    plan_item: MenuItem<tauri::Wry>,
    quota_item: MenuItem<tauri::Wry>,
    status_item: MenuItem<tauri::Wry>,
    last_rotation_item: MenuItem<tauri::Wry>,
}

#[derive(Clone, Default)]
struct StatusSnapshot {
    current_email: Option<String>,
    inventory_count: Option<usize>,
    current_plan: Option<String>,
    current_quota: Option<String>,
    current_quota_percent: Option<u8>,
    last_rotation_email: Option<String>,
    last_rotation_reason: Option<String>,
    last_message: Option<String>,
    quota_cache: Option<CachedQuotaState>,
}

fn set_quota_summary(snapshot: &mut StatusSnapshot, quota: &CachedQuotaState) {
    snapshot.current_quota = Some(quota.summary.clone());
    snapshot.current_quota_percent = quota.primary_quota_left_percent;
    snapshot.quota_cache = Some(quota.clone());
}

fn refresh_inventory_count(snapshot: &mut StatusSnapshot) {
    snapshot.inventory_count = load_pool().ok().map(|pool| pool.accounts.len());
}

fn update_snapshot(app: &AppHandle, snapshot: StatusSnapshot) {
    if let Some(menu) = app.try_state::<MenuHandles>() {
        let account_text = format!(
            "Account: {}",
            snapshot.current_email.as_deref().unwrap_or("unknown")
        );
        let inventory_text = match snapshot.inventory_count {
            Some(count) => format!("Inventory: {count} account(s)"),
            None => "Inventory: unknown".to_string(),
        };
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
        let _ = menu.inventory_item.set_text(inventory_text);
        let _ = menu.plan_item.set_text(plan_text);
        let _ = menu.quota_item.set_text(quota_text);
        let _ = menu.status_item.set_text(status_text);
        let _ = menu.last_rotation_item.set_text(rotation_text);
    }

    if let Some(tray) = app.tray_by_id("main") {
        let tooltip = match snapshot.current_quota_percent {
            Some(percent) => format!("Codex Rotate\nQuota: {percent}%\nClick for status"),
            None => "Codex Rotate\nClick for status".to_string(),
        };
        let _ = tray.set_icon(Some(build_tray_icon(snapshot.current_quota_percent)));
        let _ = tray.set_icon_as_template(true);
        let _ = tray.set_title(Option::<String>::None);
        let _ = tray.set_tooltip(Some(tooltip));
    }
}

fn run_check(app: &AppHandle, status: &SharedStatus, force_quota_refresh: bool) {
    let next = match run_watch_iteration(WatchIterationOptions {
        port: Some(DEFAULT_PORT),
        after_signal_id: None,
        cooldown_ms: None,
        force_quota_refresh,
    }) {
        Ok(result) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            refresh_inventory_count(&mut snapshot);
            if let Some(live) = result.live.as_ref() {
                snapshot.current_email = Some(live.email.clone());
                snapshot.current_plan = Some(live.plan_type.clone());
            } else if let Some(email) = result.state.last_live_email.as_ref() {
                snapshot.current_email = Some(email.clone());
            }
            if let Some(quota) = result.state.quota.as_ref() {
                set_quota_summary(&mut snapshot, quota);
            }
            if result.rotated {
                if let Some(rotation) = result.rotation.as_ref() {
                    snapshot.last_rotation_email = Some(rotation.email.clone());
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
            } else if let Some(error) = result.decision.assessment_error.as_deref() {
                snapshot.last_message = Some(format!("quota probe failed: {}", error));
            } else {
                snapshot.last_message = Some("watch healthy".to_string());
            }
            snapshot.clone()
        }
        Err(error) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            refresh_inventory_count(&mut snapshot);
            snapshot.last_message = Some(format!("watch failed: {}", error));
            snapshot.clone()
        }
    };
    update_snapshot(app, next);
}

fn next_watch_interval(status: &SharedStatus) -> Duration {
    let snapshot = status.inner.lock().expect("status mutex");
    let seconds = match snapshot.current_quota_percent {
        Some(percent) if percent <= 2 => CRITICAL_QUOTA_INTERVAL_SECONDS,
        Some(percent) if percent <= 10 => LOW_QUOTA_INTERVAL_SECONDS,
        _ => DEFAULT_INTERVAL_SECONDS,
    };
    Duration::from_secs(seconds)
}

fn run_manual_rotation(app: &AppHandle, status: &SharedStatus) {
    let next = match rotate_next_internal() {
        Ok(result) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            refresh_inventory_count(&mut snapshot);
            match &result {
                NextResult::Rotated { summary, .. }
                | NextResult::Stayed { summary, .. }
                | NextResult::Created { summary, .. } => {
                    snapshot.last_rotation_email = Some(summary.email.clone());
                }
            }

            match switch_live_account_to_current_auth(Some(DEFAULT_PORT), false, 15_000, false) {
                Ok(live) => {
                    snapshot.current_email = Some(live.email.clone());
                    snapshot.current_plan = Some(live.plan_type.clone());
                }
                Err(error) => {
                    snapshot.last_message = Some(format!("manual rotate failed: {}", error));
                    return update_snapshot(app, snapshot.clone());
                }
            }

            match refresh_quota_cache(true, snapshot.quota_cache.as_ref()) {
                Ok(quota) => set_quota_summary(&mut snapshot, &quota),
                Err(error) => {
                    snapshot.last_message = Some(format!("quota refresh failed: {}", error))
                }
            }

            snapshot.last_rotation_reason = Some("manual rotation".to_string());
            snapshot.last_message = Some(match result {
                NextResult::Rotated { .. } => "manual rotate succeeded".to_string(),
                NextResult::Stayed { .. } => "manual rotate stayed on current account".to_string(),
                NextResult::Created { .. } => "manual rotate created a fresh account".to_string(),
            });
            snapshot.clone()
        }
        Err(error) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            refresh_inventory_count(&mut snapshot);
            snapshot.last_message = Some(format!("manual rotate failed: {}", error));
            snapshot.clone()
        }
    };
    update_snapshot(app, next);
}

fn refresh_live_account(app: &AppHandle, status: &SharedStatus, force_quota_refresh: bool) {
    let next = match read_live_account(Some(DEFAULT_PORT)) {
        Ok(result) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            refresh_inventory_count(&mut snapshot);
            if let Some(account) = result.account.as_ref() {
                snapshot.current_email = account.email.clone();
                snapshot.current_plan = account.plan_type.clone();
            }
            match refresh_quota_cache(force_quota_refresh, snapshot.quota_cache.as_ref()) {
                Ok(quota) => set_quota_summary(&mut snapshot, &quota),
                Err(error) => {
                    snapshot.last_message = Some(format!("quota refresh failed: {}", error))
                }
            }
            if snapshot.last_message.is_none() {
                snapshot.last_message = Some("launcher ready".to_string());
            }
            snapshot.clone()
        }
        Err(error) => {
            let mut snapshot = status.inner.lock().expect("status mutex");
            refresh_inventory_count(&mut snapshot);
            snapshot.last_message = Some(format!("account read failed: {}", error));
            snapshot.clone()
        }
    };
    update_snapshot(app, next);
}

fn spawn_watch_loop(app: AppHandle, status: SharedStatus) {
    thread::spawn(move || loop {
        run_check(&app, &status, false);
        thread::sleep(next_watch_interval(&status));
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
            let inventory_item =
                MenuItem::with_id(app, "inventory", "Inventory: unknown", false, None::<&str>)?;
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
                MenuItem::with_id(app, "launch", "Open Managed Codex", true, None::<&str>)?;
            let check_item = MenuItem::with_id(app, "check", "Check Now", true, None::<&str>)?;
            let rotate_item = MenuItem::with_id(app, "rotate", "Rotate Now", true, None::<&str>)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
            app.manage(MenuHandles {
                account_item: account_item.clone(),
                inventory_item: inventory_item.clone(),
                plan_item: plan_item.clone(),
                quota_item: quota_item.clone(),
                status_item: status_item.clone(),
                last_rotation_item: last_rotation_item.clone(),
            });
            let menu = Menu::with_items(
                app,
                &[
                    &account_item,
                    &inventory_item,
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
                .icon(build_tray_icon(None))
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
                                let next = if let Err(error) = ensure_debug_codex_instance(
                                    None,
                                    Some(DEFAULT_PORT),
                                    None,
                                    None,
                                ) {
                                    let mut snapshot = status.inner.lock().expect("status mutex");
                                    snapshot.last_message =
                                        Some(format!("launch failed: {}", error));
                                    snapshot.clone()
                                } else {
                                    refresh_live_account(&app, &status, true);
                                    return;
                                };
                                update_snapshot(&app, next);
                            });
                        }
                        "check" => {
                            let app = app.clone();
                            let status = app.state::<SharedStatus>().inner().clone();
                            thread::spawn(move || run_check(&app, &status, true));
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

            ensure_debug_codex_instance(None, Some(DEFAULT_PORT), None, None).ok();
            refresh_live_account(&app.handle().clone(), &status, true);
            spawn_watch_loop(app.handle().clone(), status);

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("failed to run codex rotate tray");
}
