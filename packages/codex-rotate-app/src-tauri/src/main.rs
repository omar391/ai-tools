use codex_rotate_refresh::clear_tray_service_registration;
use codex_rotate_runtime::ipc::{invoke, InvokeAction, SnapshotMessageKind, StatusSnapshot};
use codex_rotate_runtime::runtime_log::{log_tray_error, log_tray_info};
use codex_rotate_runtime::watch::set_tray_enabled;
use codex_rotate_tray::{
    error_snapshot, rendered_snapshot, spawn_subscription_loop_controlled,
    spawn_tray_refresh_loop_controlled, SharedRenderState,
};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tauri::{
    image::Image,
    menu::{CheckMenuItem, Menu, MenuItem},
    tray::TrayIconBuilder,
    ActivationPolicy, AppHandle, Manager,
};

const CHECK_NOW_LABEL: &str = "Check Now";
const RECHECK_NOW_LABEL: &str = "Re-check Now";

#[derive(Clone, Default)]
struct SharedTrayState {
    inner: Arc<Mutex<TrayState>>,
    quit_prompt_open: Arc<AtomicBool>,
}

#[derive(Clone, Default)]
struct TrayState {
    last_snapshot: StatusSnapshot,
    deferred_shutdown_requested: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuitChoice {
    ForceQuit,
    QuitAfterTask,
    Cancel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrayQuitStrategy {
    Immediate,
    Deferred,
}

impl SharedTrayState {
    fn set_snapshot(&self, snapshot: StatusSnapshot) {
        let mut state = self.inner.lock().expect("tray state mutex");
        state.last_snapshot = snapshot;
    }

    fn snapshot(&self) -> StatusSnapshot {
        self.inner
            .lock()
            .expect("tray state mutex")
            .last_snapshot
            .clone()
    }

    fn set_deferred_shutdown_requested(&self, requested: bool) {
        let mut state = self.inner.lock().expect("tray state mutex");
        state.deferred_shutdown_requested = requested;
    }

    fn should_exit_after_deferred_shutdown(&self, snapshot: &StatusSnapshot) -> bool {
        let mut state = self.inner.lock().expect("tray state mutex");
        if !state.deferred_shutdown_requested {
            return false;
        }
        let disconnected = snapshot
            .last_message
            .as_deref()
            .is_some_and(|message| message.starts_with("daemon disconnected:"));
        if disconnected {
            state.deferred_shutdown_requested = false;
        }
        disconnected
    }

    fn begin_quit_prompt(&self) -> bool {
        !self.quit_prompt_open.swap(true, Ordering::SeqCst)
    }

    fn finish_quit_prompt(&self) {
        self.quit_prompt_open.store(false, Ordering::SeqCst);
    }
}

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

fn paint_arc(
    rgba: &mut [u8],
    width: u32,
    height: u32,
    center_x: f32,
    center_y: f32,
    inner_radius: f32,
    outer_radius: f32,
    start_degrees: f32,
    sweep_degrees: f32,
    alpha: u8,
) {
    for y in 0..height {
        for x in 0..width {
            let dx = x as f32 + 0.5 - center_x;
            let dy = center_y - (y as f32 + 0.5);
            let distance = (dx * dx + dy * dy).sqrt();
            let coverage = ring_coverage(distance, inner_radius, outer_radius);
            if coverage <= 0.0 {
                continue;
            }

            let mut angle = dy.atan2(dx).to_degrees();
            if angle < 0.0 {
                angle += 360.0;
            }
            if ccw_distance_degrees(start_degrees, angle) > sweep_degrees {
                continue;
            }

            let blended_alpha = (coverage * alpha as f32).round() as u8;
            if blended_alpha > 0 {
                paint_alpha(rgba, width, x, y, blended_alpha);
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

fn paint_activity_badge(rgba: &mut [u8], width: u32, height: u32) {
    let center_x = width as f32 - 18.0;
    let center_y = 19.0f32;

    paint_arc(
        rgba, width, height, center_x, center_y, 8.5, 11.5, 298.0, 250.0, 255,
    );
    paint_arc(
        rgba, width, height, center_x, center_y, 5.0, 7.0, 320.0, 220.0, 200,
    );
    paint_dot(rgba, width, height, center_x, center_y, 3.75);
}

fn build_tray_icon_rgba(
    quota_percent: Option<u8>,
    show_activity_badge: bool,
) -> (Vec<u8>, u32, u32) {
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

    if show_activity_badge {
        paint_activity_badge(&mut rgba, width, height);
    }

    (rgba, width, height)
}

fn build_tray_icon(quota_percent: Option<u8>, show_activity_badge: bool) -> Image<'static> {
    let (rgba, width, height) = build_tray_icon_rgba(quota_percent, show_activity_badge);
    Image::new_owned(rgba, width, height)
}

#[derive(Clone)]
struct MenuHandles {
    account_item: MenuItem<tauri::Wry>,
    inventory_item: MenuItem<tauri::Wry>,
    plan_item: MenuItem<tauri::Wry>,
    quota_item: MenuItem<tauri::Wry>,
    status_item: MenuItem<tauri::Wry>,
    last_rotation_item: MenuItem<tauri::Wry>,
    auto_create_item: CheckMenuItem<tauri::Wry>,
    launch_item: MenuItem<tauri::Wry>,
    check_item: MenuItem<tauri::Wry>,
    rotate_item: MenuItem<tauri::Wry>,
}

fn update_snapshot(app: &AppHandle, snapshot: StatusSnapshot) {
    if let Some(tray_state) = app.try_state::<SharedTrayState>() {
        tray_state.set_snapshot(snapshot.clone());
        if tray_state.should_exit_after_deferred_shutdown(&snapshot) {
            let app_handle = app.clone();
            let _ = app.run_on_main_thread(move || {
                app_handle.exit(0);
            });
            return;
        }
    }

    let rendered = rendered_snapshot(&snapshot);
    if let Some(render_state) = app.try_state::<SharedRenderState>() {
        if !render_state.begin_render(&rendered) {
            return;
        }
    }

    if let Some(menu) = app.try_state::<MenuHandles>() {
        let _ = menu.account_item.set_text(rendered.account_text.clone());
        let _ = menu
            .inventory_item
            .set_text(rendered.inventory_text.clone());
        let _ = menu.plan_item.set_text(rendered.plan_text.clone());
        let _ = menu.quota_item.set_text(rendered.quota_text.clone());
        let _ = menu.status_item.set_text(rendered.status_text.clone());
        let _ = menu
            .last_rotation_item
            .set_text(rendered.rotation_text.clone());
        let _ = menu
            .auto_create_item
            .set_checked(rendered.auto_create_enabled);
        let _ = menu.launch_item.set_enabled(rendered.launch_enabled);
        let _ = menu.check_item.set_enabled(rendered.check_enabled);
        let _ = menu.rotate_item.set_enabled(rendered.rotate_enabled);
    }

    if let Some(tray) = app.tray_by_id("main") {
        let _ = tray.set_icon(Some(build_tray_icon(
            rendered.quota_percent,
            rendered.show_activity_badge,
        )));
        let _ = tray.set_title(Option::<String>::None);
        let _ = tray.set_tooltip(Some(rendered.tooltip_text));
    }
}

fn run_on_main_thread(app: &AppHandle, snapshot: StatusSnapshot) {
    let app_handle = app.clone();
    let _ = app.run_on_main_thread(move || update_snapshot(&app_handle, snapshot));
}

fn spawn_invoke(app: AppHandle, action: InvokeAction) {
    thread::spawn(move || {
        let action_name = format!("{action:?}");
        if let Err(error) = invoke(action) {
            log_tray_error(format!("tray invoke {action_name} failed: {error}"));
            run_on_main_thread(&app, error_snapshot(error.to_string()));
        }
    });
}

fn snapshot_indicates_active_task(snapshot: &StatusSnapshot) -> bool {
    snapshot.last_message_kind == Some(SnapshotMessageKind::Progress)
}

fn active_quit_prompt_message(snapshot: &StatusSnapshot) -> String {
    let detail = snapshot
        .last_message
        .as_deref()
        .unwrap_or("Codex Rotate is still working.");
    format!(
        "A create or refresh task is still running.\n\n{detail}\n\nForce Quit closes now and may cancel the active task. Quit After Task keeps the tray open until the current task finishes, then shuts down the daemon."
    )
}

fn apple_script_quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "macos")]
fn confirm_active_quit_choice(snapshot: &StatusSnapshot) -> QuitChoice {
    let script = format!(
        "button returned of (display dialog \"{}\" with title \"Codex Rotate\" buttons {{\"Cancel\", \"Quit After Task\", \"Force Quit\"}} default button \"Quit After Task\")",
        apple_script_quote(&active_quit_prompt_message(snapshot))
    );
    match Command::new("osascript").arg("-e").arg(script).output() {
        Ok(output) if output.status.success() => {
            match String::from_utf8_lossy(&output.stdout).trim() {
                "Force Quit" => QuitChoice::ForceQuit,
                "Quit After Task" => QuitChoice::QuitAfterTask,
                _ => QuitChoice::Cancel,
            }
        }
        Ok(output) => {
            log_tray_error(format!(
                "quit confirmation failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
            QuitChoice::Cancel
        }
        Err(error) => {
            log_tray_error(format!("quit confirmation failed: {error}"));
            QuitChoice::Cancel
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn confirm_active_quit_choice(_snapshot: &StatusSnapshot) -> QuitChoice {
    QuitChoice::QuitAfterTask
}

fn perform_tray_quit(app: AppHandle, strategy: TrayQuitStrategy) {
    thread::spawn(move || {
        let action = match strategy {
            TrayQuitStrategy::Immediate => InvokeAction::Shutdown,
            TrayQuitStrategy::Deferred => InvokeAction::ShutdownWhenIdle,
        };
        if let Err(error) = set_tray_enabled(false) {
            log_tray_error(format!("failed to update tray enabled state: {error:#}"));
        }
        if let Err(error) = invoke(action) {
            log_tray_error(format!("tray quit invoke failed: {error}"));
            run_on_main_thread(&app, error_snapshot(error.to_string()));
            return;
        }
        clear_tray_service_registration();
        if let Some(state) = app.try_state::<SharedTrayState>() {
            state.set_deferred_shutdown_requested(matches!(strategy, TrayQuitStrategy::Deferred));
        }
        if matches!(strategy, TrayQuitStrategy::Immediate) {
            let app_handle = app.clone();
            let _ = app.run_on_main_thread(move || {
                app_handle.exit(0);
            });
        }
    });
}

fn spawn_quit_flow(app: AppHandle) {
    thread::spawn(move || {
        let snapshot = app.state::<SharedTrayState>().snapshot();
        if !snapshot_indicates_active_task(&snapshot) {
            perform_tray_quit(app, TrayQuitStrategy::Immediate);
            return;
        }

        if !app.state::<SharedTrayState>().begin_quit_prompt() {
            return;
        }

        let choice = confirm_active_quit_choice(&snapshot);
        app.state::<SharedTrayState>().finish_quit_prompt();

        match choice {
            QuitChoice::ForceQuit => perform_tray_quit(app, TrayQuitStrategy::Immediate),
            QuitChoice::QuitAfterTask => perform_tray_quit(app, TrayQuitStrategy::Deferred),
            QuitChoice::Cancel => {}
        }
    });
}

fn toggled_checked_state(current: bool) -> bool {
    !current
}

fn check_action_label(recheck: bool) -> &'static str {
    if recheck {
        RECHECK_NOW_LABEL
    } else {
        CHECK_NOW_LABEL
    }
}

fn next_auto_create_enabled(app: &AppHandle) -> bool {
    toggled_checked_state(
        app.state::<MenuHandles>()
            .auto_create_item
            .is_checked()
            .unwrap_or(true),
    )
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            #[cfg(target_os = "macos")]
            app.set_activation_policy(ActivationPolicy::Accessory);

            #[cfg(not(target_os = "macos"))]
            let _ = ActivationPolicy::Regular;

            app.manage(SharedRenderState::default());
            app.manage(SharedTrayState::default());

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
            let auto_create_item = CheckMenuItem::with_id(
                app,
                "auto_create",
                "Auto Create",
                true,
                true,
                None::<&str>,
            )?;
            let launch_item =
                MenuItem::with_id(app, "launch", "Open Managed Codex", true, None::<&str>)?;
            let check_item =
                MenuItem::with_id(app, "check", check_action_label(false), true, None::<&str>)?;
            let rotate_item = MenuItem::with_id(app, "rotate", "Rotate Now", true, None::<&str>)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
            app.manage(MenuHandles {
                account_item: account_item.clone(),
                inventory_item: inventory_item.clone(),
                plan_item: plan_item.clone(),
                quota_item: quota_item.clone(),
                status_item: status_item.clone(),
                last_rotation_item: last_rotation_item.clone(),
                auto_create_item: auto_create_item.clone(),
                launch_item: launch_item.clone(),
                check_item: check_item.clone(),
                rotate_item: rotate_item.clone(),
            });

            let menu = Menu::with_items(
                app,
                &[
                    &account_item,
                    &plan_item,
                    &quota_item,
                    &status_item,
                    &last_rotation_item,
                    &inventory_item,
                    &check_item,
                    &auto_create_item,
                    &launch_item,
                    &rotate_item,
                    &quit_item,
                ],
            )?;

            TrayIconBuilder::with_id("main")
                .icon(build_tray_icon(None, false))
                .icon_as_template(true)
                .menu(&menu)
                .show_menu_on_left_click(true)
                .on_menu_event({
                    let app = app.handle().clone();
                    move |app_handle, event| match event.id.as_ref() {
                        "launch" => spawn_invoke(app.clone(), InvokeAction::OpenManaged),
                        "check" => {
                            let _ = app
                                .state::<MenuHandles>()
                                .check_item
                                .set_text(check_action_label(true));
                            spawn_invoke(app.clone(), InvokeAction::Refresh);
                        }
                        "rotate" => spawn_invoke(app.clone(), InvokeAction::Next),
                        "auto_create" => {
                            let enabled = next_auto_create_enabled(&app);
                            let _ = app
                                .state::<MenuHandles>()
                                .auto_create_item
                                .set_checked(enabled);
                            spawn_invoke(
                                app.clone(),
                                InvokeAction::SetAutoCreateEnabled { enabled },
                            );
                        }
                        "quit" => {
                            let _ = app_handle;
                            spawn_quit_flow(app.clone());
                        }
                        _ => {}
                    }
                })
                .build(app)?;
            log_tray_info("Tray started.");

            let stop = Arc::new(AtomicBool::new(false));

            let subscription_stop = stop.clone();
            let app_handle = app.handle().clone();
            spawn_subscription_loop_controlled(subscription_stop, move |snapshot| {
                run_on_main_thread(&app_handle, snapshot)
            });

            let refresh_stop = stop.clone();
            let refresh_app = app.handle().clone();
            let error_app = app.handle().clone();
            spawn_tray_refresh_loop_controlled(
                refresh_stop,
                move || {
                    log_tray_info("Tray rebuild detected; exiting current tray instance.");
                    let _ = refresh_app.run_on_main_thread({
                        let refresh_app = refresh_app.clone();
                        move || {
                            refresh_app.exit(0);
                        }
                    });
                },
                move |error| {
                    run_on_main_thread(
                        &error_app,
                        error_snapshot(format!("tray refresh failed: {error}")),
                    )
                },
            );
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("failed to run codex rotate tray");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toggled_checked_state_inverts_the_current_menu_value() {
        assert!(toggled_checked_state(false));
        assert!(!toggled_checked_state(true));
    }

    #[test]
    fn check_action_label_switches_to_recheck_after_first_manual_refresh() {
        assert_eq!(check_action_label(false), CHECK_NOW_LABEL);
        assert_eq!(check_action_label(true), RECHECK_NOW_LABEL);
    }

    #[test]
    fn progress_snapshot_requires_quit_confirmation() {
        let snapshot = StatusSnapshot {
            last_message: Some("Creating dev.48@astronlab.com.".to_string()),
            last_message_kind: Some(SnapshotMessageKind::Progress),
            ..StatusSnapshot::default()
        };

        assert!(snapshot_indicates_active_task(&snapshot));
    }

    #[test]
    fn deferred_shutdown_exits_only_after_disconnect_snapshot() {
        let state = SharedTrayState::default();
        state.set_deferred_shutdown_requested(true);

        assert!(!state.should_exit_after_deferred_shutdown(&StatusSnapshot {
            last_message: Some("watch healthy".to_string()),
            ..StatusSnapshot::default()
        }));
        assert!(state.should_exit_after_deferred_shutdown(&StatusSnapshot {
            last_message: Some("daemon disconnected: socket closed".to_string()),
            ..StatusSnapshot::default()
        }));
    }

    #[test]
    fn tray_icon_activity_badge_changes_pixels() {
        let (idle, _, _) = build_tray_icon_rgba(Some(42), false);
        let (busy, _, _) = build_tray_icon_rgba(Some(42), true);

        assert_ne!(idle, busy);
    }
}
