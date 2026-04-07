use codex_rotate_runtime::ipc::{
    daemon_is_reachable, invoke, subscribe, InvokeAction, StatusSnapshot,
};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
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
struct SharedRenderState {
    inner: Arc<Mutex<RenderState>>,
}

#[derive(Clone, Default)]
struct RenderState {
    last_rendered: Option<RenderedSnapshot>,
}

#[derive(Clone)]
struct MenuHandles {
    account_item: MenuItem<tauri::Wry>,
    inventory_item: MenuItem<tauri::Wry>,
    plan_item: MenuItem<tauri::Wry>,
    quota_item: MenuItem<tauri::Wry>,
    status_item: MenuItem<tauri::Wry>,
    last_rotation_item: MenuItem<tauri::Wry>,
    launch_item: MenuItem<tauri::Wry>,
    check_item: MenuItem<tauri::Wry>,
    rotate_item: MenuItem<tauri::Wry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RenderedSnapshot {
    account_text: String,
    inventory_text: String,
    plan_text: String,
    quota_text: String,
    status_text: String,
    rotation_text: String,
    tooltip_text: String,
    quota_percent: Option<u8>,
    launch_enabled: bool,
    check_enabled: bool,
    rotate_enabled: bool,
}

impl SharedRenderState {
    fn begin_render(&self, rendered: &RenderedSnapshot) -> bool {
        let mut state = self.inner.lock().expect("render state mutex");
        if state.last_rendered.as_ref() == Some(rendered) {
            return false;
        }
        state.last_rendered = Some(rendered.clone());
        true
    }
}

fn rendered_snapshot(snapshot: &StatusSnapshot) -> RenderedSnapshot {
    let rotation_text = match (
        snapshot.last_rotation_from_email.as_deref(),
        snapshot.last_rotation_to_email.as_deref(),
    ) {
        (Some(from), Some(to)) if from != to => format!("Last rotation: {from} -> {to}"),
        (_, Some(to)) => format!("Last rotation: {to}"),
        _ => "Last rotation: none".to_string(),
    };

    RenderedSnapshot {
        account_text: format!(
            "Account: {}",
            snapshot.current_email.as_deref().unwrap_or("unknown")
        ),
        inventory_text: match snapshot.inventory_count {
            Some(count) => format!("Inventory: {count} account(s)"),
            None => "Inventory: unknown".to_string(),
        },
        plan_text: format!(
            "Plan: {}",
            snapshot.current_plan.as_deref().unwrap_or("unknown")
        ),
        quota_text: format!(
            "Quota: {}",
            snapshot.current_quota.as_deref().unwrap_or("unknown")
        ),
        status_text: format!(
            "Status: {}",
            snapshot.last_message.as_deref().unwrap_or("starting")
        ),
        rotation_text,
        tooltip_text: match snapshot.current_quota_percent {
            Some(percent) => format!("Codex Rotate\nQuota: {percent}%\nClick for status"),
            None => "Codex Rotate\nClick for status".to_string(),
        },
        quota_percent: snapshot.current_quota_percent,
        launch_enabled: snapshot.capabilities.managed_launch,
        check_enabled: snapshot.capabilities.quota_watch,
        rotate_enabled: snapshot.capabilities.live_account_sync,
    }
}

fn update_snapshot(app: &AppHandle, snapshot: StatusSnapshot) {
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
        let _ = menu.launch_item.set_enabled(rendered.launch_enabled);
        let _ = menu.check_item.set_enabled(rendered.check_enabled);
        let _ = menu.rotate_item.set_enabled(rendered.rotate_enabled);
    }

    if let Some(tray) = app.tray_by_id("main") {
        let _ = tray.set_icon(Some(build_tray_icon(rendered.quota_percent)));
        let _ = tray.set_title(Option::<String>::None);
        let _ = tray.set_tooltip(Some(rendered.tooltip_text));
    }
}

fn error_snapshot(message: impl Into<String>) -> StatusSnapshot {
    StatusSnapshot {
        last_message: Some(message.into()),
        ..StatusSnapshot::default()
    }
}

fn run_on_main_thread(app: &AppHandle, snapshot: StatusSnapshot) {
    let app_handle = app.clone();
    let _ = app.run_on_main_thread(move || update_snapshot(&app_handle, snapshot));
}

fn resolve_cli_binary() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("CODEX_ROTATE_CLI_BIN").map(PathBuf::from) {
        return Ok(path);
    }

    let current_exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let current_dir = current_exe
        .parent()
        .ok_or_else(|| "Failed to resolve the tray binary directory.".to_string())?;
    let binary_name = if cfg!(windows) {
        "codex-rotate-cli.exe"
    } else {
        "codex-rotate-cli"
    };
    Ok(current_dir.join(binary_name))
}

fn ensure_daemon_running() -> Result<(), String> {
    if daemon_is_reachable() {
        return Ok(());
    }

    let cli_binary = resolve_cli_binary()?;
    let _child = Command::new(&cli_binary)
        .arg("daemon")
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("Failed to launch {}: {}", cli_binary.display(), error))?;

    for _ in 0..40 {
        if daemon_is_reachable() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(250));
    }

    Err("Timed out waiting for the Codex Rotate daemon to start.".to_string())
}

fn spawn_subscription_loop(app: AppHandle) {
    thread::spawn(move || loop {
        if let Err(error) = ensure_daemon_running() {
            run_on_main_thread(
                &app,
                error_snapshot(format!("daemon start failed: {}", error)),
            );
            thread::sleep(Duration::from_secs(2));
            continue;
        }

        let mut subscription = match subscribe() {
            Ok(subscription) => subscription,
            Err(error) => {
                run_on_main_thread(
                    &app,
                    error_snapshot(format!("daemon subscribe failed: {}", error)),
                );
                thread::sleep(Duration::from_secs(2));
                continue;
            }
        };

        loop {
            match subscription.recv() {
                Ok(snapshot) => run_on_main_thread(&app, snapshot),
                Err(error) => {
                    run_on_main_thread(
                        &app,
                        error_snapshot(format!("daemon disconnected: {}", error)),
                    );
                    thread::sleep(Duration::from_secs(1));
                    break;
                }
            }
        }
    });
}

fn spawn_invoke(app: AppHandle, action: InvokeAction) {
    thread::spawn(move || {
        if let Err(error) = invoke(action) {
            run_on_main_thread(&app, error_snapshot(error.to_string()));
        }
    });
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            #[cfg(target_os = "macos")]
            app.set_activation_policy(ActivationPolicy::Accessory);

            #[cfg(not(target_os = "macos"))]
            let _ = ActivationPolicy::Regular;

            app.manage(SharedRenderState::default());

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
                        "launch" => spawn_invoke(app.clone(), InvokeAction::OpenManaged),
                        "check" => spawn_invoke(app.clone(), InvokeAction::Refresh),
                        "rotate" => spawn_invoke(app.clone(), InvokeAction::Next),
                        "quit" => app_handle.exit(0),
                        _ => {}
                    }
                })
                .build(app)?;

            spawn_subscription_loop(app.handle().clone());
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("failed to run codex rotate tray");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rendered_snapshot() -> RenderedSnapshot {
        RenderedSnapshot {
            account_text: "Account: dev.1@astronlab.com".to_string(),
            inventory_text: "Inventory: 3 account(s)".to_string(),
            plan_text: "Plan: free".to_string(),
            quota_text: "Quota: 5h 80% left".to_string(),
            status_text: "Status: watch healthy".to_string(),
            rotation_text: "Last rotation: dev.0@astronlab.com -> dev.1@astronlab.com".to_string(),
            tooltip_text: "Codex Rotate\nQuota: 80%\nClick for status".to_string(),
            quota_percent: Some(80),
            launch_enabled: true,
            check_enabled: true,
            rotate_enabled: true,
        }
    }

    #[test]
    fn render_state_dedups_identical_rendered_snapshots() {
        let render_state = SharedRenderState::default();
        let rendered = sample_rendered_snapshot();
        assert!(render_state.begin_render(&rendered));
        assert!(!render_state.begin_render(&rendered));
    }

    #[test]
    fn render_state_allows_changed_rendered_snapshots() {
        let render_state = SharedRenderState::default();
        let rendered = sample_rendered_snapshot();
        let mut changed = rendered.clone();
        changed.status_text = "Status: rotated".to_string();
        assert!(render_state.begin_render(&rendered));
        assert!(render_state.begin_render(&changed));
    }
}
