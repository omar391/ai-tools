use super::*;

impl<'de> Deserialize<'de> for BridgeLoginAttemptResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Value::deserialize(deserializer)?;
        Ok(normalize_bridge_login_attempt_result(raw))
    }
}

pub(super) fn normalize_bridge_login_attempt_result(raw: Value) -> BridgeLoginAttemptResult {
    let Value::Object(record) = raw else {
        return BridgeLoginAttemptResult::default();
    };
    let wrapped_result = record.get("result").cloned();
    let result = wrapped_result
        .or_else(|| {
            looks_like_fast_browser_run_result_record(&record)
                .then(|| Value::Object(record.clone()))
        })
        .and_then(normalize_bridge_fast_browser_run_result);
    let browser_fingerprint = record
        .get("browser_fingerprint")
        .or_else(|| record.get("browserFingerprint"))
        .cloned();
    let error_message = read_string_value(&record, "error_message")
        .or_else(|| read_string_value(&record, "errorMessage"));
    BridgeLoginAttemptResult {
        result,
        browser_fingerprint,
        error_message,
    }
}

fn looks_like_fast_browser_run_result_record(record: &Map<String, Value>) -> bool {
    record.contains_key("state")
        || record.contains_key("output")
        || record.contains_key("observability")
        || record.contains_key("finalUrl")
        || record.contains_key("status")
        || record.contains_key("ok")
        || record.contains_key("page")
        || record.contains_key("current")
        || record.contains_key("recent_events")
}
fn normalize_bridge_fast_browser_run_result(raw: Value) -> Option<FastBrowserRunResult> {
    let raw = match raw {
        Value::String(value) => serde_json::from_str::<Value>(value.trim()).ok()?,
        other => other,
    };
    let record = raw.as_object()?;
    Some(hydrate_fast_browser_run_result_from_observability(
        FastBrowserRunResult {
            state: record
                .get("state")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            output: record.get("output").cloned(),
            recent_events: record
                .get("recentEvents")
                .or_else(|| record.get("recent_events"))
                .or_else(|| record.get("events"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            final_url: read_string_value(record, "finalUrl")
                .or_else(|| read_string_value(record, "final_url")),
            page: record.get("page").cloned(),
            current: record.get("current").cloned(),
            observability: record.get("observability").and_then(|value| {
                let observability = value.as_object()?;
                Some(FastBrowserRunObservability {
                    run_path: read_string_value(observability, "runPath")
                        .or_else(|| read_string_value(observability, "run_path")),
                    status_path: read_string_value(observability, "statusPath")
                        .or_else(|| read_string_value(observability, "status_path")),
                })
            }),
        },
    ))
}

fn hydrate_fast_browser_run_result_from_observability(
    mut result: FastBrowserRunResult,
) -> FastBrowserRunResult {
    if result.final_url.is_some() && result.output.is_some() && result.page.is_some() {
        return result;
    }

    let run_path_candidates = [
        result
            .observability
            .as_ref()
            .and_then(|value| value.run_path.as_deref()),
        result
            .observability
            .as_ref()
            .and_then(|value| value.status_path.as_deref()),
    ];

    for run_path in run_path_candidates.into_iter().flatten() {
        let Ok(contents) = fs::read_to_string(run_path) else {
            continue;
        };
        let Ok(snapshot) = serde_json::from_str::<Value>(&contents) else {
            continue;
        };
        let Some(record) = snapshot.as_object() else {
            continue;
        };
        if result.final_url.is_none() {
            result.final_url = read_string_value(record, "finalUrl")
                .or_else(|| read_string_value(record, "final_url"))
                .or_else(|| {
                    record
                        .get("page")
                        .and_then(Value::as_object)
                        .and_then(|page| read_string_value(page, "url"))
                });
        }
        if result.output.is_none() {
            result.output = record.get("output").cloned();
        }
        if result.recent_events.is_none() {
            result.recent_events = record
                .get("recentEvents")
                .or_else(|| record.get("recent_events"))
                .or_else(|| record.get("events"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
        }
        if result.page.is_none() {
            result.page = record.get("page").cloned();
        }
        if result.current.is_none() {
            result.current = record.get("current").cloned();
        }
        break;
    }

    result
}

fn normalize_codex_rotate_auth_flow_session(raw: &Value) -> Option<CodexRotateAuthFlowSession> {
    let record = raw.as_object()?;
    let session = CodexRotateAuthFlowSession {
        auth_url: read_string_value(record, "auth_url"),
        callback_url: read_string_value(record, "callback_url"),
        callback_port: read_u16_value(record, "callback_port"),
        device_code: read_string_value(record, "device_code"),
        session_dir: read_string_value(record, "session_dir"),
        codex_home_path: read_string_value(record, "codex_home_path"),
        auth_file_path: read_string_value(record, "auth_file_path"),
        pid: read_u32_value(record, "pid"),
        stdout_path: read_string_value(record, "stdout_path"),
        stderr_path: read_string_value(record, "stderr_path"),
        exit_path: read_string_value(record, "exit_path"),
    };
    if session.auth_url.is_none()
        && session.session_dir.is_none()
        && session.codex_home_path.is_none()
        && session.auth_file_path.is_none()
        && session.stdout_path.is_none()
        && session.stderr_path.is_none()
        && session.exit_path.is_none()
    {
        return None;
    }
    Some(session)
}

pub(super) struct CompleteCodexLoginArgs<'a> {
    pub(super) profile_name: &'a str,
    pub(super) email: &'a str,
    pub(super) account_login_locator: Option<&'a CodexRotateSecretLocator>,
    pub(super) workflow_ref: Option<&'a str>,
    pub(super) codex_bin: Option<&'a str>,
    pub(super) workflow_run_stamp: Option<&'a str>,
    pub(super) skip_locator_preflight: Option<bool>,
    pub(super) prefer_signup_recovery: Option<bool>,
    pub(super) prefer_password_login: Option<bool>,
    pub(super) password: Option<&'a str>,
    pub(super) treat_final_add_phone_as_environment_blocker: Option<bool>,
    pub(super) birth_date: Option<&'a AdultBirthDate>,
    pub(super) persona_profile: Option<PersonaProfile>,
    pub(super) progress: Option<AutomationProgressCallback>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CompleteCodexLoginOutcome {
    pub(super) verified_account_email: Option<String>,
    pub(super) codex_session: Option<CodexRotateAuthFlowSession>,
    pub(super) browser_fingerprint: Option<Value>,
}

pub(super) fn run_complete_codex_login(
    args: CompleteCodexLoginArgs<'_>,
) -> Result<CompleteCodexLoginOutcome> {
    let CompleteCodexLoginArgs {
        profile_name,
        email,
        account_login_locator,
        workflow_ref,
        codex_bin,
        workflow_run_stamp,
        skip_locator_preflight,
        prefer_signup_recovery,
        prefer_password_login,
        password,
        treat_final_add_phone_as_environment_blocker,
        birth_date,
        persona_profile,
        progress,
    } = args;
    let workflow_defaults = resolve_login_workflow_defaults(workflow_ref)?;
    let fallback_birth_date;
    let birth_date = match birth_date {
        Some(value) => value,
        None => {
            fallback_birth_date = workflow_defaults.birth_date.clone();
            &fallback_birth_date
        }
    };
    let workflow_ref = workflow_defaults.workflow_ref;
    let resolved_codex_bin;
    let codex_bin_to_use = match codex_bin {
        Some(value) => value,
        None => {
            resolved_codex_bin = crate::workflow::codex_bin();
            &resolved_codex_bin
        }
    };
    let wrapped_codex_bin = ensure_managed_browser_wrapper(profile_name, codex_bin_to_use)?;
    let wrapped_codex_bin = wrapped_codex_bin.to_string_lossy().into_owned();
    match account_login_locator {
        Some(_) if skip_locator_preflight == Some(true) => report_progress(
            progress.as_ref(),
            format!(
                "Using a freshly generated OpenAI password for {email}; attempting password login first."
            ),
        ),
        None if password.is_some() => report_progress(
            progress.as_ref(),
            format!(
                "Bitwarden is unavailable for {email}; continuing with the generated OpenAI password without a stored vault secret."
            ),
        ),
        Some(_) => report_progress(
            progress.as_ref(),
            format!(
                "An OpenAI login secret locator is configured for {email}; attempting password login first when a usable secret resolves."
            ),
        ),
        None => report_progress(
            progress.as_ref(),
            format!("No stored OpenAI login secret was found for {email}; using one-time-code recovery."),
        ),
    }

    let persona_paths = current_persona_managed_profile_dir(true)?;
    let profile_dir_str = persona_paths.as_ref().map(|path| path.to_string_lossy());

    let mut allow_signup_recovery = prefer_signup_recovery.unwrap_or(false);
    let mut codex_session: Option<CodexRotateAuthFlowSession> = None;
    let result = (|| -> Result<CompleteCodexLoginOutcome> {
        let mut max_attempts = DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS;
        let mut attempt = 1usize;
        'attempts: while attempt <= max_attempts {
            cancel::check_canceled()?;
            report_progress(
                progress.as_ref(),
                if attempt == 1 {
                    format!("Completing Codex login in managed profile \"{profile_name}\".")
                } else {
                    format!(
                        "Retrying Codex login in managed profile \"{profile_name}\" (attempt {attempt}/{max_attempts})."
                    )
                },
            );

            for replay_pass in 1..=DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES {
                cancel::check_canceled()?;
                let login_workflow_run_stamp = workflow_run_stamp
                    .map(|stamp| format!("{stamp}-codex-login-{attempt}-{replay_pass}"));
                let options = BridgeLoginOptions {
                    profile_dir: profile_dir_str.as_ref().map(|s| s.as_ref()),
                    codex_bin: Some(wrapped_codex_bin.as_str()),
                    workflow_ref: Some(workflow_ref.as_str()),
                    workflow_run_stamp: login_workflow_run_stamp.as_deref(),
                    skip_locator_preflight,
                    prefer_signup_recovery: Some(allow_signup_recovery),
                    prefer_password_login,
                    password,
                    full_name: Some(workflow_defaults.full_name.as_str()),
                    birth_month: Some(birth_date.birth_month),
                    birth_day: Some(birth_date.birth_day),
                    birth_year: Some(birth_date.birth_year),
                    codex_session: codex_session.as_ref(),
                    persona_profile: persona_profile.clone(),
                };
                let attempt_result_raw: Value = run_automation_bridge_with_progress(
                    "complete-codex-login-attempt",
                    BridgeCompleteLoginAttemptPayload {
                        profile_name,
                        profile_dir: profile_dir_str.as_ref().map(|s| s.as_ref()),
                        email,
                        account_login_locator,
                        options: Some(options),
                    },
                    progress.clone(),
                )?;
                maybe_debug_codex_auth_flow_raw(workflow_ref.as_str(), email, &attempt_result_raw);
                let attempt_result = normalize_bridge_login_attempt_result(attempt_result_raw);
                let bridge_error_message = attempt_result.error_message.clone();
                let flow = attempt_result
                    .result
                    .as_ref()
                    .map(read_codex_rotate_auth_flow_summary)
                    .unwrap_or_default();
                maybe_debug_codex_auth_flow_result(
                    workflow_ref.as_str(),
                    email,
                    &attempt_result,
                    &flow,
                );
                if let Some(session) = attempt_result
                    .result
                    .as_ref()
                    .and_then(read_codex_rotate_auth_flow_session)
                    .or_else(|| flow.codex_session.clone())
                {
                    codex_session = Some(session);
                }
                let current_url = flow
                    .current_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let next_action = flow
                    .next_action
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let replay_reason = flow
                    .replay_reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let retry_reason = flow
                    .retry_reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let error_message = flow
                    .error_message
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .or_else(|| {
                        bridge_error_message
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                    });

                if flow.saw_oauth_consent == Some(true)
                    || flow.existing_account_prompt == Some(true)
                    || replay_reason.is_some_and(|value| value != "auth_prompt")
                {
                    allow_signup_recovery = false;
                }

                if next_action == Some("fail_invalid_credentials") {
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!("OpenAI rejected the stored password for {email}.")
                    )));
                }

                if next_action == Some("skip_account") {
                    return Err(anyhow::Error::new(WorkflowSkipAccountError::new(
                        login_error_message(
                            error_message,
                            format!(
                                "The workflow requested skipping {email}{}.",
                                current_url
                                    .map(|value| format!(" ({value})"))
                                    .unwrap_or_default()
                            ),
                        ),
                    )));
                }

                if next_action == Some("replay_auth_url")
                    && replay_pass < DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES
                {
                    let replay_reason_label =
                        format_retry_reason_label(replay_reason, "the next auth step");
                    report_progress(
                        progress.as_ref(),
                        format!(
                            "OpenAI still needs {replay_reason_label} for {email}{}. Replaying the workflow-owned Codex auth session in managed profile \"{profile_name}\" ({}/{}).",
                            current_url
                                .map(|value| format!(" ({value})"))
                                .unwrap_or_default(),
                            replay_pass + 1,
                            DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES
                        ),
                    );
                    cancel::sleep_with_cancellation(Duration::from_millis(1_000))?;
                    continue;
                }

                if next_action == Some("retry_attempt") {
                    if retry_reason == Some("final_add_phone")
                        && (stop_on_final_add_phone_retry_exhaustion()
                            || treat_final_add_phone_as_environment_blocker == Some(true))
                    {
                        return Err(final_add_phone_short_circuit_error(
                            email,
                            current_url,
                            error_message,
                        ));
                    }
                    max_attempts = max_attempts.max(codex_login_max_attempts(retry_reason));
                    if attempt < max_attempts {
                        let delay_ms = codex_login_retry_delay_ms(retry_reason, attempt);
                        let reset_session =
                            should_reset_codex_login_session_for_retry(retry_reason, attempt);
                        if reset_session {
                            codex_session = None;
                        }
                        let retry_reason_label =
                            format_retry_reason_label(retry_reason, "needs another retry");
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "OpenAI {retry_reason_label} for {email}{}. {}Waiting {}s before retrying.",
                                current_url
                                    .map(|value| format!(" ({value})"))
                                    .unwrap_or_default(),
                                if reset_session {
                                    "Starting a fresh Codex auth session. "
                                } else {
                                    ""
                                },
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                    if should_skip_account_after_retry_exhaustion(retry_reason) {
                        return Err(anyhow::Error::new(WorkflowSkipAccountError::new(
                            login_error_message(
                                error_message,
                                format!(
                                    "The workflow requested skipping {email} after exhausting final add-phone retries{}.",
                                    current_url
                                        .map(|value| format!(" ({value})"))
                                        .unwrap_or_default()
                                ),
                            ),
                        )));
                    }
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!("OpenAI could not complete the Codex login for {email}.")
                    )));
                }

                if state_mismatch_in_login_flow(&flow, error_message) {
                    if attempt < max_attempts {
                        let delay_ms = codex_login_retry_delay_ms(Some("state_mismatch"), attempt);
                        codex_session = None;
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "OpenAI returned a state mismatch during the Codex callback for {email}{}. Starting a fresh Codex auth session and retrying in {}s.",
                                current_url
                                    .map(|value| format!(" ({value})"))
                                    .unwrap_or_default(),
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!(
                            "OpenAI returned a state mismatch during the Codex callback for {email}{}.",
                            current_url
                                .map(|value| format!(" ({value})"))
                                .unwrap_or_default()
                        )
                    )));
                }

                if let Some(message) = error_message {
                    if is_retryable_codex_login_workflow_error_message(message)
                        && attempt < max_attempts
                    {
                        let delay_ms = codex_login_retry_delay_ms(
                            Some("verification_artifact_pending"),
                            attempt,
                        );
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "OpenAI verification is not ready for {email}. Waiting {}s before retrying the same managed-profile flow.",
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                    if is_device_auth_rate_limited(message) && attempt < max_attempts {
                        let delay_ms =
                            codex_login_retry_delay_ms(Some("device_auth_rate_limit"), attempt);
                        let reset_session = should_reset_device_auth_session_for_rate_limit(
                            message,
                            codex_session.as_ref(),
                        );
                        if reset_session {
                            codex_session = None;
                        }
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "Codex device authorization is rate limited for {email}. {}Waiting {}s before retrying.",
                                if reset_session {
                                    ""
                                } else {
                                    "Reusing the existing device code session when retrying. "
                                },
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                }

                if flow.callback_complete != Some(true) && flow.success != Some(true) {
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!(
                            "Codex browser login did not reach the callback for {email}{}.",
                            current_url
                                .map(|value| format!(" ({value})"))
                                .unwrap_or_default()
                        )
                    )));
                }
                if flow.codex_login_exit_ok == Some(false) && !login_cancelled_after_callback(&flow)
                {
                    let detail = flow
                        .codex_login_stderr_tail
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("");
                    return Err(anyhow!(
                        "\"codex login\" did not exit cleanly for {email}.{}",
                        if detail.is_empty() {
                            String::new()
                        } else {
                            format!("\n{detail}")
                        }
                    ));
                }
                promote_codex_auth_from_session(
                    codex_session.as_ref().or(flow.codex_session.as_ref()),
                )?;
                return Ok(CompleteCodexLoginOutcome {
                    verified_account_email: flow.verified_account_email.clone(),
                    codex_session: codex_session.clone().or(flow.codex_session.clone()),
                    browser_fingerprint: attempt_result.browser_fingerprint.clone(),
                });
            }
            attempt += 1;
        }
        Err(anyhow!(
            "Codex browser login exhausted all retry attempts for {email}."
        ))
    })();
    cancel_codex_browser_login_session(codex_session.as_ref());
    result
}

pub fn cmd_generate_browser_fingerprint(
    persona_id: &str,
    profile: &PersonaProfile,
) -> Result<Value> {
    run_automation_bridge::<_, Value>(
        "generate-browser-fingerprint",
        BridgeGenerateFingerprintPayload {
            persona_id,
            options: BridgeGenerateFingerprintOptions {
                user_agent: Some(&profile.user_agent),
                screen_width: Some(profile.screen_width),
                screen_height: Some(profile.screen_height),
                os_family: serde_json::from_str(&format!("\"{}\"", profile.os_family))?,
            },
        },
    )
}

pub(super) fn resolve_login_workflow_defaults(
    workflow_ref: Option<&str>,
) -> Result<LoginWorkflowDefaults> {
    let paths = resolve_paths()?;
    let workflow_metadata = read_workflow_file_metadata(&paths.account_flow_file)?;
    let workflow_ref = workflow_ref
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| workflow_metadata.workflow_ref.clone())
        .ok_or_else(|| {
            anyhow!(
                "Could not resolve a codex-rotate workflow ref from {}.",
                paths.account_flow_file.display()
            )
        })?;
    let full_name = workflow_metadata.default_full_name.clone().ok_or_else(|| {
        anyhow!(
            "Workflow {} is missing input.schema.document.properties.full_name.default.",
            paths.account_flow_file.display()
        )
    })?;
    let birth_date = workflow_metadata.default_birth_date().ok_or_else(|| {
        anyhow!(
            "Workflow {} is missing one or more birth-date defaults.",
            paths.account_flow_file.display()
        )
    })?;

    Ok(LoginWorkflowDefaults {
        workflow_ref,
        full_name,
        birth_date,
    })
}

fn read_string_value(record: &Map<String, Value>, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn read_bool_value(record: &Map<String, Value>, field: &str) -> Option<bool> {
    match record.get(field) {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::String(value)) => match value.trim() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn read_i32_value(record: &Map<String, Value>, field: &str) -> Option<i32> {
    match record.get(field) {
        Some(Value::Number(value)) => value.as_i64().and_then(|value| i32::try_from(value).ok()),
        Some(Value::String(value)) => value.trim().parse::<i32>().ok(),
        _ => None,
    }
}

fn read_u16_value(record: &Map<String, Value>, field: &str) -> Option<u16> {
    match record.get(field) {
        Some(Value::Number(value)) => value.as_u64().and_then(|value| u16::try_from(value).ok()),
        Some(Value::String(value)) => value.trim().parse::<u16>().ok(),
        _ => None,
    }
}

fn read_u32_value(record: &Map<String, Value>, field: &str) -> Option<u32> {
    match record.get(field) {
        Some(Value::Number(value)) => value.as_u64().and_then(|value| u32::try_from(value).ok()),
        Some(Value::String(value)) => value.trim().parse::<u32>().ok(),
        _ => None,
    }
}

fn merge_codex_rotate_auth_flow_session(
    primary: CodexRotateAuthFlowSession,
    fallback: CodexRotateAuthFlowSession,
) -> CodexRotateAuthFlowSession {
    CodexRotateAuthFlowSession {
        auth_url: primary.auth_url.or(fallback.auth_url),
        callback_url: primary.callback_url.or(fallback.callback_url),
        callback_port: primary.callback_port.or(fallback.callback_port),
        device_code: primary.device_code.or(fallback.device_code),
        session_dir: primary.session_dir.or(fallback.session_dir),
        codex_home_path: primary.codex_home_path.or(fallback.codex_home_path),
        auth_file_path: primary.auth_file_path.or(fallback.auth_file_path),
        pid: primary.pid.or(fallback.pid),
        stdout_path: primary.stdout_path.or(fallback.stdout_path),
        stderr_path: primary.stderr_path.or(fallback.stderr_path),
        exit_path: primary.exit_path.or(fallback.exit_path),
    }
}

pub(super) fn read_codex_rotate_auth_flow_summary(
    result: &FastBrowserRunResult,
) -> CodexRotateAuthFlowSummary {
    let mut summary = if let Some(record) = read_codex_rotate_auth_flow_summary_record(result) {
        CodexRotateAuthFlowSummary {
            stage: read_string_value(record, "stage"),
            current_url: read_string_value(record, "current_url"),
            headline: read_string_value(record, "headline"),
            callback_complete: read_bool_value(record, "callback_complete"),
            success: read_bool_value(record, "success"),
            account_ready: read_bool_value(record, "account_ready"),
            needs_email_verification: read_bool_value(record, "needs_email_verification"),
            follow_up_step: read_bool_value(record, "follow_up_step"),
            retryable_timeout: read_bool_value(record, "retryable_timeout"),
            session_ended: read_bool_value(record, "session_ended"),
            existing_account_prompt: read_bool_value(record, "existing_account_prompt"),
            username_not_found: read_bool_value(record, "username_not_found"),
            invalid_credentials: read_bool_value(record, "invalid_credentials"),
            rate_limit_exceeded: read_bool_value(record, "rate_limit_exceeded"),
            anti_bot_gate: read_bool_value(record, "anti_bot_gate"),
            auth_prompt: read_bool_value(record, "auth_prompt"),
            consent_blocked: read_bool_value(record, "consent_blocked"),
            consent_error: read_string_value(record, "consent_error"),
            next_action: read_string_value(record, "next_action"),
            replay_reason: read_string_value(record, "replay_reason"),
            retry_reason: read_string_value(record, "retry_reason"),
            error_message: read_string_value(record, "error_message"),
            verified_account_email: read_string_value(record, "verified_account_email"),
            codex_session: record
                .get("codex_session")
                .and_then(normalize_codex_rotate_auth_flow_session),
            codex_login_exit_ok: read_bool_value(record, "codex_login_exit_ok"),
            codex_login_exit_code: read_i32_value(record, "codex_login_exit_code"),
            codex_login_stdout_tail: read_string_value(record, "codex_login_stdout_tail"),
            codex_login_stderr_tail: read_string_value(record, "codex_login_stderr_tail"),
            saw_oauth_consent: read_bool_value(record, "saw_oauth_consent"),
        }
    } else {
        CodexRotateAuthFlowSummary::default()
    };

    if let Some(metadata) = read_codex_rotate_auth_flow_summary_from_result_metadata(result) {
        if metadata.success == Some(true) || metadata.callback_complete == Some(true) {
            summary.stage = metadata.stage.or(summary.stage);
            summary.current_url = metadata.current_url.or(summary.current_url);
            summary.headline = metadata.headline.or(summary.headline);
            summary.callback_complete = metadata.callback_complete.or(summary.callback_complete);
            summary.success = metadata.success.or(summary.success);
            summary.next_action = metadata.next_action.or(summary.next_action);
        } else {
            summary.stage = summary.stage.or(metadata.stage);
            summary.current_url = summary.current_url.or(metadata.current_url);
            summary.headline = summary.headline.or(metadata.headline);
            summary.callback_complete = summary.callback_complete.or(metadata.callback_complete);
            summary.success = summary.success.or(metadata.success);
            summary.next_action = summary.next_action.or(metadata.next_action);
        }
    }

    summary
}

pub(super) fn read_codex_rotate_auth_flow_summary_from_result_metadata(
    result: &FastBrowserRunResult,
) -> Option<CodexRotateAuthFlowSummary> {
    let step_metadata = read_codex_rotate_auth_flow_step_metadata(result);
    let current_url = result
        .final_url
        .clone()
        .or_else(|| {
            result
                .page
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|record| read_string_value(record, "url"))
        })
        .or_else(|| {
            result
                .current
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|record| {
                    record
                        .get("details")
                        .and_then(Value::as_object)
                        .and_then(|details| read_string_value(details, "current_url"))
                })
        })
        .or_else(|| step_metadata.current_url.clone());
    let headline = result
        .current
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|record| {
            record
                .get("details")
                .and_then(Value::as_object)
                .and_then(|details| read_string_value(details, "headline"))
        })
        .or_else(|| {
            result
                .page
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|record| {
                    read_string_value(record, "title").or_else(|| {
                        read_string_value(record, "text").and_then(|text| {
                            text.lines()
                                .map(str::trim)
                                .find(|line| !line.is_empty())
                                .map(str::to_string)
                        })
                    })
                })
        })
        .or_else(|| step_metadata.headline.clone());
    let page_text = result
        .page
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "text"))
        .or_else(|| step_metadata.page_text.clone())
        .unwrap_or_default()
        .to_lowercase();
    let headline_text = headline
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    let callback_url = current_url
        .as_deref()
        .map(str::to_lowercase)
        .unwrap_or_default();
    let localhost_callback = callback_url.starts_with("http://localhost")
        || callback_url.starts_with("http://127.0.0.1");
    let device_auth_callback = callback_url.contains("auth.openai.com/deviceauth/callback")
        && (headline_text.contains("signed in to codex")
            || headline_text.contains("you may now close this page")
            || page_text.contains("signed in to codex")
            || page_text.contains("you may now close this page"));
    let success_copy = headline_text.contains("signed in to codex")
        || headline_text.contains("you may now close this page")
        || page_text.contains("signed in to codex")
        || page_text.contains("you may now close this page");
    if current_url.is_none() && headline.is_none() {
        return None;
    }
    Some(CodexRotateAuthFlowSummary {
        stage: if localhost_callback || device_auth_callback || success_copy {
            Some("success".to_string())
        } else {
            None
        },
        current_url,
        headline,
        callback_complete: (localhost_callback || device_auth_callback || success_copy)
            .then_some(true),
        success: (localhost_callback || device_auth_callback || success_copy).then_some(true),
        next_action: (localhost_callback || device_auth_callback || success_copy)
            .then_some("complete".to_string()),
        ..CodexRotateAuthFlowSummary::default()
    })
}

#[derive(Clone, Debug, Default)]
struct FastBrowserStepMetadata {
    current_url: Option<String>,
    headline: Option<String>,
    page_text: Option<String>,
}

fn read_codex_rotate_auth_flow_step_metadata(
    result: &FastBrowserRunResult,
) -> FastBrowserStepMetadata {
    let mut metadata = FastBrowserStepMetadata::default();
    let Some(state) = result.state.as_ref() else {
        return metadata;
    };

    for step in state.steps.values() {
        let Some(action) = step.action.as_ref().and_then(Value::as_object) else {
            continue;
        };
        let url = read_string_value(action, "url")
            .or_else(|| read_string_value(action, "current_url"))
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "current_url"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "current_url"))
            });
        let headline = read_string_value(action, "headline")
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "headline"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "headline"))
            });
        let page_text = read_string_value(action, "text")
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "text"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "text"))
            });
        let success = read_bool_value(action, "success")
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_bool_value(record, "success"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_bool_value(record, "success"))
            });

        let url_text = url.as_deref().map(str::to_lowercase).unwrap_or_default();
        let headline_text = headline
            .as_deref()
            .map(str::to_lowercase)
            .unwrap_or_default();
        let page_text_lc = page_text
            .as_deref()
            .map(str::to_lowercase)
            .unwrap_or_default();
        let looks_like_callback_success = url_text.contains("auth.openai.com/deviceauth/callback")
            && (success == Some(true)
                || headline_text.contains("signed in to codex")
                || headline_text.contains("you may now close this page")
                || page_text_lc.contains("signed in to codex")
                || page_text_lc.contains("you may now close this page"));

        if looks_like_callback_success {
            return FastBrowserStepMetadata {
                current_url: url,
                headline,
                page_text,
            };
        }

        metadata.current_url = metadata.current_url.or(url);
        metadata.headline = metadata.headline.or(headline);
        metadata.page_text = metadata.page_text.or(page_text);
    }

    metadata
}

pub(super) fn read_codex_rotate_auth_flow_summary_record(
    result: &FastBrowserRunResult,
) -> Option<&Map<String, Value>> {
    result
        .output
        .as_ref()
        .and_then(Value::as_object)
        .or_else(|| {
            const FINAL_SUMMARY_STEP_IDS: [&str; 3] = [
                "finalize_selected_flow",
                "finalize_flow_summary",
                "finalize_device_auth_tail_summary",
            ];

            let state = result.state.as_ref()?;
            FINAL_SUMMARY_STEP_IDS.iter().find_map(|step_id| {
                state
                    .steps
                    .get(*step_id)
                    .and_then(|step| step.action.as_ref())
                    .and_then(read_codex_rotate_auth_flow_summary_action_record)
            })
        })
        .or_else(|| read_codex_rotate_auth_flow_summary_record_from_recent_events(result))
}

pub(super) fn read_codex_rotate_auth_flow_summary_action_record(
    action: &Value,
) -> Option<&Map<String, Value>> {
    let record = action.as_object()?;
    record
        .get("value")
        .and_then(Value::as_object)
        .filter(|value| looks_like_codex_rotate_auth_flow_summary_record(value))
        .or_else(|| looks_like_codex_rotate_auth_flow_summary_record(record).then_some(record))
}

fn looks_like_codex_rotate_auth_flow_summary_record(record: &Map<String, Value>) -> bool {
    record.contains_key("callback_complete")
        || record.contains_key("success")
        || record.contains_key("next_action")
        || record.contains_key("retry_reason")
        || record.contains_key("replay_reason")
        || record.contains_key("error_message")
        || record.contains_key("verified_account_email")
        || record.contains_key("codex_session")
}

pub(super) fn read_codex_rotate_auth_flow_summary_record_from_recent_events(
    result: &FastBrowserRunResult,
) -> Option<&Map<String, Value>> {
    const FINAL_SUMMARY_STEP_IDS: [&str; 3] = [
        "finalize_selected_flow",
        "finalize_flow_summary",
        "finalize_device_auth_tail_summary",
    ];

    result
        .recent_events
        .as_ref()?
        .iter()
        .rev()
        .find_map(|event| {
            let record = event.as_object()?;
            let step_id = read_string_value(record, "step_id")
                .or_else(|| read_string_value(record, "stepId"))?;
            if !FINAL_SUMMARY_STEP_IDS.contains(&step_id.as_str()) {
                return None;
            }
            let phase = read_string_value(record, "phase");
            if phase.as_deref() != Some("action") {
                return None;
            }
            let status = read_string_value(record, "status");
            if status.as_deref() != Some("ok") {
                return None;
            }

            record
                .get("details")
                .and_then(Value::as_object)
                .and_then(|details| {
                    details
                        .get("result")
                        .and_then(Value::as_object)
                        .and_then(|result| result.get("value"))
                        .and_then(Value::as_object)
                        .filter(|value| looks_like_codex_rotate_auth_flow_summary_record(value))
                        .or_else(|| {
                            details
                                .get("value")
                                .and_then(Value::as_object)
                                .filter(|value| {
                                    looks_like_codex_rotate_auth_flow_summary_record(value)
                                })
                        })
                        .or_else(|| {
                            details
                                .get("result")
                                .and_then(Value::as_object)
                                .filter(|value| {
                                    looks_like_codex_rotate_auth_flow_summary_record(value)
                                })
                        })
                })
        })
}

pub(super) fn read_codex_rotate_auth_flow_session(
    result: &FastBrowserRunResult,
) -> Option<CodexRotateAuthFlowSession> {
    let summary = read_codex_rotate_auth_flow_summary(result);
    let action = result
        .state
        .as_ref()
        .and_then(|state| state.steps.get("start_codex_login_session"))
        .and_then(|step| step.action.as_ref())
        .and_then(Value::as_object);
    let action_session = action.and_then(|action| {
        action
            .get("value")
            .and_then(normalize_codex_rotate_auth_flow_session)
            .or_else(|| normalize_codex_rotate_auth_flow_session(&Value::Object(action.clone())))
    });

    match (summary.codex_session, action_session) {
        (Some(primary), Some(fallback)) => {
            Some(merge_codex_rotate_auth_flow_session(primary, fallback))
        }
        (Some(session), None) | (None, Some(session)) => Some(session),
        (None, None) => None,
    }
}

fn login_error_message(error_message: Option<&str>, fallback: String) -> String {
    error_message.map(str::to_string).unwrap_or(fallback)
}

fn maybe_debug_codex_auth_flow_result(
    workflow_ref: &str,
    email: &str,
    attempt_result: &BridgeLoginAttemptResult,
    flow: &CodexRotateAuthFlowSummary,
) {
    if std::env::var("CODEX_ROTATE_DEBUG_AUTH_FLOW_RESULT").as_deref() != Ok("1") {
        return;
    }

    let result = attempt_result.result.as_ref();
    let final_url = result
        .and_then(|value| value.final_url.as_deref())
        .unwrap_or("");
    let page_url = result
        .and_then(|value| value.page.as_ref())
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "url"))
        .unwrap_or_default();
    let page_title = result
        .and_then(|value| value.page.as_ref())
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "title"))
        .unwrap_or_default();
    let current_url = result
        .and_then(|value| value.current.as_ref())
        .and_then(Value::as_object)
        .and_then(|record| record.get("details"))
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "current_url"))
        .unwrap_or_default();
    let run_path = result
        .and_then(|value| value.observability.as_ref())
        .and_then(|value| value.run_path.as_deref())
        .unwrap_or("");
    let status_path = result
        .and_then(|value| value.observability.as_ref())
        .and_then(|value| value.status_path.as_deref())
        .unwrap_or("");
    eprintln!(
        "[codex-rotate-rust] auth-flow debug workflow={workflow_ref} email={email} final_url={final_url:?} page_url={page_url:?} page_title={page_title:?} current_url={current_url:?} run_path={run_path:?} status_path={status_path:?} callback_complete={:?} success={:?} next_action={:?} error_message={:?} has_output={} has_state={}",
        flow.callback_complete,
        flow.success,
        flow.next_action,
        flow.error_message,
        result.and_then(|value| value.output.as_ref()).is_some(),
        result.and_then(|value| value.state.as_ref()).is_some(),
    );
}

fn maybe_debug_codex_auth_flow_raw(workflow_ref: &str, email: &str, raw: &Value) {
    if std::env::var("CODEX_ROTATE_DEBUG_AUTH_FLOW_RESULT").as_deref() != Ok("1") {
        return;
    }

    let raw_json = serde_json::to_string(raw).unwrap_or_else(|_| "<serialize-failed>".to_string());
    let preview = if raw_json.len() > 4000 {
        format!("{}...", &raw_json[..4000])
    } else {
        raw_json
    };
    eprintln!(
        "[codex-rotate-rust] auth-flow raw workflow={workflow_ref} email={email} payload={preview}"
    );
}

pub(super) fn workflow_verified_expected_email(
    verified_account_email: Option<&str>,
    expected_email: &str,
) -> bool {
    verified_account_email
        .map(normalize_email_key)
        .is_some_and(|value| value == normalize_email_key(expected_email))
}

pub(super) fn is_workflow_skip_account_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<WorkflowSkipAccountError>().is_some()
}

pub(super) fn is_missing_account_login_ref_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains("Workflow input 'account_login_ref' must be a secret ref")
}

pub(super) fn is_optional_account_secret_prepare_error(error: &anyhow::Error) -> bool {
    let normalized = error.to_string().trim().to_lowercase();
    !normalized.is_empty()
        && (normalized.contains("bitwarden cli is locked")
            || normalized.contains("bitwarden cli is not logged in")
            || normalized.contains("bitwarden cli is not ready")
            || normalized.contains("timed out while trying to read bitwarden cli status")
            || normalized.contains("failed to read secret-store status"))
}

pub(super) fn is_final_add_phone_environment_blocker_error(error: &anyhow::Error) -> bool {
    let normalized = error.to_string().trim().to_lowercase();
    !normalized.is_empty()
        && (normalized.contains("openai final_add_phone blocked")
            || normalized.contains(
                "openai still requires phone setup before the codex callback can complete",
            )
            || normalized.contains("after exhausting final add-phone retries")
            || normalized.contains("after exhausting final add phone retries"))
}

pub(super) fn is_retryable_codex_login_workflow_error_message(message: &str) -> bool {
    let normalized = message.trim().to_lowercase();
    !normalized.is_empty()
        && (normalized.contains("signup-verification-code-missing")
            || normalized.contains("login-verification-code-missing")
            || normalized.contains("signup-verification-submit-stuck:email_verification")
            || normalized.contains("login-verification-submit-stuck:email_verification"))
}

fn codex_login_retry_delays_ms(reason: Option<&str>) -> &'static [u64] {
    match reason {
        Some("verification_artifact_pending") => DEFAULT_CODEX_LOGIN_VERIFICATION_RETRY_DELAYS_MS,
        Some("retryable_timeout") => DEFAULT_CODEX_LOGIN_RETRYABLE_TIMEOUT_DELAYS_MS,
        Some("device_auth_rate_limit") | Some("rate_limit") => {
            DEFAULT_CODEX_LOGIN_RATE_LIMIT_RETRY_DELAYS_MS
        }
        _ => DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS,
    }
}

pub(super) fn codex_login_retry_delay_ms(reason: Option<&str>, attempt: usize) -> u64 {
    let delays = codex_login_retry_delays_ms(reason);
    let index = attempt
        .saturating_sub(1)
        .min(delays.len().saturating_sub(1));
    delays
        .get(index)
        .copied()
        .unwrap_or(DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS[0])
}

pub(super) fn should_reset_codex_login_session_for_retry(
    retry_reason: Option<&str>,
    attempt: usize,
) -> bool {
    retry_reason == Some("state_mismatch")
        || retry_reason == Some("username_not_found")
        || retry_reason == Some("final_add_phone")
        || (retry_reason == Some("retryable_timeout") && attempt >= 2)
}

pub(super) fn codex_login_max_attempts(retry_reason: Option<&str>) -> usize {
    if retry_reason == Some("final_add_phone") {
        FINAL_ADD_PHONE_CODEX_LOGIN_MAX_ATTEMPTS
    } else {
        DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS
    }
}

pub(super) fn should_skip_account_after_retry_exhaustion(retry_reason: Option<&str>) -> bool {
    retry_reason == Some("final_add_phone")
}

pub(super) fn stop_on_final_add_phone_retry_exhaustion() -> bool {
    matches!(
        std::env::var(CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE_ENV).as_deref(),
        Ok("1")
    )
}

pub(super) fn final_add_phone_short_circuit_error(
    email: &str,
    current_url: Option<&str>,
    error_message: Option<&str>,
) -> anyhow::Error {
    anyhow::Error::new(WorkflowSkipAccountError::new(login_error_message(
        error_message,
        format!(
            "The workflow requested skipping {email} after final add-phone short-circuit{}.",
            current_url
                .map(|value| format!(" ({value})"))
                .unwrap_or_default()
        ),
    )))
}

pub(super) fn should_reset_device_auth_session_for_rate_limit(
    message: &str,
    session: Option<&CodexRotateAuthFlowSession>,
) -> bool {
    let normalized = message.trim().to_lowercase();
    if normalized.is_empty() {
        return true;
    }
    let has_reusable_device_challenge = session
        .and_then(|value| value.auth_url.as_deref())
        .is_some_and(|value| !value.trim().is_empty())
        && session
            .and_then(|value| value.device_code.as_deref())
            .is_some_and(|value| !value.trim().is_empty());
    if (normalized.contains("device auth failed with status 429")
        || normalized.contains("device auth failed:")
            && normalized.contains("429 too many requests"))
        && has_reusable_device_challenge
    {
        return false;
    }
    true
}

fn is_device_auth_rate_limited(message: &str) -> bool {
    let normalized = message.to_lowercase();
    normalized.contains("device code request failed with status 429")
        || normalized.contains("device auth failed with status 429")
        || normalized.contains("codex-login-exited-before-auth-url:")
            && normalized.contains("429 too many requests")
        || normalized.contains("429 too many requests")
}

fn format_retry_reason_label(reason: Option<&str>, fallback: &str) -> String {
    reason
        .map(|value| value.replace('_', " "))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn state_mismatch_in_login_flow(
    flow: &CodexRotateAuthFlowSummary,
    error_message: Option<&str>,
) -> bool {
    if flow.consent_error.as_deref() == Some("state_mismatch") {
        return true;
    }
    if flow.callback_complete != Some(true) || flow.codex_login_exit_ok != Some(false) {
        return false;
    }
    let combined = [
        flow.headline.as_deref(),
        flow.codex_login_stderr_tail.as_deref(),
        flow.codex_login_stdout_tail.as_deref(),
        error_message,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n")
    .to_lowercase();
    combined.contains("state mismatch")
}

pub(super) fn login_cancelled_after_callback(flow: &CodexRotateAuthFlowSummary) -> bool {
    if flow.callback_complete != Some(true) || flow.codex_login_exit_ok != Some(false) {
        return false;
    }
    let callback_url = flow.current_url.as_deref().unwrap_or_default();
    let callback_surface = callback_url.starts_with("http://localhost:")
        || callback_url.contains("/auth/callback")
        || callback_url.contains("/success");
    if !callback_surface {
        return false;
    }
    let combined = [
        flow.headline.as_deref(),
        flow.codex_login_stderr_tail.as_deref(),
        flow.codex_login_stdout_tail.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n")
    .to_lowercase();
    combined.contains("login cancelled")
}

fn promote_codex_auth_from_session(session: Option<&CodexRotateAuthFlowSession>) -> Result<()> {
    let Some(auth_file_path) = session
        .and_then(|value| value.auth_file_path.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let auth_file_path = Path::new(auth_file_path);
    if !auth_file_path.exists() {
        return Err(anyhow!(
            "Codex device authorization completed without producing {}.",
            auth_file_path.display()
        ));
    }
    let paths = resolve_paths()?;
    if let Some(parent) = paths.codex_auth_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::copy(auth_file_path, &paths.codex_auth_file).with_context(|| {
        format!(
            "Failed to copy {} to {}.",
            auth_file_path.display(),
            paths.codex_auth_file.display()
        )
    })?;
    Ok(())
}

fn cancel_codex_browser_login_session(session: Option<&CodexRotateAuthFlowSession>) {
    let Some(pid) = session
        .and_then(|value| value.pid)
        .filter(|value| *value > 1)
    else {
        return;
    };
    #[cfg(unix)]
    {
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .ok();
    }
    #[cfg(windows)]
    {
        Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .ok();
    }
}

pub(super) fn build_openai_account_login_locator(email: &str) -> CodexRotateSecretLocator {
    CodexRotateSecretLocator::LoginLookup {
        store: "bitwarden-cli".to_string(),
        username: email.trim().to_lowercase(),
        uris: vec![
            "https://auth.openai.com".to_string(),
            "https://chatgpt.com".to_string(),
        ],
        field_path: "/password".to_string(),
    }
}
