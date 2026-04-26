use super::*;

pub fn export_thread_handoffs(port: u16, account_id: &str) -> Result<Vec<ThreadHandoff>> {
    export_thread_handoffs_with_identity(port, account_id, account_id)
}

pub(super) fn export_thread_handoffs_with_identity(
    port: u16,
    watch_account_id: &str,
    sync_identity: &str,
) -> Result<Vec<ThreadHandoff>> {
    export_thread_handoffs_with_identity_and_candidates(
        port,
        watch_account_id,
        sync_identity,
        vec![],
    )
}

pub(super) fn export_thread_handoffs_with_identity_and_candidates(
    port: u16,
    watch_account_id: &str,
    sync_identity: &str,
    initial_thread_ids: Vec<String>,
) -> Result<Vec<ThreadHandoff>> {
    let paths = crate::paths::resolve_paths()?;
    let active_thread_ids = read_active_thread_ids(Some(port))?;
    let mut thread_ids = initial_thread_ids.clone();
    thread_ids.extend(active_thread_ids.clone());
    let state_thread_ids =
        read_thread_handoff_candidate_ids_from_state_db(&paths.codex_state_db_file)?;
    thread_ids.extend(state_thread_ids);
    let mut pending_recovery_thread_ids = BTreeSet::new();
    if let Ok(watch_state) = read_watch_state() {
        if let Some(account_state) = watch_state.accounts.get(watch_account_id) {
            for event in &account_state.thread_recovery_pending_events {
                pending_recovery_thread_ids.insert(event.thread_id.clone());
                thread_ids.push(event.thread_id.clone());
            }
        }
    }
    let active_thread_ids = active_thread_ids.into_iter().collect::<BTreeSet<_>>();
    let transport = HostConversationTransport::new(port);
    export_thread_handoffs_from_candidates(
        &transport,
        sync_identity,
        thread_ids,
        &active_thread_ids,
        &pending_recovery_thread_ids,
    )
}

pub(super) fn export_thread_handoffs_from_candidates(
    transport: &dyn ConversationTransport,
    sync_identity: &str,
    thread_ids: Vec<String>,
    continue_thread_ids: &BTreeSet<String>,
    pending_recovery_thread_ids: &BTreeSet<String>,
) -> Result<Vec<ThreadHandoff>> {
    let mut unique = BTreeSet::new();
    let mut handoffs = Vec::new();
    for thread_id in thread_ids {
        if !unique.insert(thread_id.clone()) {
            continue;
        }
        match export_single_thread_handoff_with_identity(transport, &thread_id, sync_identity) {
            Ok(Some(handoff)) => {
                if handoff.metadata.archived == Some(true)
                    && !continue_thread_ids.contains(&thread_id)
                    && !pending_recovery_thread_ids.contains(&thread_id)
                {
                    continue;
                }
                handoffs.push(handoff);
            }
            Ok(None) => {}
            Err(error) if is_skippable_thread_handoff_read_error(&error) => {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(handoffs)
}

fn is_skippable_thread_handoff_read_error(error: &anyhow::Error) -> bool {
    if is_terminal_thread_read_error(error) {
        return true;
    }
    let message = format!("{error:#}");
    host_thread_not_ready_message(&message)
}

pub(super) fn is_terminal_thread_read_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_lowercase();
    message.contains("no rollout found for thread id")
        || message.contains("thread not found")
        || message.contains("no thread found")
        || message.contains("unknown thread")
        || message.contains("does not exist")
        || (message.contains("failed to load thread history")
            && message.contains("no such file or directory"))
}

pub(super) fn export_single_thread_handoff_with_identity(
    transport: &dyn ConversationTransport,
    thread_id: &str,
    sync_identity: &str,
) -> Result<Option<ThreadHandoff>> {
    let response = transport.read_thread(thread_id)?;
    let mut handoff =
        export_single_thread_handoff_from_response(response, thread_id, sync_identity)?;
    if let Some(handoff) = handoff.as_mut() {
        merge_thread_sidebar_metadata(
            &mut handoff.metadata,
            transport.read_thread_ui_metadata(thread_id)?,
        );
    }
    Ok(handoff)
}

pub(super) fn export_single_thread_handoff_from_response(
    response: Value,
    thread_id: &str,
    sync_identity: &str,
) -> Result<Option<ThreadHandoff>> {
    let Some(thread) = response.get("thread") else {
        return Ok(None);
    };
    let cwd = thread
        .get("cwd")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let items = mapped_response_items_from_thread(thread);
    let metadata = thread_handoff_metadata_from_thread(thread_id, thread, &items);

    let paths = crate::paths::resolve_paths()?;
    let mut store = ConversationSyncStore::new(&paths.conversation_sync_db_file)?;
    let lineage_id = match store.get_lineage_id(sync_identity, thread_id)? {
        Some(lineage_id) => lineage_id,
        None => {
            store.bind_local_thread_id(sync_identity, thread_id, thread_id)?;
            thread_id.to_string()
        }
    };
    let watermark = thread_handoff_content_watermark(&items);
    store.set_watermark(sync_identity, &lineage_id, watermark.as_deref())?;

    Ok(Some(ThreadHandoff {
        source_thread_id: thread_id.to_string(),
        lineage_id,
        watermark,
        cwd,
        items,
        metadata,
    }))
}

pub(super) fn thread_handoff_content_watermark(items: &[Value]) -> Option<String> {
    if items.is_empty() {
        return None;
    }
    let serialized = serde_json::to_vec(items).ok()?;
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in &serialized {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Some(format!("items-fnv1a64-{hash:016x}-{}", items.len()))
}

pub(super) fn mapped_response_items_from_thread(thread: &Value) -> Vec<Value> {
    thread
        .get("turns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.get("items")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(map_thread_item_to_response_item)
        .collect()
}

pub(super) fn mapped_response_items_from_thread_read_response(
    response: &Value,
) -> Option<Vec<Value>> {
    response
        .get("thread")
        .map(mapped_response_items_from_thread)
}

pub fn import_thread_handoffs(
    transport: &dyn ConversationTransport,
    target_account_id: &str,
    handoffs: &[ThreadHandoff],
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<ThreadHandoffImportOutcome> {
    let mut outcome = ThreadHandoffImportOutcome::default();
    let paths = crate::paths::resolve_paths()?;
    let mut store = ConversationSyncStore::new(&paths.conversation_sync_db_file)?;

    for handoff in handoffs {
        if let Some(progress) = progress {
            progress(format!(
                "Restoring transferred thread {} (lineage: {}).",
                handoff.source_thread_id, handoff.lineage_id
            ));
        }

        let current_watermark = store.get_watermark(target_account_id, &handoff.lineage_id)?;
        let current_content_is_synced = current_watermark.as_deref()
            == handoff.watermark.as_deref()
            && handoff.watermark.is_some();
        let mut created_thread_id = None;
        let mut lineage_claim_token = None;
        let existing_local_id = match store
            .claim_lineage_binding(target_account_id, &handoff.lineage_id)?
        {
            LineageBindingClaim::Existing(local_id) => {
                let should_reclaim = if local_id == handoff.source_thread_id {
                    true
                } else if current_content_is_synced {
                    false
                } else {
                    match transport.thread_exists(&local_id) {
                        Ok(exists) => !exists,
                        Err(error) => {
                            outcome.failures.push(ThreadHandoffImportFailure {
                                source_thread_id: handoff.source_thread_id.clone(),
                                created_thread_id: None,
                                stage: ThreadHandoffImportFailureStage::Start,
                                error: format!(
                                    "Failed to validate mapped local thread {local_id}: {error}"
                                ),
                            });
                            continue;
                        }
                    }
                };
                if should_reclaim {
                    match store.reclaim_lineage_binding(
                        target_account_id,
                        &handoff.lineage_id,
                        &local_id,
                    )? {
                        LineageBindingClaim::Claimed { claim_token } => {
                            lineage_claim_token = Some(claim_token);
                            None
                        }
                        LineageBindingClaim::Existing(repaired_local_id) => Some(repaired_local_id),
                        LineageBindingClaim::Busy => {
                            outcome.failures.push(ThreadHandoffImportFailure {
                                source_thread_id: handoff.source_thread_id.clone(),
                                created_thread_id: None,
                                stage: ThreadHandoffImportFailureStage::Start,
                                error: format!(
                                    "Lineage {} is already being synchronized for account {}; retry.",
                                    handoff.lineage_id, target_account_id
                                ),
                            });
                            continue;
                        }
                    }
                } else {
                    Some(local_id)
                }
            }
            LineageBindingClaim::Claimed { claim_token } => {
                lineage_claim_token = Some(claim_token);
                None
            }
            LineageBindingClaim::Busy => {
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id: None,
                    stage: ThreadHandoffImportFailureStage::Start,
                    error: format!(
                        "Lineage {} is already being synchronized for account {}; retry.",
                        handoff.lineage_id, target_account_id
                    ),
                });
                continue;
            }
        };

        let mut target_thread_id = match existing_local_id {
            Some(local_id) => {
                outcome.prevented_duplicates_count += 1;
                local_id
            }
            None => {
                // Binding absent: create exactly one new target-local thread.
                let new_thread_id = match transport.start_thread(handoff.cwd.as_deref()) {
                    Ok(id) => id,
                    Err(error) => {
                        if let Some(claim_token) = lineage_claim_token.as_deref() {
                            let _ = store.release_lineage_claim(
                                target_account_id,
                                &handoff.lineage_id,
                                claim_token,
                            );
                        }
                        outcome.failures.push(ThreadHandoffImportFailure {
                            source_thread_id: handoff.source_thread_id.clone(),
                            created_thread_id: None,
                            stage: ThreadHandoffImportFailureStage::Start,
                            error: error.to_string(),
                        });
                        continue;
                    }
                };

                if new_thread_id == handoff.source_thread_id {
                    if let Some(claim_token) = lineage_claim_token.as_deref() {
                        let _ = store.release_lineage_claim(
                            target_account_id,
                            &handoff.lineage_id,
                            claim_token,
                        );
                    }
                    outcome.failures.push(ThreadHandoffImportFailure {
                        source_thread_id: handoff.source_thread_id.clone(),
                        created_thread_id: None,
                        stage: ThreadHandoffImportFailureStage::Start,
                        error: "Codex thread/start unexpectedly reused the source thread ID."
                            .to_string(),
                    });
                    continue;
                }

                created_thread_id = Some(new_thread_id.clone());
                new_thread_id
            }
        };

        let needs_update = current_watermark.as_deref() != handoff.watermark.as_deref();
        let mut items_to_inject = if created_thread_id.is_some() || needs_update {
            handoff.items.clone()
        } else {
            Vec::new()
        };
        let mut replaced_thread_id = None::<String>;

        if created_thread_id.is_none() && needs_update && !handoff.items.is_empty() {
            match plan_existing_thread_materialization(transport, &target_thread_id, handoff) {
                Ok(ExistingThreadMaterializationPlan::AlreadyCurrent) => {
                    items_to_inject.clear();
                }
                Ok(ExistingThreadMaterializationPlan::AppendSuffix(suffix)) => {
                    items_to_inject = suffix;
                }
                Ok(ExistingThreadMaterializationPlan::Recreate) => {
                    match store.reclaim_lineage_binding(
                        target_account_id,
                        &handoff.lineage_id,
                        &target_thread_id,
                    )? {
                        LineageBindingClaim::Claimed { claim_token } => {
                            lineage_claim_token = Some(claim_token);
                            let old_thread_id = target_thread_id.clone();
                            let new_thread_id = match transport.start_thread(handoff.cwd.as_deref())
                            {
                                Ok(id) => id,
                                Err(error) => {
                                    if let Some(claim_token) = lineage_claim_token.as_deref() {
                                        let _ = store.release_lineage_claim(
                                            target_account_id,
                                            &handoff.lineage_id,
                                            claim_token,
                                        );
                                    }
                                    outcome.failures.push(ThreadHandoffImportFailure {
                                        source_thread_id: handoff.source_thread_id.clone(),
                                        created_thread_id: None,
                                        stage: ThreadHandoffImportFailureStage::Start,
                                        error: error.to_string(),
                                    });
                                    continue;
                                }
                            };
                            if new_thread_id == handoff.source_thread_id {
                                if let Some(claim_token) = lineage_claim_token.as_deref() {
                                    let _ = store.release_lineage_claim(
                                        target_account_id,
                                        &handoff.lineage_id,
                                        claim_token,
                                    );
                                }
                                outcome.failures.push(ThreadHandoffImportFailure {
                                    source_thread_id: handoff.source_thread_id.clone(),
                                    created_thread_id: None,
                                    stage: ThreadHandoffImportFailureStage::Start,
                                    error:
                                        "Codex thread/start unexpectedly reused the source thread ID."
                                            .to_string(),
                                });
                                continue;
                            }
                            target_thread_id = new_thread_id.clone();
                            created_thread_id = Some(new_thread_id);
                            replaced_thread_id = Some(old_thread_id);
                            items_to_inject = handoff.items.clone();
                        }
                        LineageBindingClaim::Existing(repaired_local_id) => {
                            outcome.failures.push(ThreadHandoffImportFailure {
                                source_thread_id: handoff.source_thread_id.clone(),
                                created_thread_id: None,
                                stage: ThreadHandoffImportFailureStage::Start,
                                error: format!(
                                    "Mapped local thread {} needed replacement, but lineage was concurrently rebound to {}.",
                                    target_thread_id, repaired_local_id
                                ),
                            });
                            continue;
                        }
                        LineageBindingClaim::Busy => {
                            outcome.failures.push(ThreadHandoffImportFailure {
                                source_thread_id: handoff.source_thread_id.clone(),
                                created_thread_id: None,
                                stage: ThreadHandoffImportFailureStage::Start,
                                error: format!(
                                    "Lineage {} is already being synchronized for account {}; retry.",
                                    handoff.lineage_id, target_account_id
                                ),
                            });
                            continue;
                        }
                    }
                }
                Err(error) => {
                    outcome.failures.push(ThreadHandoffImportFailure {
                        source_thread_id: handoff.source_thread_id.clone(),
                        created_thread_id: None,
                        stage: ThreadHandoffImportFailureStage::Start,
                        error: error.to_string(),
                    });
                    continue;
                }
            }
        }

        let should_materialize = created_thread_id.is_some() || needs_update;

        if should_materialize && !items_to_inject.is_empty() {
            if let Err(error) = transport.inject_items(&target_thread_id, items_to_inject.clone()) {
                let error_message = format!("{:#}", error);
                if created_thread_id.is_none() && thread_not_found_message(&error_message) {
                    match store.reclaim_lineage_binding(
                        target_account_id,
                        &handoff.lineage_id,
                        &target_thread_id,
                    )? {
                        LineageBindingClaim::Claimed { claim_token } => {
                            lineage_claim_token = Some(claim_token);
                            let old_thread_id = target_thread_id.clone();
                            let new_thread_id = match transport.start_thread(handoff.cwd.as_deref())
                            {
                                Ok(id) => id,
                                Err(start_error) => {
                                    if let Some(claim_token) = lineage_claim_token.as_deref() {
                                        let _ = store.release_lineage_claim(
                                            target_account_id,
                                            &handoff.lineage_id,
                                            claim_token,
                                        );
                                    }
                                    outcome.failures.push(ThreadHandoffImportFailure {
                                        source_thread_id: handoff.source_thread_id.clone(),
                                        created_thread_id: None,
                                        stage: ThreadHandoffImportFailureStage::Start,
                                        error: start_error.to_string(),
                                    });
                                    continue;
                                }
                            };
                            if new_thread_id == handoff.source_thread_id {
                                if let Some(claim_token) = lineage_claim_token.as_deref() {
                                    let _ = store.release_lineage_claim(
                                        target_account_id,
                                        &handoff.lineage_id,
                                        claim_token,
                                    );
                                }
                                outcome.failures.push(ThreadHandoffImportFailure {
                                        source_thread_id: handoff.source_thread_id.clone(),
                                        created_thread_id: None,
                                        stage: ThreadHandoffImportFailureStage::Start,
                                        error: "Codex thread/start unexpectedly reused the source thread ID."
                                            .to_string(),
                                    });
                                continue;
                            }
                            target_thread_id = new_thread_id.clone();
                            created_thread_id = Some(new_thread_id);
                            replaced_thread_id = Some(old_thread_id);
                            items_to_inject = handoff.items.clone();
                            if let Err(retry_error) =
                                transport.inject_items(&target_thread_id, items_to_inject.clone())
                            {
                                let _ = store.bind_local_thread_id(
                                    target_account_id,
                                    &handoff.lineage_id,
                                    &target_thread_id,
                                );
                                outcome.failures.push(ThreadHandoffImportFailure {
                                    source_thread_id: handoff.source_thread_id.clone(),
                                    created_thread_id: created_thread_id.clone(),
                                    stage: ThreadHandoffImportFailureStage::InjectItems,
                                    error: retry_error.to_string(),
                                });
                                continue;
                            }
                        }
                        LineageBindingClaim::Existing(repaired_local_id) => {
                            outcome.failures.push(ThreadHandoffImportFailure {
                                    source_thread_id: handoff.source_thread_id.clone(),
                                    created_thread_id: None,
                                    stage: ThreadHandoffImportFailureStage::InjectItems,
                                    error: format!(
                                        "Mapped local thread {} disappeared, but lineage was concurrently rebound to {}.",
                                        target_thread_id, repaired_local_id
                                    ),
                                });
                            continue;
                        }
                        LineageBindingClaim::Busy => {
                            outcome.failures.push(ThreadHandoffImportFailure {
                                    source_thread_id: handoff.source_thread_id.clone(),
                                    created_thread_id: None,
                                    stage: ThreadHandoffImportFailureStage::InjectItems,
                                    error: format!(
                                        "Lineage {} is already being synchronized for account {}; retry.",
                                        handoff.lineage_id, target_account_id
                                    ),
                                });
                            continue;
                        }
                    }
                } else {
                    if created_thread_id.is_some() {
                        let _ = store.bind_local_thread_id(
                            target_account_id,
                            &handoff.lineage_id,
                            &target_thread_id,
                        );
                    }
                    outcome.failures.push(ThreadHandoffImportFailure {
                        source_thread_id: handoff.source_thread_id.clone(),
                        created_thread_id: created_thread_id.clone(),
                        stage: ThreadHandoffImportFailureStage::InjectItems,
                        error: error.to_string(),
                    });
                    continue;
                }
            }
            wait_for_imported_thread_persistence();
            if let Err(error) =
                transport.ensure_thread_user_message_event(&target_thread_id, handoff)
            {
                if created_thread_id.is_some() || lineage_claim_token.is_some() {
                    let _ = store.bind_local_thread_id(
                        target_account_id,
                        &handoff.lineage_id,
                        &target_thread_id,
                    );
                }
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id: created_thread_id.clone(),
                    stage: ThreadHandoffImportFailureStage::Metadata,
                    error: error.to_string(),
                });
                continue;
            }
        }

        if let Err(error) = transport.publish_thread_handoff_metadata(&target_thread_id, handoff) {
            if created_thread_id.is_some() || lineage_claim_token.is_some() {
                let _ = store.bind_and_update_watermark(
                    target_account_id,
                    &handoff.lineage_id,
                    &target_thread_id,
                    handoff.watermark.as_deref(),
                );
            }
            outcome.failures.push(ThreadHandoffImportFailure {
                source_thread_id: handoff.source_thread_id.clone(),
                created_thread_id: created_thread_id.clone(),
                stage: ThreadHandoffImportFailureStage::Metadata,
                error: error.to_string(),
            });
            continue;
        }

        // Update binding and watermark transactionally after materialization.
        let persist_result = if let Some(claim_token) = lineage_claim_token.as_deref() {
            store.finalize_lineage_claim(
                target_account_id,
                &handoff.lineage_id,
                claim_token,
                &target_thread_id,
                handoff.watermark.as_deref(),
            )
        } else if needs_update {
            store.bind_and_update_watermark(
                target_account_id,
                &handoff.lineage_id,
                &target_thread_id,
                handoff.watermark.as_deref(),
            )
        } else {
            Ok(())
        };
        if let Err(error) = persist_result {
            outcome.failures.push(ThreadHandoffImportFailure {
                source_thread_id: handoff.source_thread_id.clone(),
                created_thread_id,
                stage: ThreadHandoffImportFailureStage::Persist,
                error: format!("Failed to update binding and watermark: {}", error),
            });
            continue;
        }

        if let Some(stale_thread_id) = replaced_thread_id.as_deref() {
            if let Err(error) =
                transport.cleanup_replaced_thread(&target_thread_id, stale_thread_id)
            {
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id,
                    stage: ThreadHandoffImportFailureStage::Persist,
                    error: format!(
                        "Failed to remove replaced local thread {stale_thread_id}: {error}"
                    ),
                });
                continue;
            }
        }

        outcome
            .completed_source_thread_ids
            .push(handoff.source_thread_id.clone());
    }
    Ok(outcome)
}

pub(super) enum ExistingThreadMaterializationPlan {
    AlreadyCurrent,
    AppendSuffix(Vec<Value>),
    Recreate,
}

pub(super) fn plan_existing_thread_materialization(
    transport: &dyn ConversationTransport,
    target_thread_id: &str,
    handoff: &ThreadHandoff,
) -> Result<ExistingThreadMaterializationPlan> {
    let response = match transport.read_thread(target_thread_id) {
        Ok(response) => response,
        Err(error) => {
            let message = format!("{error:#}");
            if thread_not_found_message(&message) {
                return Ok(ExistingThreadMaterializationPlan::Recreate);
            }
            return Err(error);
        }
    };
    let Some(existing_items) = mapped_response_items_from_thread_read_response(&response) else {
        return Ok(ExistingThreadMaterializationPlan::Recreate);
    };
    if existing_items == handoff.items {
        return Ok(ExistingThreadMaterializationPlan::AlreadyCurrent);
    }
    if response_items_are_prefix(&existing_items, &handoff.items) {
        return Ok(ExistingThreadMaterializationPlan::AppendSuffix(
            handoff.items[existing_items.len()..].to_vec(),
        ));
    }
    Ok(ExistingThreadMaterializationPlan::Recreate)
}

pub(super) fn response_items_are_prefix(prefix: &[Value], items: &[Value]) -> bool {
    prefix.len() <= items.len() && prefix.iter().zip(items).all(|(left, right)| left == right)
}

#[cfg(not(test))]
pub(super) fn wait_for_imported_thread_persistence() {
    std::thread::sleep(Duration::from_secs(2));
}

#[cfg(test)]
pub(super) fn wait_for_imported_thread_persistence() {}

pub(super) fn thread_handoff_metadata_from_thread(
    thread_id: &str,
    thread: &Value,
    mapped_items: &[Value],
) -> ThreadHandoffMetadata {
    let mut metadata = read_thread_sidebar_metadata_from_active_home(thread_id)
        .unwrap_or_else(|_| ThreadHandoffMetadata::default());
    fill_if_missing(
        &mut metadata.title,
        first_non_empty_thread_string(thread, &["title", "thread_name", "name"]),
    );
    fill_if_missing(
        &mut metadata.first_user_message,
        first_non_empty_thread_string(thread, &["first_user_message", "firstUserMessage"])
            .or_else(|| first_user_message_from_thread_turns(thread))
            .or_else(|| first_user_message_from_response_items(mapped_items)),
    );
    fill_if_missing_i64(
        &mut metadata.updated_at,
        first_thread_i64(thread, &["updated_at", "updatedAt"]),
    );
    fill_if_missing_i64(
        &mut metadata.updated_at_ms,
        first_thread_i64(thread, &["updated_at_ms", "updatedAtMs"]),
    );
    if let Some(cwd) = first_non_empty_thread_string(thread, &["cwd"]) {
        metadata.cwd = Some(cwd);
    }
    metadata
}

pub(super) fn first_non_empty_thread_string(thread: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        thread
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

pub(super) fn first_thread_i64(thread: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        thread.get(*key).and_then(|value| {
            value.as_i64().or_else(|| {
                value
                    .as_str()
                    .and_then(|text| text.trim().parse::<i64>().ok())
            })
        })
    })
}

pub(super) fn fill_if_missing(target: &mut Option<String>, candidate: Option<String>) {
    if target.as_deref().unwrap_or_default().trim().is_empty() {
        *target = candidate.filter(|value| !value.trim().is_empty());
    }
}

pub(super) fn fill_if_missing_i64(target: &mut Option<i64>, candidate: Option<i64>) {
    if target.is_none() {
        *target = candidate;
    }
}

pub(super) fn fill_if_missing_bool(target: &mut Option<bool>, candidate: Option<bool>) {
    if target.is_none() {
        *target = candidate;
    }
}

pub(super) fn first_user_message_from_response_items(items: &[Value]) -> Option<String> {
    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("message")
            || item.get("role").and_then(Value::as_str) != Some("user")
        {
            continue;
        }
        let text = item
            .get("content")
            .and_then(Value::as_array)
            .map(|content| {
                content
                    .iter()
                    .filter_map(|entry| {
                        let content_type = entry.get("type").and_then(Value::as_str)?;
                        if content_type == "input_text" || content_type == "text" {
                            entry.get("text").and_then(Value::as_str)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        if !text.trim().is_empty() && !is_handoff_context_user_text(&text) {
            return Some(truncate_handoff_text(&text));
        }
    }
    None
}

pub(super) fn first_user_message_from_thread_turns(thread: &Value) -> Option<String> {
    for item in thread
        .get("turns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.get("items")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
    {
        if let Some(text) = user_text_from_thread_item(item) {
            if is_handoff_context_user_text(&text) {
                continue;
            }
            return Some(truncate_handoff_text(&text));
        }
    }
    None
}

pub(super) fn user_text_from_thread_item(item: &Value) -> Option<String> {
    match item.get("type").and_then(Value::as_str)? {
        "message" if item.get("role").and_then(Value::as_str) == Some("user") => item
            .get("content")
            .and_then(Value::as_array)
            .map(|content| text_from_content_array(content))
            .filter(|text| !text.trim().is_empty()),
        "userMessage" | "user_message" => item
            .get("content")
            .and_then(Value::as_array)
            .map(|content| text_from_content_array(content))
            .or_else(|| {
                item.get("message")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                item.get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .filter(|text| !text.trim().is_empty()),
        _ => None,
    }
}

pub(super) fn text_from_content_array(content: &[Value]) -> String {
    content
        .iter()
        .filter_map(|entry| {
            let content_type = entry.get("type").and_then(Value::as_str)?;
            if matches!(content_type, "input_text" | "text") {
                entry.get("text").and_then(Value::as_str)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn is_handoff_context_user_text(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<environment_context>")
        || trimmed
            .starts_with("Continue this transferred conversation from its latest unfinished state")
}

pub(super) fn read_thread_sidebar_metadata_from_active_home(
    thread_id: &str,
) -> Result<ThreadHandoffMetadata> {
    let paths = crate::paths::resolve_paths()?;
    read_thread_sidebar_metadata(&paths.codex_home, thread_id)
}

pub(super) fn read_thread_sidebar_metadata(
    codex_home: &Path,
    thread_id: &str,
) -> Result<ThreadHandoffMetadata> {
    let mut metadata = ThreadHandoffMetadata::default();
    if let Some(state_db_path) = resolve_state_db_file_in_codex_home(codex_home) {
        if state_db_path.exists() {
            merge_thread_sidebar_metadata(
                &mut metadata,
                read_thread_sidebar_metadata_from_state_db(&state_db_path, thread_id)?,
            );
        }
    }
    merge_thread_sidebar_metadata(
        &mut metadata,
        read_thread_sidebar_metadata_from_session_index(codex_home, thread_id)?,
    );
    Ok(metadata)
}

pub(super) fn merge_thread_sidebar_metadata(
    target: &mut ThreadHandoffMetadata,
    source: ThreadHandoffMetadata,
) {
    fill_if_missing(&mut target.title, source.title);
    fill_if_missing(&mut target.first_user_message, source.first_user_message);
    fill_if_missing_i64(&mut target.updated_at, source.updated_at);
    fill_if_missing_i64(&mut target.updated_at_ms, source.updated_at_ms);
    fill_if_missing(
        &mut target.session_index_updated_at,
        source.session_index_updated_at,
    );
    fill_if_missing(&mut target.source, source.source);
    fill_if_missing(&mut target.model_provider, source.model_provider);
    fill_if_missing(&mut target.cwd, source.cwd);
    fill_if_missing(&mut target.sandbox_policy, source.sandbox_policy);
    fill_if_missing(&mut target.approval_mode, source.approval_mode);
    fill_if_missing_bool(&mut target.projectless, source.projectless);
    fill_if_missing(&mut target.workspace_root_hint, source.workspace_root_hint);
    fill_if_missing_bool(&mut target.archived, source.archived);
}

pub(super) fn read_thread_sidebar_metadata_from_state_db(
    state_db_path: &Path,
    thread_id: &str,
) -> Result<ThreadHandoffMetadata> {
    let connection = rusqlite::Connection::open(state_db_path)
        .with_context(|| format!("Failed to open {}.", state_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(ThreadHandoffMetadata::default());
    }
    let columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !columns.iter().any(|column| column == "id") {
        return Ok(ThreadHandoffMetadata::default());
    }
    let mut metadata = ThreadHandoffMetadata::default();
    if columns.iter().any(|column| column == "title") {
        metadata.title = query_thread_optional_string(&connection, "title", thread_id)?
            .filter(|value| !value.trim().is_empty())
            .filter(|value| !is_handoff_context_user_text(value));
    }
    if columns.iter().any(|column| column == "first_user_message") {
        metadata.first_user_message =
            query_thread_optional_string(&connection, "first_user_message", thread_id)?
                .filter(|value| !value.trim().is_empty())
                .filter(|value| !is_handoff_context_user_text(value));
    }
    if columns.iter().any(|column| column == "updated_at") {
        metadata.updated_at = query_thread_optional_i64(&connection, "updated_at", thread_id)?;
    }
    if columns.iter().any(|column| column == "updated_at_ms") {
        metadata.updated_at_ms =
            query_thread_optional_i64(&connection, "updated_at_ms", thread_id)?;
    }
    if columns.iter().any(|column| column == "source") {
        metadata.source = query_thread_optional_string(&connection, "source", thread_id)?
            .filter(|value| !value.trim().is_empty());
    }
    if columns.iter().any(|column| column == "model_provider") {
        metadata.model_provider =
            query_thread_optional_string(&connection, "model_provider", thread_id)?
                .filter(|value| !value.trim().is_empty());
    }
    if columns.iter().any(|column| column == "cwd") {
        metadata.cwd = query_thread_optional_string(&connection, "cwd", thread_id)?
            .filter(|value| !value.trim().is_empty());
    }
    if columns.iter().any(|column| column == "sandbox_policy") {
        metadata.sandbox_policy =
            query_thread_optional_string(&connection, "sandbox_policy", thread_id)?
                .filter(|value| !value.trim().is_empty());
    }
    if columns.iter().any(|column| column == "approval_mode") {
        metadata.approval_mode =
            query_thread_optional_string(&connection, "approval_mode", thread_id)?
                .filter(|value| !value.trim().is_empty());
    }
    if columns.iter().any(|column| column == "archived") {
        metadata.archived =
            query_thread_optional_i64(&connection, "archived", thread_id)?.map(|value| value != 0);
    }
    Ok(metadata)
}

pub(super) fn query_thread_optional_string(
    connection: &rusqlite::Connection,
    column: &str,
    thread_id: &str,
) -> Result<Option<String>> {
    let sql = format!(
        "select {} from threads where id = ?1",
        quote_sql_identifier(column)
    );
    connection
        .query_row(&sql, [thread_id], |row| row.get::<_, Option<String>>(0))
        .optional()
        .map(|value| value.flatten())
        .with_context(|| format!("Failed to query thread {thread_id} metadata column {column}."))
}

pub(super) fn query_thread_optional_i64(
    connection: &rusqlite::Connection,
    column: &str,
    thread_id: &str,
) -> Result<Option<i64>> {
    let sql = format!(
        "select {} from threads where id = ?1",
        quote_sql_identifier(column)
    );
    connection
        .query_row(&sql, [thread_id], |row| row.get::<_, Option<i64>>(0))
        .optional()
        .map(|value| value.flatten())
        .with_context(|| format!("Failed to query thread {thread_id} metadata column {column}."))
}

pub(super) fn read_thread_sidebar_metadata_from_session_index(
    codex_home: &Path,
    thread_id: &str,
) -> Result<ThreadHandoffMetadata> {
    let path = codex_home.join("session_index.jsonl");
    let Ok(contents) = fs::read_to_string(&path) else {
        return Ok(ThreadHandoffMetadata::default());
    };
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("id").and_then(Value::as_str) != Some(thread_id) {
            continue;
        }
        return Ok(ThreadHandoffMetadata {
            title: value
                .get("thread_name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .filter(|value| !is_handoff_context_user_text(value))
                .map(ToOwned::to_owned),
            first_user_message: None,
            updated_at: None,
            updated_at_ms: None,
            session_index_updated_at: value
                .get("updated_at")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            ..ThreadHandoffMetadata::default()
        });
    }
    Ok(ThreadHandoffMetadata::default())
}

#[cfg(test)]
pub(super) fn publish_thread_sidebar_metadata(
    codex_home: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
) -> Result<()> {
    let Some(metadata) = prepared_thread_sidebar_metadata(codex_home, thread_id, metadata)? else {
        return Ok(());
    };
    publish_prepared_thread_sidebar_metadata(codex_home, thread_id, &metadata)
}

pub(super) fn publish_thread_sidebar_metadata_until_visible(
    codex_home: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
    timeout: Duration,
) -> Result<()> {
    let Some(metadata) = prepared_thread_sidebar_metadata(codex_home, thread_id, metadata)? else {
        return Ok(());
    };
    let deadline = Instant::now() + timeout;
    let mut last_error = None::<anyhow::Error>;
    loop {
        match publish_prepared_thread_sidebar_metadata(codex_home, thread_id, &metadata)
            .and_then(|_| thread_sidebar_metadata_visible(codex_home, thread_id, &metadata))
        {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => last_error = Some(error),
        }
        if Instant::now() >= deadline {
            if let Some(error) = last_error {
                return Err(error);
            }
            return Err(anyhow!(
                "Timed out waiting for thread {thread_id} sidebar metadata to become visible."
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

pub(super) fn prepared_thread_sidebar_metadata(
    codex_home: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
) -> Result<Option<ThreadHandoffMetadata>> {
    let mut metadata = metadata.clone();
    if metadata
        .title
        .as_deref()
        .map(is_handoff_context_user_text)
        .unwrap_or(false)
    {
        metadata.title = None;
    }
    if metadata
        .first_user_message
        .as_deref()
        .map(is_handoff_context_user_text)
        .unwrap_or(false)
    {
        metadata.first_user_message = None;
    }
    if metadata
        .title
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        metadata.title = metadata
            .first_user_message
            .as_deref()
            .map(truncate_handoff_text)
            .filter(|value| !value.trim().is_empty());
    }
    if metadata
        .first_user_message
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        metadata.first_user_message =
            first_user_message_from_thread_rollout(codex_home, thread_id)?;
    }
    if metadata
        .title
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        metadata.title = metadata
            .first_user_message
            .as_deref()
            .map(truncate_handoff_text)
            .filter(|value| !value.trim().is_empty());
    }
    if metadata
        .title
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
        && metadata
            .first_user_message
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
        && metadata.archived.is_none()
    {
        return Ok(None);
    }

    Ok(Some(metadata))
}

pub(super) fn publish_prepared_thread_sidebar_metadata(
    codex_home: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
) -> Result<()> {
    if let Some(state_db_path) = resolve_state_db_file_in_codex_home(codex_home) {
        if state_db_path.exists() {
            publish_thread_sidebar_metadata_to_state_db(
                codex_home,
                &state_db_path,
                thread_id,
                metadata,
            )?;
        }
    }
    publish_thread_sidebar_metadata_to_session_index(codex_home, thread_id, metadata)?;
    Ok(())
}

pub(super) fn thread_sidebar_metadata_visible(
    codex_home: &Path,
    thread_id: &str,
    expected: &ThreadHandoffMetadata,
) -> Result<bool> {
    let Some(state_db_path) = resolve_state_db_file_in_codex_home(codex_home) else {
        return Ok(true);
    };
    if !state_db_path.exists() {
        return Ok(true);
    }
    let actual = read_thread_sidebar_metadata_from_state_db(&state_db_path, thread_id)?;
    let title_visible = match expected
        .title
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Some(_) => actual
            .title
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false),
        None => true,
    };
    let first_user_visible = match expected
        .first_user_message
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Some(expected) => actual
            .first_user_message
            .as_deref()
            .map(|value| value.contains(expected) || !value.trim().is_empty())
            .unwrap_or(false),
        None => true,
    };
    let archived_visible = match expected.archived {
        Some(expected_archived) => actual.archived == Some(expected_archived),
        None => true,
    };
    Ok(title_visible && first_user_visible && archived_visible)
}

pub(super) fn publish_thread_sidebar_metadata_to_state_db(
    codex_home: &Path,
    state_db_path: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
) -> Result<()> {
    let connection = rusqlite::Connection::open(state_db_path)
        .with_context(|| format!("Failed to open {}.", state_db_path.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .with_context(|| format!("Failed to configure {}.", state_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(());
    }
    let columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !columns.iter().any(|column| column == "id") {
        return Ok(());
    }
    let has_row: Option<i64> = connection
        .query_row(
            "select 1 from threads where id = ?1 limit 1",
            [thread_id],
            |row| row.get(0),
        )
        .optional()
        .with_context(|| format!("Failed to query thread row {thread_id}."))?;
    if has_row.is_none() {
        insert_thread_sidebar_metadata_row_if_missing(
            &connection,
            codex_home,
            thread_id,
            metadata,
            &columns,
        )?;
    }

    if columns.iter().any(|column| column == "title") {
        if let Some(title) = metadata
            .title
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            connection.execute(
                "update threads set title = ?1 where id = ?2 and (trim(coalesce(title, '')) = '' or title like 'Continue this transferred conversation from its latest unfinished state%')",
                rusqlite::params![title, thread_id],
            )?;
        }
    }
    if columns.iter().any(|column| column == "first_user_message") {
        if let Some(message) = metadata
            .first_user_message
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            connection.execute(
                "update threads set first_user_message = ?1 where id = ?2 and (trim(coalesce(first_user_message, '')) = '' or first_user_message like 'Continue this transferred conversation from its latest unfinished state%')",
                rusqlite::params![message, thread_id],
            )?;
        }
    }
    if columns.iter().any(|column| column == "has_user_event")
        && metadata
            .first_user_message
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    {
        connection.execute(
            "update threads set has_user_event = 1 where id = ?1",
            [thread_id],
        )?;
    }
    if columns.iter().any(|column| column == "archived") {
        if let Some(archived) = metadata.archived {
            connection.execute(
                "update threads set archived = ?1 where id = ?2 and archived != ?1",
                rusqlite::params![archived as i64, thread_id],
            )?;
        }
    }
    if columns.iter().any(|column| column == "updated_at") {
        if let Some(updated_at) = metadata.updated_at {
            connection.execute(
                "update threads set updated_at = ?1 where id = ?2",
                rusqlite::params![updated_at, thread_id],
            )?;
        }
    }
    if columns.iter().any(|column| column == "updated_at_ms") {
        let updated_at_ms = metadata.updated_at_ms.or_else(|| {
            metadata
                .updated_at
                .and_then(|value| value.checked_mul(1000))
        });
        if let Some(updated_at_ms) = updated_at_ms {
            connection.execute(
                "update threads set updated_at_ms = ?1 where id = ?2",
                rusqlite::params![updated_at_ms, thread_id],
            )?;
        }
    }
    Ok(())
}

pub(super) fn insert_thread_sidebar_metadata_row_if_missing(
    connection: &rusqlite::Connection,
    codex_home: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
    columns: &[String],
) -> Result<()> {
    let now = current_unix_timestamp_secs();
    let updated_at = metadata.updated_at.unwrap_or(now);
    let updated_at_ms = metadata
        .updated_at_ms
        .unwrap_or_else(|| updated_at.saturating_mul(1000));
    let title = metadata
        .title
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or(metadata.first_user_message.as_deref())
        .map(truncate_handoff_text)
        .unwrap_or_else(|| thread_id.to_string());
    let first_user_message = metadata
        .first_user_message
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_default()
        .to_string();
    let rollout_path = find_thread_rollout_path(codex_home, thread_id)
        .map(|path| path.display().to_string())
        .unwrap_or_default();

    let mut insert_columns = Vec::new();
    let mut values = Vec::<rusqlite::types::Value>::new();
    let mut push = |column: &str, value: rusqlite::types::Value| {
        if columns.iter().any(|candidate| candidate == column) {
            insert_columns.push(column.to_string());
            values.push(value);
        }
    };
    push("id", thread_id.to_string().into());
    push("rollout_path", rollout_path.into());
    push("created_at", updated_at.into());
    push("updated_at", updated_at.into());
    push("updated_at_ms", updated_at_ms.into());
    push(
        "source",
        metadata
            .source
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("vscode")
            .to_string()
            .into(),
    );
    push(
        "model_provider",
        metadata
            .model_provider
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("openai")
            .to_string()
            .into(),
    );
    push(
        "cwd",
        metadata
            .cwd
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("/")
            .to_string()
            .into(),
    );
    push("title", title.into());
    push("first_user_message", first_user_message.into());
    push(
        "sandbox_policy",
        metadata
            .sandbox_policy
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("workspace-write")
            .to_string()
            .into(),
    );
    push(
        "approval_mode",
        metadata
            .approval_mode
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("never")
            .to_string()
            .into(),
    );
    push("tokens_used", 0_i64.into());
    push("has_user_event", 1_i64.into());
    push(
        "archived",
        (metadata.archived.unwrap_or(false) as i64).into(),
    );

    if !insert_columns.iter().any(|column| column == "id") {
        return Ok(());
    }
    let placeholders = (1..=insert_columns.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>();
    let sql = format!(
        "insert or ignore into threads ({}) values ({})",
        insert_columns.join(", "),
        placeholders.join(", ")
    );
    connection
        .execute(&sql, rusqlite::params_from_iter(values))
        .with_context(|| format!("Failed to insert target thread metadata row {thread_id}."))?;
    Ok(())
}

pub(super) fn find_thread_rollout_path(codex_home: &Path, thread_id: &str) -> Option<PathBuf> {
    if !codex_home.exists() {
        return None;
    }
    let mut stack = vec![codex_home.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).ok()?;
            if metadata.is_dir() {
                stack.push(path);
                continue;
            }
            let file_name_matches = path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains(thread_id))
                .unwrap_or(false);
            if file_name_matches {
                return Some(path);
            }
        }
    }
    None
}

pub(super) fn ensure_thread_rollout_user_message_event(
    codex_home: &Path,
    thread_id: &str,
    handoff: &ThreadHandoff,
) -> Result<()> {
    let message = handoff
        .metadata
        .first_user_message
        .clone()
        .or_else(|| first_user_message_from_response_items(&handoff.items))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let Some(message) = message else {
        return Ok(());
    };
    let Some(path) = find_thread_rollout_path(codex_home, thread_id) else {
        return Ok(());
    };
    let contents =
        fs::read_to_string(&path).with_context(|| format!("Failed to read {}.", path.display()))?;
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let payload = value.get("payload").unwrap_or(&value);
        if payload.get("type").and_then(Value::as_str) == Some("user_message")
            && payload
                .get("message")
                .and_then(Value::as_str)
                .map(str::trim)
                == Some(message.as_str())
        {
            return Ok(());
        }
    }
    let event = json!({
        "timestamp": current_rfc3339_timestamp(),
        "type": "event_msg",
        "payload": {
            "type": "user_message",
            "message": message,
            "images": [],
            "local_images": [],
            "text_elements": [],
        }
    });
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .with_context(|| format!("Failed to open {} for append.", path.display()))?;
    if !contents.ends_with('\n') {
        writeln!(file)
            .with_context(|| format!("Failed to append newline to {}.", path.display()))?;
    }
    writeln!(file, "{}", serde_json::to_string(&event)?)
        .with_context(|| format!("Failed to append user message event to {}.", path.display()))
}

pub(super) fn first_user_message_from_thread_rollout(
    codex_home: &Path,
    thread_id: &str,
) -> Result<Option<String>> {
    let Some(path) = find_thread_rollout_path(codex_home, thread_id) else {
        return Ok(None);
    };
    let contents =
        fs::read_to_string(&path).with_context(|| format!("Failed to read {}.", path.display()))?;
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(text) = first_user_message_from_rollout_event(&value) {
            return Ok(Some(truncate_handoff_text(&text)));
        }
    }
    Ok(None)
}

pub(super) fn first_user_message_from_rollout_event(value: &Value) -> Option<String> {
    let payload = value.get("payload").unwrap_or(value);
    if payload.get("type").and_then(Value::as_str) == Some("user_message") {
        return payload
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .filter(|text| !is_handoff_context_user_text(text))
            .map(ToOwned::to_owned);
    }
    if payload.get("type").and_then(Value::as_str) == Some("message")
        && payload.get("role").and_then(Value::as_str) == Some("user")
    {
        return payload
            .get("content")
            .and_then(Value::as_array)
            .map(|content| {
                content
                    .iter()
                    .filter_map(|entry| {
                        let entry_type = entry.get("type").and_then(Value::as_str)?;
                        if matches!(entry_type, "input_text" | "text") {
                            entry.get("text").and_then(Value::as_str)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty() && !is_handoff_context_user_text(text));
    }
    None
}

pub(super) fn current_unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

pub(super) fn publish_thread_sidebar_metadata_to_session_index(
    codex_home: &Path,
    thread_id: &str,
    metadata: &ThreadHandoffMetadata,
) -> Result<()> {
    let Some(title) = metadata
        .title
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(());
    };
    let path = codex_home.join("session_index.jsonl");
    let updated_at = metadata
        .session_index_updated_at
        .clone()
        .or_else(|| metadata.updated_at_ms.and_then(timestamp_millis_to_rfc3339))
        .or_else(|| metadata.updated_at.and_then(timestamp_secs_to_rfc3339))
        .unwrap_or_else(current_rfc3339_timestamp);
    let replacement = json!({
        "id": thread_id,
        "thread_name": title,
        "updated_at": updated_at,
    });
    let replacement_line = serde_json::to_string(&replacement)?;
    let contents = fs::read_to_string(&path).unwrap_or_default();
    let mut replaced = false;
    let mut lines = Vec::new();
    for line in contents.lines() {
        let should_replace = serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|value| value.get("id").and_then(Value::as_str).map(str::to_owned))
            .map(|id| id == thread_id)
            .unwrap_or(false);
        if should_replace {
            if !replaced {
                lines.push(replacement_line.clone());
                replaced = true;
            }
        } else {
            lines.push(line.to_string());
        }
    }
    if !replaced {
        lines.push(replacement_line);
    }
    let mut output = lines.join("\n");
    output.push('\n');
    fs::write(&path, output).with_context(|| format!("Failed to write {}.", path.display()))
}

pub(super) fn cleanup_stale_thread_handoff_source(
    codex_home: &Path,
    target_thread_id: &str,
    source_thread_id: &str,
) -> Result<()> {
    if target_thread_id == source_thread_id {
        return Ok(());
    }

    if let Some(state_db_path) = resolve_state_db_file_in_codex_home(codex_home) {
        if state_db_path.exists() {
            cleanup_stale_thread_state_db_row(&state_db_path, source_thread_id)?;
        }
    }
    cleanup_stale_thread_session_index_row(codex_home, source_thread_id)?;
    cleanup_stale_thread_artifact_files(codex_home, source_thread_id)?;
    Ok(())
}

pub(super) fn cleanup_stale_thread_state_db_row(
    state_db_path: &Path,
    source_thread_id: &str,
) -> Result<()> {
    let connection = rusqlite::Connection::open(state_db_path)
        .with_context(|| format!("Failed to open {}.", state_db_path.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .with_context(|| format!("Failed to configure {}.", state_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(());
    }
    let columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !columns.iter().any(|column| column == "id") {
        return Ok(());
    }
    connection
        .execute("delete from threads where id = ?1", [source_thread_id])
        .with_context(|| {
            format!(
                "Failed to remove stale source thread row {source_thread_id} from {}.",
                state_db_path.display()
            )
        })?;
    Ok(())
}

pub(super) fn cleanup_stale_thread_session_index_row(
    codex_home: &Path,
    source_thread_id: &str,
) -> Result<()> {
    let path = codex_home.join("session_index.jsonl");
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("Failed to read {}.", path.display()))
        }
    };
    let mut changed = false;
    let mut lines = Vec::new();
    for line in contents.lines() {
        let is_source_thread = serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|value| value.get("id").and_then(Value::as_str).map(str::to_owned))
            .map(|id| id == source_thread_id)
            .unwrap_or(false);
        if is_source_thread {
            changed = true;
        } else {
            lines.push(line.to_string());
        }
    }
    if !changed {
        return Ok(());
    }
    let output = if lines.is_empty() {
        String::new()
    } else {
        let mut output = lines.join("\n");
        output.push('\n');
        output
    };
    fs::write(&path, output).with_context(|| format!("Failed to write {}.", path.display()))
}

pub(super) fn cleanup_stale_thread_artifact_files(
    codex_home: &Path,
    source_thread_id: &str,
) -> Result<()> {
    for root in [
        codex_home.join("sessions"),
        codex_home.join("archived_sessions"),
        codex_home.join("shell_snapshots"),
    ] {
        remove_thread_artifact_files_with_id(&root, source_thread_id)?;
    }
    Ok(())
}

pub(super) fn remove_thread_artifact_files_with_id(root: &Path, thread_id: &str) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir).with_context(|| {
            format!(
                "Failed to read thread artifact directory {}.",
                dir.display()
            )
        })?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("Failed to inspect {}.", path.display()))?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                stack.push(path);
                continue;
            }
            let file_name_matches = path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains(thread_id))
                .unwrap_or(false);
            if file_name_matches {
                remove_path_if_exists(&path)?;
            }
        }
    }
    Ok(())
}

pub(super) fn timestamp_millis_to_rfc3339(value: i64) -> Option<String> {
    Utc.timestamp_millis_opt(value)
        .single()
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Micros, true))
}

pub(super) fn timestamp_secs_to_rfc3339(value: i64) -> Option<String> {
    Utc.timestamp_opt(value, 0)
        .single()
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Micros, true))
}

pub(super) fn current_rfc3339_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub(super) fn map_thread_item_to_response_item(item: &Value) -> Option<Value> {
    let item_type = item.get("type").and_then(Value::as_str)?;
    match item_type {
        "message" => message_item_to_response_item(item),
        "userMessage" | "user_message" => {
            let text = item
                .get("content")
                .and_then(Value::as_array)
                .map(|content| {
                    content
                        .iter()
                        .filter_map(user_input_to_text)
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .or_else(|| {
                    item.get("message")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .or_else(|| {
                    item.get("text")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_default();
            (!text.trim().is_empty()).then(|| {
                json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": normalize_handoff_item_text(&text),
                        }
                    ]
                })
            })
        }
        "agentMessage" => item
            .get("text")
            .and_then(Value::as_str)
            .map(assistant_message_item),
        "plan" => item
            .get("text")
            .and_then(Value::as_str)
            .map(|text| assistant_message_item(&format!("[Plan]\n{text}"))),
        "reasoning" => {
            let summary = item
                .get("summary")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            (!summary.is_empty())
                .then(|| assistant_message_item(&format!("[Reasoning Summary]\n{summary}")))
        }
        "commandExecution" => {
            let command = item.get("command").and_then(Value::as_str).unwrap_or("");
            let output = item
                .get("aggregatedOutput")
                .and_then(Value::as_str)
                .unwrap_or("");
            let combined = if output.trim().is_empty() {
                format!("[Command]\n{command}")
            } else {
                format!("[Command]\n{command}\n\n[Output]\n{output}")
            };
            Some(assistant_message_item(&combined))
        }
        _ => None,
    }
}

pub(super) fn message_item_to_response_item(item: &Value) -> Option<Value> {
    let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
    let text = item
        .get("content")
        .and_then(Value::as_array)
        .map(|content| {
            content
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if text.trim().is_empty() {
        return None;
    }
    if role == "user" {
        return Some(json!({
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": normalize_handoff_item_text(&text),
                }
            ]
        }));
    }
    Some(assistant_message_item(&text))
}

pub(super) fn assistant_message_item(text: &str) -> Value {
    json!({
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "output_text",
                "text": normalize_handoff_item_text(text),
            }
        ]
    })
}

pub(super) fn user_input_to_text(item: &Value) -> Option<String> {
    match item.get("type").and_then(Value::as_str)? {
        "text" => item
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        "image" => item
            .get("url")
            .and_then(Value::as_str)
            .map(|value| format!("[Image] {value}")),
        "localImage" => item
            .get("path")
            .and_then(Value::as_str)
            .map(|value| format!("[Local image] {value}")),
        "skill" => item
            .get("name")
            .and_then(Value::as_str)
            .map(|value| format!("[Skill] {value}")),
        "mention" => item
            .get("name")
            .and_then(Value::as_str)
            .map(|value| format!("[Mention] {value}")),
        _ => None,
    }
}

pub(super) fn truncate_handoff_text(value: &str) -> String {
    let mut normalized = value.trim().to_string();
    if normalized.chars().count() > MAX_HANDOFF_TEXT_CHARS {
        normalized = normalized
            .chars()
            .take(MAX_HANDOFF_TEXT_CHARS)
            .collect::<String>();
        normalized.push_str("\n[… truncated]");
    }
    normalized
}

pub(super) fn normalize_handoff_item_text(value: &str) -> String {
    value.trim().to_string()
}

pub trait ConversationTransport {
    fn list_threads(&self) -> Result<Vec<String>>;
    fn read_thread(&self, thread_id: &str) -> Result<Value>;
    fn thread_exists(&self, _thread_id: &str) -> Result<bool> {
        Ok(true)
    }
    fn start_thread(&self, cwd: Option<&str>) -> Result<String>;
    fn inject_items(&self, thread_id: &str, items: Vec<Value>) -> Result<()>;
    fn read_thread_ui_metadata(&self, _thread_id: &str) -> Result<ThreadHandoffMetadata> {
        Ok(ThreadHandoffMetadata::default())
    }
    fn ensure_thread_user_message_event(
        &self,
        _thread_id: &str,
        _handoff: &ThreadHandoff,
    ) -> Result<()> {
        Ok(())
    }
    fn publish_thread_metadata(
        &self,
        _thread_id: &str,
        _metadata: &ThreadHandoffMetadata,
    ) -> Result<()> {
        Ok(())
    }
    fn publish_thread_handoff_metadata(
        &self,
        thread_id: &str,
        handoff: &ThreadHandoff,
    ) -> Result<()> {
        self.publish_thread_metadata(thread_id, &handoff.metadata)
    }
    fn cleanup_replaced_thread(&self, _kept_thread_id: &str, _stale_thread_id: &str) -> Result<()> {
        Ok(())
    }
}

pub struct HostConversationTransport {
    port: u16,
}

impl HostConversationTransport {
    pub fn new(port: u16) -> Self {
        Self { port }
    }
}

pub(super) fn host_thread_not_ready_message(message: &str) -> bool {
    let message = message.to_lowercase();
    message.contains("includeturns is unavailable before first user message")
        || message.contains("thread not loaded")
        || message.contains("is not materialized yet")
        || message.contains("no rollout found for thread id")
}

pub(super) fn thread_not_found_message(message: &str) -> bool {
    let message = message.to_lowercase();
    message.contains("thread not found")
        || message.contains("no rollout found for thread id")
        || message.contains("no thread found")
        || message.contains("unknown thread")
        || message.contains("does not exist")
        || (message.contains("failed to load thread history")
            && message.contains("no such file or directory"))
}

pub(super) fn thread_rollout_artifact_exists(codex_home: &Path, thread_id: &str) -> bool {
    find_thread_rollout_path(&codex_home.join("sessions"), thread_id).is_some()
        || find_thread_rollout_path(&codex_home.join("archived_sessions"), thread_id).is_some()
}

pub(super) fn thread_state_db_indicates_existing_thread(
    codex_home: &Path,
    thread_id: &str,
) -> Result<bool> {
    let Some(state_db_path) = resolve_state_db_file_in_codex_home(codex_home) else {
        return Ok(false);
    };
    if !state_db_path.exists() {
        return Ok(false);
    }
    let connection = rusqlite::Connection::open(&state_db_path)
        .with_context(|| format!("Failed to open {}.", state_db_path.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .with_context(|| format!("Failed to configure {}.", state_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(false);
    }
    let columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !columns.iter().any(|column| column == "id") {
        return Ok(false);
    }

    let archived_expr = if columns.iter().any(|column| column == "archived") {
        "coalesce(archived, 0)"
    } else {
        "0"
    };
    let rollout_path_expr = if columns.iter().any(|column| column == "rollout_path") {
        "coalesce(rollout_path, '')"
    } else {
        "''"
    };
    let sql =
        format!("select {archived_expr}, {rollout_path_expr} from threads where id = ?1 limit 1");
    let row = connection
        .query_row(&sql, [thread_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .optional()
        .with_context(|| {
            format!(
                "Failed to query thread row {thread_id} in {}.",
                state_db_path.display()
            )
        })?;
    let Some((archived, rollout_path)) = row else {
        return Ok(false);
    };
    if archived != 0 {
        return Ok(true);
    }
    let rollout_path = rollout_path.trim();
    Ok(!rollout_path.is_empty() && Path::new(rollout_path).exists())
}

pub(super) fn wait_for_host_thread_materialization(port: u16, thread_id: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match send_codex_app_request::<Value>(port, "thread/read", json!({ "threadId": thread_id }))
        {
            Ok(response) if response.get("thread").is_some() => return Ok(()),
            Ok(_) => {}
            Err(error) => {
                let message = format!("{:#}", error);
                if !host_thread_not_ready_message(&message) {
                    return Err(error);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "Timed out waiting for imported thread {thread_id} to materialize in Codex."
            ));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

pub(super) fn read_host_global_state_value(port: u16, key: &str) -> Result<Value> {
    let response: Value =
        send_codex_host_fetch_request(port, "get-global-state", json!({ "key": key }))?;
    Ok(response.get("value").cloned().unwrap_or(Value::Null))
}

pub(super) fn write_host_global_state_value(port: u16, key: &str, value: Value) -> Result<()> {
    let _: Value = send_codex_host_fetch_request(
        port,
        "set-global-state",
        json!({ "key": key, "value": value }),
    )?;
    Ok(())
}

pub(super) fn host_projectless_thread_ids(port: u16) -> Result<BTreeSet<String>> {
    Ok(
        read_host_global_state_value(port, PROJECTLESS_THREAD_IDS_KEY)?
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
    )
}

pub(super) fn host_thread_workspace_root_hints(port: u16) -> Result<BTreeMap<String, String>> {
    Ok(
        read_host_global_state_value(port, THREAD_WORKSPACE_ROOT_HINTS_KEY)?
            .as_object()
            .into_iter()
            .flat_map(|object| object.iter())
            .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_string())))
            .collect(),
    )
}

pub(super) fn publish_projectless_thread_ui_metadata(
    port: u16,
    thread_id: &str,
    source_thread_id: Option<&str>,
    metadata: &ThreadHandoffMetadata,
) -> Result<()> {
    if metadata.projectless != Some(true) {
        return Ok(());
    }

    let mut ids = host_projectless_thread_ids(port)?;
    let mut ids_changed = false;
    if let Some(source_thread_id) = source_thread_id.filter(|source| *source != thread_id) {
        ids_changed |= ids.remove(source_thread_id);
    }
    ids_changed |= ids.insert(thread_id.to_string());
    if ids_changed {
        write_host_global_state_value(
            port,
            PROJECTLESS_THREAD_IDS_KEY,
            Value::Array(ids.into_iter().map(Value::String).collect()),
        )?;
    }

    let root_hint = metadata
        .workspace_root_hint
        .as_deref()
        .or(metadata.cwd.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(root_hint) = root_hint {
        let mut hints = host_thread_workspace_root_hints(port)?;
        let mut hints_changed = false;
        if let Some(source_thread_id) = source_thread_id.filter(|source| *source != thread_id) {
            hints_changed |= hints.remove(source_thread_id).is_some();
        }
        if hints.get(thread_id).map(String::as_str) != Some(root_hint) {
            hints.insert(thread_id.to_string(), root_hint.to_string());
            hints_changed = true;
        }
        if hints_changed {
            let value = Value::Object(
                hints
                    .into_iter()
                    .map(|(key, value)| (key, Value::String(value)))
                    .collect(),
            );
            write_host_global_state_value(port, THREAD_WORKSPACE_ROOT_HINTS_KEY, value)?;
        }
    }

    Ok(())
}

impl ConversationTransport for HostConversationTransport {
    fn list_threads(&self) -> Result<Vec<String>> {
        crate::thread_recovery::read_active_thread_ids(Some(self.port))
    }

    fn read_thread(&self, thread_id: &str) -> Result<Value> {
        send_codex_app_request(
            self.port,
            "thread/read",
            json!({ "threadId": thread_id, "includeTurns": true }),
        )
    }

    fn thread_exists(&self, thread_id: &str) -> Result<bool> {
        let paths = crate::paths::resolve_paths()?;
        if thread_state_db_indicates_existing_thread(&paths.codex_home, thread_id)? {
            return Ok(true);
        }
        if !thread_rollout_artifact_exists(&paths.codex_home, thread_id) {
            return Ok(false);
        }
        match wait_for_host_thread_materialization(self.port, thread_id) {
            Ok(()) => match self.read_thread(thread_id) {
                Ok(_) => Ok(true),
                Err(error) => {
                    let message = format!("{:#}", error);
                    if thread_not_found_message(&message) {
                        Ok(false)
                    } else {
                        Err(error)
                    }
                }
            },
            Err(error) => {
                let message = format!("{:#}", error);
                if thread_not_found_message(&message) {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn start_thread(&self, cwd: Option<&str>) -> Result<String> {
        let response: Value = send_codex_app_request(
            self.port,
            "thread/start",
            json!({
                "cwd": cwd,
                "model": Value::Null,
                "modelProvider": Value::Null,
                "serviceTier": Value::Null,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandbox": Value::Null,
                "personality": "pragmatic",
            }),
        )?;
        let thread_id = response
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| anyhow!("Codex thread/start did not return a thread id."))?;
        wait_for_host_thread_materialization(self.port, &thread_id)?;
        Ok(thread_id)
    }

    fn inject_items(&self, thread_id: &str, items: Vec<Value>) -> Result<()> {
        send_codex_app_request::<Value>(
            self.port,
            "thread/inject_items",
            json!({
                "threadId": thread_id,
                "items": items,
            }),
        )
        .map(|_| ())
    }

    fn read_thread_ui_metadata(&self, thread_id: &str) -> Result<ThreadHandoffMetadata> {
        let projectless_ids = host_projectless_thread_ids(self.port)?;
        let mut workspace_root_hints = host_thread_workspace_root_hints(self.port)?;
        let workspace_root_hint = workspace_root_hints
            .remove(thread_id)
            .filter(|value| !value.trim().is_empty());
        Ok(ThreadHandoffMetadata {
            projectless: Some(projectless_ids.contains(thread_id)),
            workspace_root_hint,
            ..ThreadHandoffMetadata::default()
        })
    }

    fn ensure_thread_user_message_event(
        &self,
        thread_id: &str,
        handoff: &ThreadHandoff,
    ) -> Result<()> {
        let paths = crate::paths::resolve_paths()?;
        ensure_thread_rollout_user_message_event(&paths.codex_home, thread_id, handoff)
    }

    fn publish_thread_metadata(
        &self,
        thread_id: &str,
        metadata: &ThreadHandoffMetadata,
    ) -> Result<()> {
        let paths = crate::paths::resolve_paths()?;
        publish_thread_sidebar_metadata_until_visible(
            &paths.codex_home,
            thread_id,
            metadata,
            Duration::from_secs(10),
        )?;
        publish_projectless_thread_ui_metadata(self.port, thread_id, None, metadata)
    }

    fn publish_thread_handoff_metadata(
        &self,
        thread_id: &str,
        handoff: &ThreadHandoff,
    ) -> Result<()> {
        let paths = crate::paths::resolve_paths()?;
        let mut metadata = handoff.metadata.clone();
        fill_if_missing(
            &mut metadata.first_user_message,
            first_user_message_from_response_items(&handoff.items),
        );
        if metadata
            .title
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            metadata.title = metadata
                .first_user_message
                .as_deref()
                .map(truncate_handoff_text)
                .filter(|value| !value.trim().is_empty());
        }
        cleanup_stale_thread_handoff_source(
            &paths.codex_home,
            thread_id,
            &handoff.source_thread_id,
        )?;
        publish_thread_sidebar_metadata_until_visible(
            &paths.codex_home,
            thread_id,
            &metadata,
            Duration::from_secs(10),
        )?;
        publish_projectless_thread_ui_metadata(
            self.port,
            thread_id,
            Some(&handoff.source_thread_id),
            &metadata,
        )
    }

    fn cleanup_replaced_thread(&self, kept_thread_id: &str, stale_thread_id: &str) -> Result<()> {
        let paths = crate::paths::resolve_paths()?;
        cleanup_stale_thread_handoff_source(&paths.codex_home, kept_thread_id, stale_thread_id)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ThreadHandoff {
    pub source_thread_id: String,
    pub lineage_id: String,
    pub watermark: Option<String>,
    pub cwd: Option<String>,
    pub items: Vec<Value>,
    #[serde(default)]
    pub metadata: ThreadHandoffMetadata,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ThreadHandoffMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_user_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_index_updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projectless: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadHandoffImportOutcome {
    pub completed_source_thread_ids: Vec<String>,
    pub failures: Vec<ThreadHandoffImportFailure>,
    pub prevented_duplicates_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadHandoffImportFailure {
    pub source_thread_id: String,
    pub created_thread_id: Option<String>,
    pub stage: ThreadHandoffImportFailureStage,
    pub error: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadHandoffImportFailureStage {
    Start,
    InjectItems,
    Metadata,
    Persist,
}

impl ThreadHandoffImportOutcome {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn describe(&self) -> String {
        if self.failures.is_empty() {
            return format!(
                "Imported {} transferred thread(s).",
                self.completed_source_thread_ids.len()
            );
        }

        let completed = self.completed_source_thread_ids.len();
        let failed = self.failures.len();
        let failure = &self.failures[0];
        let created_thread = failure
            .created_thread_id
            .as_ref()
            .map(|thread_id| format!(" after creating {}", thread_id))
            .unwrap_or_default();
        let stage = thread_handoff_import_stage_label(failure.stage);
        format!(
            "Partial thread handoff import: {completed} completed, {failed} failed. Source thread {}{created_thread} failed at {stage}: {}",
            failure.source_thread_id, failure.error
        )
    }
}

pub fn thread_handoff_import_stage_label(stage: ThreadHandoffImportFailureStage) -> &'static str {
    match stage {
        ThreadHandoffImportFailureStage::Start => "thread/start",
        ThreadHandoffImportFailureStage::InjectItems => "thread/inject_items",
        ThreadHandoffImportFailureStage::Metadata => "metadata",
        ThreadHandoffImportFailureStage::Persist => "persist",
    }
}
