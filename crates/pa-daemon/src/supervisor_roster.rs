//! Supervisor-side roster serving: subscribe/unsubscribe handling, worker
//! roster deltas, the stop-path passivation, and the `roster_update` pushes subscribers receive.

use serde_json::Map;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pa_types::daemon::agent_roster::AgentRosterEntry;
use pa_types::daemon::DaemonOutbound;
use serde_json::{json, Value};

use crate::backpressure::RouteAdmission;
use crate::lease::canonical_session_path;
use crate::protocol::{response_failure, response_success, DaemonResponse};
use crate::registry::ResidentWorker;
use crate::supervisor::{ClientRouting, Supervisor, ROUTE_TIMEOUT_MS};
use crate::supervisor_roster_seed::family_descends_from;

/// `worker_roster_delta`'s parsed frame. `sequence` feeds the stale-delta
/// gate (the per-worker watermark drops a delayed older snapshot);
/// `worker_instance_id` is the generation the gate's slot names (a
/// replacement process restarts the counter under a new instance).
pub(crate) struct WorkerRosterDelta {
    pub worker_token: String,
    pub summary: Value,
    pub removed: Vec<String>,
    pub sequence: Option<u64>,
    pub worker_instance_id: Option<String>,
}

impl Supervisor {
    /// `roster_subscribe`: answer with the full roster snapshot (the
    /// caller stores the client flag). Pure in-memory.
    pub(crate) async fn handle_roster_subscribe(
        self: &Arc<Self>,
        command_id: &str,
        type_name: &str,
    ) -> DaemonResponse {
        // A registration seed's pushes must never overtake this answer
        // (a client applying the push first loses the seeded rows).
        let pending = std::mem::take(&mut *self.pending_registration_seeds.lock().unwrap());
        for handle in pending {
            let _ = handle.await;
        }
        let roster = self.roster.lock().unwrap().entries();
        response_success(
            Some(command_id),
            type_name,
            Some(json!({ "roster": roster })),
        )
    }

    /// The seed roots: every worker's `sessionFile` (the durable create's
    /// `sessionPath` as fallback), canonicalized.
    pub(crate) async fn roster_seed_roots(self: &Arc<Self>) -> HashSet<PathBuf> {
        let mut roots = HashSet::new();
        for resident in self.registry.list().await {
            let descriptor = resident.descriptor.lock().await;
            let root = descriptor
                .session_file
                .clone()
                .or_else(|| descriptor.create_command.session_path.clone());
            if let Some(root) = root {
                roots.insert(canonical_session_path(Path::new(&root)));
            }
        }
        roots
    }

    pub(crate) fn handle_roster_unsubscribe(command_id: &str, type_name: &str) -> DaemonResponse {
        response_success(Some(command_id), type_name, None)
    }

    pub(crate) async fn handle_worker_roster_delta(
        self: &Arc<Self>,
        command_id: &str,
        type_name: &str,
        delta: WorkerRosterDelta,
    ) -> DaemonResponse {
        let WorkerRosterDelta {
            worker_token,
            summary,
            removed,
            sequence,
            worker_instance_id,
        } = delta;
        let Some(resident) = self.registry.find_by_token(&worker_token).await else {
            return response_failure(
                Some(command_id),
                type_name,
                "Worker authentication failed",
                None,
            );
        };
        // One per-worker critical section spans the roster write and the
        // identity follow (the descriptor lock): no half-applied swap; the
        // gate and the write share one roster lock, so accept order is
        // apply order.
        let mut changed = Vec::new();
        let mut removed_ids = Vec::new();
        {
            let mut descriptor = resident.descriptor.lock().await;
            {
                let mut roster = self.roster.lock().unwrap();
                if !roster.accept_delta_sequence(
                    &resident.worker_id,
                    worker_instance_id.as_deref().unwrap_or(""),
                    sequence.unwrap_or(0),
                ) {
                    return response_success(Some(command_id), type_name, None);
                }
                let entry = roster.write_summary(summary.clone(), Some(&resident.worker_id), None);
                // The worker's root slot can swap to a new durable
                // session: the superseded row must not present as live.
                for swapped in roster.swapped_out_root_rows(&resident.worker_id, &entry) {
                    roster.delete(&swapped);
                    removed_ids.push(swapped);
                }
                changed.push(entry);
                for agent_id in removed {
                    if roster.get(&agent_id).is_some() {
                        roster.delete(&agent_id);
                        removed_ids.push(agent_id);
                    }
                }
            }
            // The supervisor-side identity follows the write inside the
            // same critical section; the boot quarantine lifts ONLY on a
            // root-identity-bearing write.
            let root_identity_bearing =
                self.sync_root_identity_from_roster(&resident, &mut descriptor);
            if root_identity_bearing {
                resident.clear_identity_quarantine();
            }
        }
        self.push_roster_update(changed, removed_ids);
        response_success(Some(command_id), type_name, None)
    }

    /// Test-support arm: production paths write through the sequence-gated
    /// [`Self::write_roster_summary_for_resident`].
    #[cfg(test)]
    pub(crate) fn write_roster_summary(
        &self,
        summary: &Value,
        worker_id: Option<&str>,
    ) -> Option<AgentRosterEntry> {
        let entry = self
            .roster
            .lock()
            .unwrap()
            .write_summary(summary.clone(), worker_id, None);
        self.push_roster_update(vec![entry.clone()], Vec::new());
        Some(entry)
    }

    pub(crate) async fn write_roster_summary_for_resident(
        self: &Arc<Self>,
        resident: &Arc<ResidentWorker>,
        summary: &Value,
    ) -> Option<AgentRosterEntry> {
        let stamped_instance = summary
            .get("workerInstanceId")
            .and_then(serde_json::Value::as_str)
            .filter(|stamped| !stamped.is_empty());
        let instance = match stamped_instance {
            Some(stamped) => stamped.to_string(),
            None => resident
                .descriptor
                .lock()
                .await
                .worker_instance_id
                .clone()
                .unwrap_or_default(),
        };
        // The counter stamp stays an Option: ABSENT means unsequenced (a
        // legacy summary predating the field) — an authoritative write;
        // PRESENT-and-zero orders like any other snapshot.
        let counter = summary
            .get("rosterDeltaSequence")
            .and_then(serde_json::Value::as_u64);
        let (entry, swapped) = {
            // The pull shares the delta path's per-worker critical
            // section.
            let mut descriptor = resident.descriptor.lock().await;
            let (entry, swapped) = {
                let mut roster = self.roster.lock().unwrap();
                if !roster.accept_roster_pull(&resident.worker_id, &instance, counter) {
                    return None;
                }
                let entry = roster.write_summary(summary.clone(), Some(&resident.worker_id), None);
                // The pull sees the same root-slot swap: the superseded
                // row retires, and the identity follow re-binds.
                let swapped = roster.swapped_out_root_rows(&resident.worker_id, &entry);
                for agent_id in &swapped {
                    roster.delete(agent_id);
                }
                (entry, swapped)
            };
            let root_identity_bearing =
                self.sync_root_identity_from_roster(resident, &mut descriptor);
            if root_identity_bearing {
                resident.clear_identity_quarantine();
            }
            (entry, swapped)
        };
        self.push_roster_update(vec![entry.clone()], swapped);
        Some(entry)
    }

    /// Refresh one resident worker's entry from its live `get_state` (registration, adoption, and
    /// create flows); `false` means the identity reconciliation did not run.
    pub(crate) async fn refresh_roster_entry(
        self: &Arc<Self>,
        resident: &Arc<ResidentWorker>,
    ) -> bool {
        let response = self
            .route_command_typed(
                resident,
                "get_state",
                json!({}),
                ROUTE_TIMEOUT_MS,
                RouteAdmission::SupervisorInternal,
            )
            .await;
        let Ok(response) = response else {
            return false;
        };
        if !response.success {
            return false;
        }
        let Some(data) = response.data else {
            return false;
        };
        self.write_roster_summary_for_resident(resident, &data)
            .await
            .is_some()
    }

    /// `passivate_roster_worker` (TS `flipWorkerRosterEntriesInactive`,
    /// in place — no ledger reseed): the TOP-LEVEL row passivates keeping
    /// every durable display field (`lifecycle` stays `"live"`); an
    /// ephemeral worker's rows die; a subagent row keeps the family walk.
    pub(crate) async fn passivate_roster_worker(
        self: &Arc<Self>,
        worker_id: &str,
        ephemeral: bool,
    ) {
        let (owned, unowned_at_start) = {
            let mut roster = self.roster.lock().unwrap();
            let owned: Vec<AgentRosterEntry> = roster
                .entries_for_worker(worker_id)
                .into_iter()
                .cloned()
                .collect();
            // The unowned rows are snapshotted here: a family whose root
            // registers while this pass awaits would read as unanchored; the
            // snapshot scopes the sweep to rows existing at stop start.
            let unowned_at_start: Vec<AgentRosterEntry> = roster
                .entries()
                .into_iter()
                .filter(|entry| entry.worker_id.is_none())
                .collect();
            // The sequence slot dies BEFORE the ledger/roots awaits: a
            // stop that awaits first would race a re-registration and
            // delete the replacement's FRESH slot.
            roster.forget_worker_sequences(worker_id);
            (owned, unowned_at_start)
        };
        // The stopping worker's family view — live edges and surviving
        // resident roots — decides each subagent row's fate; a ledger
        // failure degrades to an empty view.
        let ledger_view = self.live_edges_and_parents().await;
        let empty_view: (
            Vec<crate::rlm_ledger::RlmLedgerEdge>,
            HashMap<PathBuf, PathBuf>,
        ) = (Vec::new(), HashMap::new());
        let (_, parent_by_child) = ledger_view.as_ref().unwrap_or(&empty_view);
        let roots = self.roster_seed_roots().await;
        // The stop's own ledger event: an RLM delete tombstoned its child
        // before the stop began, and the shutdown route was the
        // transcript's flush barrier, so the fold beside the ledger view
        // reads the final bucket - the later capture amendment yields the
        // same value, so no second push follows it.
        let bucket_fold = self.deleted_descendant_usage_bucket().await;
        // The ledger/roots awaits opened a late-write window. Only
        // rows that carry the STOPPED generation settle here: the
        // stopped worker's own in-flight delta can have written a row
        // the snapshot missed (settle it), but a same-session
        // re-registration reuses the worker id (the sequence-slot fix
        // assumes it) and its replacement rows are LIVE. The registry
        // decides, OUTSIDE the roster lock (an await cannot run under
        // it): the passivation caller removed the stopped resident
        // before this call, so a resident that is BACK in the registry
        // by now belongs to the replacement - return and let the
        // replacement's own registration/refresh own its rows (a
        // just-resumed session must not vanish or render inactive).
        let replacement_live = self
            .registry
            .get(worker_id)
            .await
            .is_some_and(|resident| !resident.route_state().retired);
        if replacement_live {
            return;
        }
        let mut changed = Vec::new();
        let mut removed = Vec::new();
        {
            let mut roster = self.roster.lock().unwrap();
            // The refreshed bucket applies FIRST: the settle loop's
            // passivated rewrites then attach the new value at store
            // time, and the rewritten rows ship in this same push - the
            // child's removal and its parent's new bucket together, with
            // no frame in between where the spend dips.
            if let Some((ticket, bucket)) = bucket_fold {
                changed.extend(roster.set_deleted_descendant_usage(ticket, bucket));
            }
            let mut settle: Vec<AgentRosterEntry> = owned;
            for late in roster.entries_for_worker(worker_id).into_iter().cloned() {
                if !settle
                    .iter()
                    .any(|entry: &AgentRosterEntry| entry.agent_id == late.agent_id)
                {
                    settle.push(late);
                }
            }
            for entry in settle {
                if roster
                    .get(&entry.agent_id)
                    .is_none_or(|current| current.worker_id.as_deref() != Some(worker_id))
                {
                    continue;
                }
                // TOP-LEVEL rows passivate in place (the view merges them
                // with the saved catalog row); a SUBAGENT row keeps the
                // family walk.
                let subagent = entry
                    .summary
                    .get("rlmChildId")
                    .and_then(Value::as_str)
                    .is_some();
                let anchored = !subagent
                    || entry
                        .summary
                        .get("sessionFile")
                        .and_then(Value::as_str)
                        .is_some_and(|file| {
                            parent_by_child
                                .get(&canonical_session_path(Path::new(file)))
                                .is_some_and(|parent| {
                                    family_descends_from(parent_by_child, parent, &roots)
                                })
                        });
                if !ephemeral && entry.queued_child != Some(true) && anchored {
                    let passivated =
                        roster.write_summary(passivated_summary(entry.summary), None, None);
                    changed.push(passivated);
                } else {
                    roster.delete(&entry.agent_id);
                    removed.push(entry.agent_id);
                }
            }
            // The seeded unowned rows outlived their departed root and
            // flashed as top-level rows: settle them with the same anchor
            // verdict as the owned rows, scoped to the snapshot.
            if ledger_view.is_ok() {
                for entry in &unowned_at_start {
                    if roster
                        .get(&entry.agent_id)
                        .is_none_or(|current| current.worker_id.is_some())
                    {
                        continue;
                    }
                    let subagent = entry
                        .summary
                        .get("rlmChildId")
                        .and_then(Value::as_str)
                        .is_some();
                    // Only seeded subagent rows are the sweep's business:
                    // a passivated TOP-LEVEL row is not a dead family's
                    // flash.
                    if !subagent {
                        continue;
                    }
                    let anchored = entry
                        .summary
                        .get("sessionFile")
                        .and_then(Value::as_str)
                        .is_some_and(|file| {
                            parent_by_child
                                .get(&canonical_session_path(Path::new(file)))
                                .is_some_and(|parent| {
                                    family_descends_from(parent_by_child, parent, &roots)
                                })
                        });
                    if !anchored {
                        roster.delete(&entry.agent_id);
                        removed.push(entry.agent_id.clone());
                    }
                }
            }
        }
        self.push_roster_update(changed, removed);
    }

    /// Push one `roster_update` to subscribed clients, content-diffed
    /// per entry against the last published form: an identical rewrite broadcasts nothing.
    pub(crate) fn push_roster_update(&self, changed: Vec<AgentRosterEntry>, removed: Vec<String>) {
        // ONE lock acquisition spans the diff, the rebase, and the send:
        // interleaved pushes would deliver stale A after B and then
        // suppress the correction.
        let mut last = self.last_published_roster.lock().unwrap();
        let mut changed = changed;
        changed.retain(|entry| {
            let Some(published) = serde_json::to_value(entry).ok() else {
                return true; // an unserializable entry always ships
            };
            let id = entry.agent_id.clone();
            let is_new = match last.get(&id) {
                Some(previous) => *previous != published,
                None => true,
            };
            if is_new {
                last.insert(id, published);
            }
            is_new
        });
        let mut removed = removed;
        removed.retain(|id| last.remove(id).is_some());
        if changed.is_empty() && removed.is_empty() {
            return;
        }
        let update = DaemonOutbound::RosterUpdate {
            changed: serde_json::to_value(changed).unwrap_or(Value::Null),
            removed: (!removed.is_empty()).then_some(removed),
            resync: None,
            rest: Map::default(),
        };
        let Ok(payload) = serde_json::to_value(&update) else {
            return;
        };
        let _ = self.events.send((
            ClientRouting::RosterSubscribers,
            std::sync::Arc::new(payload),
        ));
    }

    /// Broadcast one `roster_update` WITHOUT the content-diff guard (the
    /// seeded-row publish's replay contract): the content still REBASES the
    /// last-published map, so a later identical mutation drops.
    pub(crate) fn push_roster_update_unguarded(
        &self,
        changed: &[AgentRosterEntry],
        removed: Vec<String>,
    ) {
        if changed.is_empty() && removed.is_empty() {
            return;
        }
        // The rebase and the send share one lock hold.
        let mut last = self.last_published_roster.lock().unwrap();
        for entry in changed {
            if let Ok(published) = serde_json::to_value(entry) {
                last.insert(entry.agent_id.clone(), published);
            }
            // An unserializable row still shipped; dropping its map
            // entry only makes a later push ship again.
        }
        for id in &removed {
            last.remove(id);
        }
        let update = DaemonOutbound::RosterUpdate {
            changed: serde_json::to_value(changed).unwrap_or(Value::Null),
            removed: (!removed.is_empty()).then_some(removed),
            resync: None,
            rest: Map::default(),
        };
        let Ok(payload) = serde_json::to_value(&update) else {
            return;
        };
        let _ = self.events.send((
            ClientRouting::RosterSubscribers,
            std::sync::Arc::new(payload),
        ));
    }
}

/// TS `passivatedWorkerRosterEntry`: keep every durable display field,
/// strip only the live-runtime fields (heartbeat/cron marks survive).
fn passivated_summary(summary: Value) -> Value {
    let mut summary = summary;
    let Some(object) = summary.as_object_mut() else {
        return summary;
    };
    let keep_heartbeat = object
        .get("hasRegisteredHeartbeat")
        .and_then(Value::as_bool)
        == Some(true);
    let keep_cron = object.get("hasRegisteredCronJob").and_then(Value::as_bool) == Some(true);
    for key in [
        "activeSessionId",
        "directAttachedClients",
        "hasActiveHeartbeat",
        "hasRegisteredHeartbeat",
        "hasRegisteredCronJob",
        "hasRunningRlmChildren",
        "isBashRunning",
        "isRunningTools",
        "workerState",
        "workerPid",
    ] {
        object.remove(key);
    }
    object.insert("activity".to_string(), json!("idle"));
    object.insert("isSessionActive".to_string(), json!(false));
    object.insert("isStreaming".to_string(), json!(false));
    object.insert("isCompacting".to_string(), json!(false));
    object.insert("attachedClients".to_string(), json!(0));
    if keep_heartbeat {
        object.insert("hasRegisteredHeartbeat".to_string(), json!(true));
    }
    if keep_cron {
        object.insert("hasRegisteredCronJob".to_string(), json!(true));
    }
    if let Some(session_id) = object.get("sessionId").and_then(Value::as_str) {
        object.insert("id".to_string(), json!(session_id));
    }
    normalize_model_to_durable_pair(object);
    summary
}

/// The passivated row's `model` is the DURABLE pair `{provider, modelId}` the ledger-seed hydrate
/// writes and the agents view reads; a live summary's fuller descriptor collapses to it.
fn normalize_model_to_durable_pair(object: &mut serde_json::Map<String, Value>) {
    let Some(model) = object.get("model") else {
        return;
    };
    let Some(provider) = model.get("provider").and_then(Value::as_str) else {
        return;
    };
    if model.get("modelId").and_then(Value::as_str).is_some() {
        return;
    }
    let Some(model_id) = model.get("id").and_then(Value::as_str) else {
        return;
    };
    object.insert(
        "model".to_string(),
        json!({ "provider": provider, "modelId": model_id }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor_roster_seed::tests::{
        append_family_edge, drain_pending_seeds_for_tests, live_child_summary,
        register_root_worker, roster_fixture, roster_row_for_child, write_display_file,
    };
    use pa_types::daemon::agent_roster::AgentRosterStatus;

    #[tokio::test]
    async fn subscribe_is_a_pure_in_memory_snapshot() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        register_root_worker(&supervisor, "w-root", &root_file).await;
        let agent_dir = dir.join("agent");
        append_family_edge(
            &agent_dir,
            &agent_dir.join("sessions"),
            "sub-9",
            &root_file,
            &child_file,
        );
        let mut events = supervisor.events.subscribe();
        let mut root_summary = live_child_summary(&root_file, &child_file);
        root_summary["runtimeKind"] = json!("top-level");
        root_summary["sessionId"] = json!("root-persisted");
        root_summary["id"] = json!("root-persisted");
        root_summary["sessionFile"] = json!(root_file.to_string_lossy());
        root_summary.as_object_mut().unwrap().remove("rlmChildId");
        root_summary
            .as_object_mut()
            .unwrap()
            .remove("parentSessionPath");
        supervisor.write_roster_summary(&root_summary, Some("w-root"));
        let _ = drain_roster_pushes(&mut events);

        let first = supervisor
            .handle_roster_subscribe("s1", "roster_subscribe")
            .await;
        assert!(first.success);
        let roster = first.data.expect("roster snapshot")["roster"].clone();
        assert_eq!(
            roster.as_array().map(Vec::len),
            Some(1),
            "only the in-memory row answers: {roster}"
        );
        let second = supervisor
            .handle_roster_subscribe("s2", "roster_subscribe")
            .await;
        assert_eq!(second.data.expect("roster snapshot")["roster"], roster);
        assert!(
            drain_roster_pushes(&mut events).is_empty(),
            "subscribe never pushes"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_identical_rewrite_does_not_broadcast() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        register_root_worker(&supervisor, "w-root", &root_file).await;
        let mut events = supervisor.events.subscribe();
        let mut summary = live_child_summary(&root_file, &child_file);
        summary["runtimeKind"] = json!("top-level");
        summary["sessionId"] = json!("root-persisted");
        summary["id"] = json!("root-persisted");
        summary["sessionFile"] = json!(root_file.to_string_lossy());
        summary.as_object_mut().unwrap().remove("rlmChildId");
        summary.as_object_mut().unwrap().remove("parentSessionPath");
        supervisor.write_roster_summary(&summary, Some("w-root"));
        let first = drain_roster_pushes(&mut events);
        assert_eq!(first.len(), 1, "the first write publishes: {first:?}");

        supervisor.write_roster_summary(&summary, Some("w-root"));
        assert!(
            drain_roster_pushes(&mut events).is_empty(),
            "an identical rewrite broadcasts nothing"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_changed_rewrite_broadcasts_and_resets_the_diff_baseline() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        register_root_worker(&supervisor, "w-root", &root_file).await;
        let mut events = supervisor.events.subscribe();
        let mut summary = live_child_summary(&root_file, &child_file);
        summary["runtimeKind"] = json!("top-level");
        summary["sessionId"] = json!("root-persisted");
        summary["id"] = json!("root-persisted");
        summary["sessionFile"] = json!(root_file.to_string_lossy());
        summary.as_object_mut().unwrap().remove("rlmChildId");
        summary.as_object_mut().unwrap().remove("parentSessionPath");
        supervisor.write_roster_summary(&summary, Some("w-root"));
        let _ = drain_roster_pushes(&mut events);

        summary["model"] = json!("changed-model");
        supervisor.write_roster_summary(&summary, Some("w-root"));
        let changed = drain_roster_pushes(&mut events);
        assert_eq!(changed.len(), 1, "the changed row broadcasts: {changed:?}");
        assert_eq!(
            changed[0]["changed"][0]["summary"]["model"],
            json!("changed-model"),
            "the push carries the changed form: {changed:?}"
        );

        supervisor.write_roster_summary(&summary, Some("w-root"));
        assert!(
            drain_roster_pushes(&mut events).is_empty(),
            "the rebased identical rewrite broadcasts nothing"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stop_passivates_an_anchored_child_preserving_display_fields() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        register_root_worker(&supervisor, "w-root", &root_file).await;
        let agent_dir = dir.join("agent");
        append_family_edge(
            &agent_dir,
            &agent_dir.join("sessions"),
            "sub-9",
            &root_file,
            &child_file,
        );
        let mut events = supervisor.events.subscribe();
        supervisor.write_roster_summary(
            &live_child_summary(&root_file, &child_file),
            Some("w-child"),
        );
        let _ = drain_roster_pushes(&mut events);

        supervisor.passivate_roster_worker("w-child", false).await;
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(pushes.len(), 1, "one settle push: {pushes:?}");
        assert_eq!(pushes[0]["changed"].as_array().map(Vec::len), Some(1));
        assert!(pushes[0]["removed"].is_null() || pushes[0]["removed"].as_array().is_none());
        let row = roster_row_for_child(&supervisor, "sub-9");
        assert_eq!(row.worker_id, None, "the row is no longer worker-owned");
        assert!(row.summary.get("activeSessionId").is_none());
        assert!(row.summary.get("workerState").is_none());
        assert!(row.summary.get("workerPid").is_none());
        assert_eq!(row.summary["activity"], "idle");
        assert_eq!(row.summary["isStreaming"], false);
        assert_eq!(row.status, AgentRosterStatus::Inactive);
        assert_eq!(row.summary["cwd"], "/the/live/cwd");
        assert_eq!(row.summary["model"]["provider"], "live");
        assert_eq!(row.summary["thinkingLevel"], "low");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn passivation_normalizes_the_live_model_descriptor_to_the_durable_pair() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        register_root_worker(&supervisor, "w-root", &root_file).await;
        let agent_dir = dir.join("agent");
        let sessions_dir = agent_dir.join("sessions");
        append_family_edge(&agent_dir, &sessions_dir, "sub-9", &root_file, &child_file);
        let mut live = live_child_summary(&root_file, &child_file);
        live["model"] = json!({
            "id": "mock-1",
            "name": "Mock 1",
            "provider": "battery",
            "reasoning": true,
        });
        let _ = supervisor.write_roster_summary(&live, Some("w-child"));
        let mut events = supervisor.events.subscribe();
        let _ = drain_roster_pushes(&mut events);
        supervisor.passivate_roster_worker("w-child", false).await;
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(pushes.len(), 1, "the passivated row publishes: {pushes:?}");
        let row = roster_row_for_child(&supervisor, "sub-9");
        assert_eq!(
            row.summary["model"],
            json!({ "provider": "battery", "modelId": "mock-1" }),
            "the durable pair, not the live descriptor: {row:?}"
        );
        assert_eq!(row.summary["thinkingLevel"], json!("low"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The RLM delete's stop: the passivation's single push carries both
    /// the tombstoned child's removal and its parent's refreshed
    /// deleted-descendant bucket. The child's transcript survives under
    /// session-artifacts (the real RLM-delete shape: no catalog row
    /// exists for it), so its captured spend bills through the bucket
    /// on the parent's row - not through a row anywhere.
    #[tokio::test]
    async fn stop_carries_the_deleted_childs_spend_to_its_parent() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        let agent_dir = dir.join("agent");
        let sessions_dir = agent_dir.join("sessions");
        register_root_worker(&supervisor, "w-root", &root_file).await;
        // The parent's top-level row: the bucket's target.
        let mut parent_summary = live_child_summary(&root_file, &child_file);
        parent_summary["runtimeKind"] = json!("top-level");
        parent_summary["sessionId"] = json!("root-persisted");
        parent_summary["id"] = json!("root-persisted");
        parent_summary["sessionFile"] = json!(root_file.to_string_lossy());
        parent_summary.as_object_mut().unwrap().remove("rlmChildId");
        parent_summary
            .as_object_mut()
            .unwrap()
            .remove("parentSessionPath");
        supervisor.write_roster_summary(&parent_summary, Some("w-root"));
        // The deleted child's real transcript location: under the agent
        // dir's session-artifacts tree (the file exists - the shape the
        // RLM delete leaves behind).
        let child_artifact = agent_dir
            .join("session-artifacts")
            .join("root-1")
            .join("sub-9")
            .join("sub-9.jsonl");
        std::fs::create_dir_all(child_artifact.parent().expect("artifact dir")).unwrap();
        write_display_file(&child_artifact, "/the/deleted/cwd");
        // The flushed transcript carries the child's final own spend
        // ($0.30): the kill route was the flush barrier, so the
        // passivation fold reads this row.
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&child_artifact)
                .expect("open the child transcript for its billed turn");
            writeln!(
                file,
                "{}",
                json!({
                    "type": "message",
                    "id": "dm1a",
                    "parentId": null,
                    "timestamp": "2026-09-29T00:00:02.100Z",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "work complete"}],
                        "timestamp": 2100,
                        "usage": {
                            "input": 60,
                            "output": 6,
                            "cacheRead": 0,
                            "cacheWrite": 0,
                            "totalTokens": 66,
                            "cost": {"input": 0.0, "output": 0.3, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.3}
                        }
                    }
                })
            )
            .expect("append the billed turn");
        }
        append_family_edge(
            &agent_dir,
            &sessions_dir,
            "sub-9",
            &root_file,
            &child_artifact,
        );
        // The RLM delete's tombstone carries no usage yet (the capture
        // amendment lands after the stop); the passivation fold bills
        // the flushed transcript.
        let ledger = crate::rlm_ledger::RlmSpawnLedger::new(&agent_dir, &sessions_dir, |_| {});
        ledger
            .append_delete(
                "sub-9",
                &child_artifact.to_string_lossy(),
                crate::rlm_ledger::RlmLedgerDeleteReason::User,
            )
            .expect("append delete");
        // The child's live roster row, owned by the stopping worker.
        let mut events = supervisor.events.subscribe();
        supervisor.write_roster_summary(
            &live_child_summary(&root_file, &child_artifact),
            Some("w-child"),
        );
        let child_agent_id = drain_roster_pushes(&mut events)[0]["changed"][0]["agentId"]
            .as_str()
            .expect("the child agent id")
            .to_string();

        supervisor.passivate_roster_worker("w-child", false).await;
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(pushes.len(), 1, "one settle push: {pushes:?}");
        // The parent's refreshed row is the push's whole `changed`.
        let parent_after = supervisor
            .roster
            .lock()
            .unwrap()
            .entries()
            .into_iter()
            .find(|entry| {
                entry.summary.get("sessionId").and_then(Value::as_str) == Some("root-persisted")
            })
            .expect("the parent row");
        assert_eq!(
            pushes[0]["changed"],
            serde_json::to_value(vec![parent_after]).expect("serialized parent"),
            "the push carries the parent's refreshed row: {pushes:?}"
        );
        assert_eq!(
            pushes[0]["changed"][0]["summary"]["deletedDescendantUsage"],
            json!({ "inputTokens": 60, "outputTokens": 6, "cost": 0.3 }),
            "the deleted child's captured spend bills through the parent"
        );
        assert_eq!(
            pushes[0]["removed"],
            json!([child_agent_id]),
            "the tombstoned child's row dies in the same push: {pushes:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A saved delete of a subagent whose only row was the saved listing
    /// (no roster row - its parent is stopped, so no seed ever wrote
    /// one) still refreshes the bucket: the ledger tombstone is the
    /// event, and the parent's roster row bills the deleted child's
    /// captured spend in the delete's own push.
    #[tokio::test]
    async fn a_saved_delete_without_a_roster_row_bills_the_parent() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        let agent_dir = dir.join("agent");
        let sessions_dir = agent_dir.join("sessions");
        register_root_worker(&supervisor, "w-root", &root_file).await;
        // The parent's top-level row: the ONLY roster row (the child has
        // none).
        let mut parent_summary = live_child_summary(&root_file, &child_file);
        parent_summary["runtimeKind"] = json!("top-level");
        parent_summary["sessionId"] = json!("root-persisted");
        parent_summary["id"] = json!("root-persisted");
        parent_summary["sessionFile"] = json!(root_file.to_string_lossy());
        parent_summary.as_object_mut().unwrap().remove("rlmChildId");
        parent_summary
            .as_object_mut()
            .unwrap()
            .remove("parentSessionPath");
        supervisor.write_roster_summary(&parent_summary, Some("w-root"));
        // The child: a transcript under the session-artifacts tree whose
        // header links it to the parent (the capture's child shape), with
        // one billed assistant row ($0.30), plus its spawn edge.
        let child_artifact = agent_dir
            .join("session-artifacts")
            .join("root-1")
            .join("sub-9")
            .join("sub-9.jsonl");
        std::fs::create_dir_all(child_artifact.parent().expect("artifact dir")).unwrap();
        std::fs::write(
            &child_artifact,
            format!(
                "{{\"type\":\"session\",\"version\":3,\"id\":\"sub-9\",\"timestamp\":\"2024-01-01T00:00:00.000Z\",\"cwd\":\"/the/deleted/cwd\",\"parentSession\":\"{}\",\"rlmDepth\":1}}\n",
                root_file.to_string_lossy()
            ),
        )
        .unwrap();
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&child_artifact)
                .expect("open the child transcript for its billed turn");
            writeln!(
                file,
                "{}",
                json!({
                    "type": "message",
                    "id": "dm1a",
                    "parentId": null,
                    "timestamp": "2026-09-29T00:00:02.100Z",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "work complete"}],
                        "timestamp": 2100,
                        "usage": {
                            "input": 60,
                            "output": 6,
                            "cacheRead": 0,
                            "cacheWrite": 0,
                            "totalTokens": 66,
                            "cost": {"input": 0.0, "output": 0.3, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.3}
                        }
                    }
                })
            )
            .expect("append the billed turn");
        }
        append_family_edge(
            &agent_dir,
            &sessions_dir,
            "sub-9",
            &root_file,
            &child_artifact,
        );
        // The selector-less saved delete of the child: the supervisor arm.
        let mut events = supervisor.events.subscribe();
        let command = pa_types::daemon::DaemonCommand::DeleteSavedSession {
            id: None,
            active_session_id: None,
            session_path: child_artifact.to_string_lossy().to_string(),
            rest: Map::default(),
        };
        let (responses, _) = supervisor
            .handle_delete_saved_session(&command, "client-1", "c1", "delete_saved_session")
            .await;
        assert_eq!(
            responses[0]["success"],
            json!(true),
            "the delete succeeded: {responses:?}"
        );
        assert!(!child_artifact.is_file(), "the child transcript is gone");
        // The delete's own push carries the parent's refreshed row - the
        // whole `changed`, with no removal (the child had no row).
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(pushes.len(), 1, "one refresh push: {pushes:?}");
        let parent_after = supervisor
            .roster
            .lock()
            .unwrap()
            .entries()
            .into_iter()
            .find(|entry| {
                entry.summary.get("sessionId").and_then(Value::as_str) == Some("root-persisted")
            })
            .expect("the parent row");
        assert_eq!(
            pushes[0]["changed"],
            serde_json::to_value(vec![parent_after]).expect("serialized parent"),
            "the push carries the parent's refreshed row: {pushes:?}"
        );
        assert_eq!(
            pushes[0]["changed"][0]["summary"]["deletedDescendantUsage"],
            json!({ "inputTokens": 60, "outputTokens": 6, "cost": 0.3 }),
            "the deleted child's captured spend bills through the parent"
        );
        assert!(
            pushes[0].get("removed").is_none() || pushes[0]["removed"].is_null(),
            "no removal rides the push: {pushes:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Unanchored subagent rows are removed, never passivated (a
    /// tombstoned ledger edge - the user deleted the subagent - or a live
    /// edge with no resident root), a queued child and an ephemeral
    /// worker's rows die with the stop, and the TOP-LEVEL row passivates
    /// (TS keeps every stopped non-ephemeral row visible: the operator's
    /// rows-disappear report), surviving later stops' unowned sweeps.
    #[tokio::test]
    async fn stop_removes_unanchored_children_and_passivates_the_top_level_row() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        let agent_dir = dir.join("agent");
        let sessions_dir = agent_dir.join("sessions");
        let mut events = supervisor.events.subscribe();

        // A tombstoned child: the deleted edge means no live edge carries the row.
        append_family_edge(&agent_dir, &sessions_dir, "sub-9", &root_file, &child_file);
        let ledger = crate::rlm_ledger::RlmSpawnLedger::new(&agent_dir, &sessions_dir, |_| {});
        ledger
            .append_delete(
                "sub-9",
                &child_file.to_string_lossy(),
                crate::rlm_ledger::RlmLedgerDeleteReason::User,
            )
            .expect("append delete");
        register_root_worker(&supervisor, "w-root", &root_file).await;
        supervisor.write_roster_summary(
            &live_child_summary(&root_file, &child_file),
            Some("w-child"),
        );
        let _ = drain_roster_pushes(&mut events);
        supervisor.passivate_roster_worker("w-child", false).await;
        let pushes = drain_roster_pushes(&mut events);
        let removed: Vec<String> = pushes[0]["removed"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|id| id.as_str().map(str::to_string))
            .collect();
        assert_eq!(removed.len(), 1, "the tombstoned row dies: {pushes:?}");
        assert!(supervisor.roster.lock().unwrap().get(&removed[0]).is_none());

        let orphan_file = sessions_dir.join("sub-orphan.jsonl");
        write_display_file(&orphan_file, "/the/orphan/cwd");
        append_family_edge(
            &agent_dir,
            &sessions_dir,
            "sub-orphan",
            &root_file,
            &orphan_file,
        );
        let mut orphan_summary = live_child_summary(&root_file, &orphan_file);
        orphan_summary["rlmChildId"] = json!("sub-orphan");
        supervisor.write_roster_summary(&orphan_summary, Some("w-orphan"));
        for worker in supervisor.registry.list().await {
            supervisor.registry.remove(&worker.worker_id).await;
        }
        let _ = drain_roster_pushes(&mut events);
        supervisor.passivate_roster_worker("w-orphan", false).await;
        let pushes = drain_roster_pushes(&mut events);
        assert!(
            pushes[0]["removed"]
                .as_array()
                .is_some_and(|ids| !ids.is_empty()),
            "an unanchored row dies: {pushes:?}"
        );

        // A top-level row PASSIVATES: `lifecycle` stays "live", the row stays visible.
        let mut top_summary = live_child_summary(&root_file, &child_file);
        top_summary["runtimeKind"] = json!("top-level");
        top_summary["sessionId"] = json!("root-persisted");
        top_summary["id"] = json!("root-persisted");
        top_summary["sessionFile"] = json!(root_file.to_string_lossy());
        // The summary carries its lifecycle (the view's visibility gate).
        top_summary["lifecycle"] = json!("live");
        top_summary.as_object_mut().unwrap().remove("rlmChildId");
        top_summary
            .as_object_mut()
            .unwrap()
            .remove("parentSessionPath");
        supervisor.write_roster_summary(&top_summary, Some("w-top"));
        let _ = drain_roster_pushes(&mut events);
        supervisor.passivate_roster_worker("w-top", false).await;
        let pushes = drain_roster_pushes(&mut events);
        assert!(
            pushes[0]["changed"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry["summary"]["sessionId"] == json!("root-persisted")
                        && entry["status"] == json!("inactive")
                        && entry["summary"]["lifecycle"] == json!("live")
                })),
            "the top-level row passivates (lifecycle stays live): {pushes:?}"
        );
        assert!(
            pushes[0]["removed"].is_null() || pushes[0]["removed"] == json!([]),
            "the passivated top-level row is not a removal: {pushes:?}"
        );
        let entries = supervisor.roster.lock().unwrap().entries();
        let passivated = entries
            .iter()
            .find(|entry| {
                entry.summary.get("sessionId").and_then(Value::as_str) == Some("root-persisted")
            })
            .expect("the passivated top-level row stays in the roster");
        assert_eq!(passivated.status, AgentRosterStatus::Inactive);
        assert!(
            passivated.summary.get("workerState").is_none()
                && passivated.summary.get("activeSessionId").is_none(),
            "the live-only fields dropped with the passivation: {passivated:?}"
        );
        let mut queued_summary = live_child_summary(&root_file, &child_file);
        queued_summary["rlmChildId"] = json!("sub-queued");
        queued_summary["queuedChild"] = json!(true);
        let queued_child_file = sessions_dir.join("sub-queued.jsonl");
        write_display_file(&queued_child_file, "/the/queued/cwd");
        append_family_edge(
            &agent_dir,
            &sessions_dir,
            "sub-queued",
            &root_file,
            &queued_child_file,
        );
        queued_summary["sessionFile"] = json!(queued_child_file.to_string_lossy());
        supervisor.write_roster_summary(&queued_summary, Some("w-queued"));
        let _ = drain_roster_pushes(&mut events);
        supervisor.passivate_roster_worker("w-queued", true).await;
        let pushes = drain_roster_pushes(&mut events);
        assert!(
            pushes[0]["removed"]
                .as_array()
                .is_some_and(|ids| ids.len() == 1),
            "the queued/ephemeral row dies: {pushes:?}"
        );
        assert!(supervisor
            .roster
            .lock()
            .unwrap()
            .entries()
            .iter()
            .all(
                |entry| entry.summary.get("rlmChildId").and_then(Value::as_str)
                    != Some("sub-queued")
            ));

        // A later stop's sweep never revisits the passivated row (dead families only).
        supervisor.passivate_roster_worker("w-none", false).await;
        let roster = supervisor.roster.lock().unwrap();
        assert!(
            roster.entries().iter().any(|entry| {
                entry.summary.get("sessionId").and_then(Value::as_str) == Some("root-persisted")
            }),
            "the passivated top-level row survives later stops' sweeps"
        );
        drop(roster);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Orphaned family's seeded rows die; another root's anchored row keeps its display.
    #[tokio::test]
    async fn stop_prunes_seeded_rows_of_a_family_that_lost_its_root() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        let agent_dir = dir.join("agent");
        let sessions_dir = agent_dir.join("sessions");
        let survivor_file = sessions_dir.join("root-2.jsonl");
        let survivor_child = sessions_dir.join("sub-live.jsonl");
        write_display_file(&survivor_file, "/the/survivor/cwd");
        write_display_file(&survivor_child, "/the/survivor/child/cwd");
        register_root_worker(&supervisor, "w-survivor", &survivor_file).await;
        append_family_edge(
            &agent_dir,
            &sessions_dir,
            "sub-live",
            &survivor_file,
            &survivor_child,
        );
        append_family_edge(&agent_dir, &sessions_dir, "sub-9", &root_file, &child_file);
        register_root_worker(&supervisor, "w-root", &root_file).await;
        // The stopping root owns its own top-level row, like the production stop.
        let mut root_summary = live_child_summary(&root_file, &child_file);
        root_summary["runtimeKind"] = json!("top-level");
        root_summary["sessionId"] = json!("root-persisted");
        root_summary["id"] = json!("root-persisted");
        root_summary["sessionFile"] = json!(root_file.to_string_lossy());
        root_summary.as_object_mut().unwrap().remove("rlmChildId");
        root_summary
            .as_object_mut()
            .unwrap()
            .remove("parentSessionPath");
        supervisor.write_roster_summary(&root_summary, Some("w-root"));
        // The boot seed publishes both families' seeded rows.
        supervisor.spawn_roster_boot_seed();
        drain_pending_seeds_for_tests(&supervisor).await;
        let mut events = supervisor.events.subscribe();
        let _ = drain_roster_pushes(&mut events);
        let _ = roster_row_for_child(&supervisor, "sub-9");
        let _ = roster_row_for_child(&supervisor, "sub-live");

        // The stop: the caller removed the resident from the registry first.
        supervisor.registry.remove("w-root").await;
        supervisor.passivate_roster_worker("w-root", false).await;

        let pushes = drain_roster_pushes(&mut events);
        let removed: Vec<String> = pushes
            .iter()
            .flat_map(|push| {
                push["removed"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|id| id.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(
            removed.iter().any(|id| id.ends_with("#sub-9")),
            "the orphaned family's seeded row leaves the roster (and the subscribers): {removed:?}"
        );
        let roster = supervisor.roster.lock().unwrap();
        assert!(
            !roster.entries().iter().any(|entry| entry
                .summary
                .get("rlmChildId")
                .and_then(Value::as_str)
                == Some("sub-9")),
            "the departed family's seeded row is gone"
        );
        drop(roster);
        let kept = roster_row_for_child(&supervisor, "sub-live");
        assert_eq!(
            kept.worker_id, None,
            "the anchored seeded row stays unowned"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stop_leaves_the_first_roster_snapshot_clean_behind_hundreds_of_seeded_rows() {
        let (dir, supervisor, root_file, _child_file) = roster_fixture().await;
        let agent_dir = dir.join("agent");
        let sessions_dir = agent_dir.join("sessions");
        register_root_worker(&supervisor, "w-root", &root_file).await;
        for index in 0..300 {
            let seeded_child = sessions_dir.join(format!("sub-flash-{index}.jsonl"));
            write_display_file(&seeded_child, "/the/flash/cwd");
            append_family_edge(
                &agent_dir,
                &sessions_dir,
                &format!("sub-flash-{index}"),
                &root_file,
                &seeded_child,
            );
        }
        supervisor.spawn_roster_boot_seed();
        drain_pending_seeds_for_tests(&supervisor).await;
        let before = supervisor
            .handle_roster_subscribe("s1", "roster_subscribe")
            .await;
        let roster = before.data.expect("roster snapshot")["roster"].clone();
        let family_rows = roster
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter(|entry| {
                        entry["summary"]["rlmChildId"]
                            .as_str()
                            .is_some_and(|id| id.starts_with("sub-flash-"))
                    })
                    .count()
            })
            .unwrap_or_default();
        assert_eq!(
            family_rows, 300,
            "a resident root's seeded family serves to subscribers (TS parity)"
        );

        supervisor.registry.remove("w-root").await;
        supervisor.passivate_roster_worker("w-root", false).await;

        let after = supervisor
            .handle_roster_subscribe("s2", "roster_subscribe")
            .await;
        let roster = after.data.expect("roster snapshot")["roster"].clone();
        let family_rows = roster
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter(|entry| {
                        entry["summary"]["rlmChildId"]
                            .as_str()
                            .is_some_and(|id| id.starts_with("sub-flash-"))
                    })
                    .count()
            })
            .unwrap_or_default();
        assert_eq!(
            family_rows, 0,
            "the first roster snapshot behind a departed family is clean: {roster:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn passivation_keeps_registration_marks_and_strips_live_fields() {
        let passivated = passivated_summary(json!({
            "sessionId": "persisted-id",
            "activeSessionId": "a-child",
            "activity": "working",
            "isSessionActive": true,
            "isStreaming": true,
            "isCompacting": true,
            "attachedClients": 2,
            "directAttachedClients": 2,
            "hasActiveHeartbeat": true,
            "hasRegisteredHeartbeat": true,
            "hasRegisteredCronJob": false,
            "hasRunningRlmChildren": true,
            "isBashRunning": true,
            "isRunningTools": true,
            "workerState": "ready",
            "workerPid": 4242,
            "cwd": "/the/live/cwd",
            "model": { "provider": "live", "modelId": "lm" },
            "thinkingLevel": "low",
        }));
        assert_eq!(passivated["id"], "persisted-id");
        assert_eq!(passivated["activity"], "idle");
        assert_eq!(passivated["isSessionActive"], false);
        assert_eq!(passivated["isStreaming"], false);
        assert_eq!(passivated["isCompacting"], false);
        assert_eq!(passivated["attachedClients"], 0);
        for key in [
            "activeSessionId",
            "directAttachedClients",
            "hasActiveHeartbeat",
            "hasRegisteredCronJob",
            "hasRunningRlmChildren",
            "isBashRunning",
            "isRunningTools",
            "workerState",
            "workerPid",
        ] {
            assert!(passivated.get(key).is_none(), "{key} is live-only");
        }
        assert_eq!(
            passivated["hasRegisteredHeartbeat"], true,
            "the mark survives"
        );
        assert_eq!(passivated["cwd"], "/the/live/cwd");
        assert_eq!(
            passivated["model"],
            json!({ "provider": "live", "modelId": "lm" })
        );
        assert_eq!(passivated["thinkingLevel"], "low");
    }

    #[tokio::test]
    async fn a_reregistration_replaces_the_passive_row() {
        let (dir, supervisor, root_file, child_file) = roster_fixture().await;
        register_root_worker(&supervisor, "w-root", &root_file).await;
        let agent_dir = dir.join("agent");
        append_family_edge(
            &agent_dir,
            &agent_dir.join("sessions"),
            "sub-9",
            &root_file,
            &child_file,
        );
        let mut events = supervisor.events.subscribe();
        supervisor.write_roster_summary(
            &live_child_summary(&root_file, &child_file),
            Some("w-child"),
        );
        let _ = drain_roster_pushes(&mut events);
        supervisor.passivate_roster_worker("w-child", false).await;
        let _ = drain_roster_pushes(&mut events);

        let mut resumed = live_child_summary(&root_file, &child_file);
        resumed["model"] = json!({ "provider": "resumed", "modelId": "rm" });
        resumed["activity"] = json!("idle");
        resumed["isStreaming"] = json!(false);
        resumed["isSessionActive"] = json!(false);
        supervisor.write_roster_summary(&resumed, Some("w-resumed"));
        let row = roster_row_for_child(&supervisor, "sub-9");
        assert_eq!(row.worker_id.as_deref(), Some("w-resumed"));
        assert_eq!(row.summary["model"]["provider"], "resumed");
        assert_eq!(row.status, AgentRosterStatus::Idle);
        assert_eq!(
            supervisor.roster.lock().unwrap().entries().len(),
            1,
            "the passive row was replaced, not duplicated"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    async fn supervisor_with_registered_worker(dir: &Path) -> Supervisor {
        let supervisor = Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.join("daemon.sock"),
            agent_dir: dir.join("agent"),
        })
        .expect("supervisor");
        let descriptor = pa_types::daemon::DaemonWorkerDescriptor {
            version: 1,
            worker_id: "w-delta".to_string(),
            pid: 4242,
            process_start_id: None,
            socket_path: dir.join("worker.sock").to_string_lossy().to_string(),
            recovery_journal_path: dir.join("recovery.jsonl").to_string_lossy().to_string(),
            orphan_process_journal_path: None,
            supervisor_socket_path: dir.join("daemon.sock").to_string_lossy().to_string(),
            authentication_token: "delta-token".to_string(),
            worker_instance_id: None,
            root_active_session_id: "a-delta".to_string(),
            owner_client_id: None,
            root_session_id: None,
            session_file: Some(dir.join("session.jsonl").to_string_lossy().to_string()),
            session_dir: Some(dir.to_string_lossy().to_string()),
            telemetry_disabled: Some(true),
            created_at: "t".to_string(),
            updated_at: "t".to_string(),
            lifecycle: pa_types::daemon::DaemonWorkerLifecycle::Ready,
            create_command: pa_types::daemon::DurableDaemonCreateCommand {
                session_path: None,
                no_session: None,
                rest: Map::default(),
            },
            consecutive_failures: 0,
            stop_requested_at: None,
            archive_on_stop: None,
            last_failure_at: None,
            last_error: None,
            rest: Map::default(),
        };
        supervisor
            .registry
            .insert(ResidentWorker::new(
                "w-delta".to_string(),
                descriptor,
                dir.join("descriptor.json"),
            ))
            .await;
        supervisor
    }

    /// The session summary in the wire shape `push_roster_delta` sends:
    /// busy carries `activity: "working"` / `isStreaming: true`, idle settles both back.
    fn flip_summary(dir: &Path, busy: bool) -> Value {
        json!({
            "id": "a-delta",
            "lifecycle": "active",
            "activity": if busy { "working" } else { "idle" },
            "isSessionActive": busy,
            "isStreaming": busy,
            "isCompacting": false,
            "activeSessionId": "a-delta",
            "sessionId": "s-delta",
            "sessionFile": dir.join("session.jsonl").to_string_lossy(),
            "sessionName": "bench",
            "cwd": dir.to_string_lossy(),
            "rlmDepth": 0,
            "runtimeKind": "top-level",
            "messageCount": 12,
            "attachedClients": 0,
            "thinkingLevel": "default",
            "lastActivityAt": "2026-09-23T00:00:00.000Z",
            "created": "2026-09-23T00:00:00.000Z",
            "modified": "2026-09-23T00:00:00.000Z",
            "workerState": "ready",
            "workerPid": 4242,
        })
    }

    /// Drain the pushed roster frames; other channel events are skipped.
    fn drain_roster_pushes(
        events: &mut tokio::sync::broadcast::Receiver<(ClientRouting, std::sync::Arc<Value>)>,
    ) -> Vec<Value> {
        let mut pushes = Vec::new();
        loop {
            match events.try_recv() {
                Ok((ClientRouting::RosterSubscribers, payload)) => pushes.push((*payload).clone()),
                Ok(_) => {}
                Err(
                    tokio::sync::broadcast::error::TryRecvError::Empty
                    | tokio::sync::broadcast::error::TryRecvError::Closed,
                ) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(missed)) => {
                    panic!("roster push subscriber lagged by {missed}; drain per delta");
                }
            }
        }
        pushes
    }

    /// The stale-delta gate: per-request supervisor links deliver deltas
    /// unordered, so a delayed older snapshot must not overwrite a newer
    /// one (deliberate divergence from TS's one ordered socket).
    #[tokio::test]
    async fn worker_roster_delta_drops_stale_sequences() {
        fn summary(level: &str) -> Value {
            serde_json::json!({
                "sessionId": "s1",
                "activeSessionId": "s1",
                "activity": "idle",
                "thinkingLevel": level,
            })
        }
        async fn delta(
            supervisor: &Arc<Supervisor>,
            token: &str,
            level: &str,
            sequence: Option<u64>,
            instance: &str,
        ) -> DaemonResponse {
            supervisor
                .handle_worker_roster_delta(
                    "d",
                    "worker_roster_delta",
                    WorkerRosterDelta {
                        worker_token: token.to_string(),
                        summary: summary(level),
                        removed: Vec::new(),
                        sequence,
                        worker_instance_id: Some(instance.to_string()),
                    },
                )
                .await
        }

        let dir = std::env::temp_dir().join(format!("pa-roster-seq-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let supervisor = Arc::new(
            Supervisor::new(crate::supervisor::SupervisorOptions {
                socket_path: dir.join("supervisor.sock"),
                agent_dir: dir.join("agent"),
            })
            .expect("supervisor"),
        );
        let descriptor: pa_types::daemon::DaemonWorkerDescriptor =
            serde_json::from_value(serde_json::json!({
                "version": 2,
                "workerId": "seq-worker",
                "pid": 0,
                "socketPath": "/tmp/none.sock",
                "recoveryJournalPath": "/tmp/none.jsonl",
                "supervisorSocketPath": "/tmp/none.sock",
                "authenticationToken": "seq-token",
                "workerInstanceId": "i1",
                "rootActiveSessionId": "s1",
                "createdAt": "2026-09-23T00:00:00Z",
                "updatedAt": "2026-09-23T00:00:00Z",
                "lifecycle": "ready",
                "createCommand": {},
                "consecutiveFailures": 0,
            }))
            .expect("descriptor");
        supervisor
            .registry
            .insert(ResidentWorker::new(
                "seq-worker".to_string(),
                descriptor,
                dir.join("descriptor.json"),
            ))
            .await;
        let entry_level = || {
            supervisor
                .roster
                .lock()
                .unwrap()
                .get("s1")
                .map(|entry| entry.summary["thinkingLevel"].clone())
                .expect("the roster entry")
        };
        // A fresh worker's pull stamps the ZERO counter: it starts the
        // slot and applies — the first authoritative write.
        let resident = supervisor
            .registry
            .get("seq-worker")
            .await
            .expect("resident");
        let mut fresh_pull = summary("off");
        fresh_pull["rosterDeltaSequence"] = serde_json::json!(0);
        fresh_pull["workerInstanceId"] = serde_json::json!("i1");
        let fresh = supervisor
            .write_roster_summary_for_resident(&resident, &fresh_pull)
            .await;
        assert!(
            fresh.is_some(),
            "a stamped-zero pull starts the slot: {fresh:?}"
        );
        assert_eq!(entry_level(), serde_json::json!("off"));
        let applied = delta(&supervisor, "seq-token", "high", Some(2), "i1").await;
        assert!(applied.success, "sequence 2 applies: {applied:?}");
        assert_eq!(entry_level(), serde_json::json!("high"));
        let stale = delta(&supervisor, "seq-token", "low", Some(1), "i1").await;
        assert!(
            stale.success,
            "a stale delta still answers success: {stale:?}"
        );
        assert_eq!(
            entry_level(),
            serde_json::json!("high"),
            "the stale snapshot never overwrites the newer one"
        );
        let applied = delta(&supervisor, "seq-token", "low", Some(3), "i1").await;
        assert!(applied.success, "sequence 3 applies: {applied:?}");
        assert_eq!(entry_level(), serde_json::json!("low"));
        // The authoritative pull gates in the same lock section that
        // writes: the embedded counter raises the watermark.
        let mut pulled = summary("off");
        pulled["rosterDeltaSequence"] = serde_json::json!(4);
        pulled["workerInstanceId"] = serde_json::json!("i1");
        let pull = supervisor
            .write_roster_summary_for_resident(&resident, &pulled)
            .await;
        assert_eq!(
            pull.expect("pull entry").summary["thinkingLevel"],
            serde_json::json!("off")
        );
        assert_eq!(entry_level(), serde_json::json!("off"));
        let stale = delta(&supervisor, "seq-token", "high", Some(4), "i1").await;
        assert!(
            stale.success,
            "the in-flight delta answers success: {stale:?}"
        );
        assert_eq!(
            entry_level(),
            serde_json::json!("off"),
            "a delta older than the pull never overwrites the pull"
        );
        let mut stale_pull = summary("medium");
        stale_pull["rosterDeltaSequence"] = serde_json::json!(3);
        stale_pull["workerInstanceId"] = serde_json::json!("i1");
        assert!(
            supervisor
                .write_roster_summary_for_resident(&resident, &stale_pull)
                .await
                .is_none(),
            "an older in-flight refresh drops"
        );
        assert_eq!(entry_level(), serde_json::json!("off"));
        // A delayed ZERO-counter pull is sequenced like any other.
        let mut zero_pull = summary("high");
        zero_pull["rosterDeltaSequence"] = serde_json::json!(0);
        zero_pull["workerInstanceId"] = serde_json::json!("i1");
        assert!(
            supervisor
                .write_roster_summary_for_resident(&resident, &zero_pull)
                .await
                .is_none(),
            "a delayed pre-push pull never overwrites a newer delta"
        );
        assert_eq!(entry_level(), serde_json::json!("off"));
        // A replacement registers a new generation; its restarted
        // counter never compares against the predecessor's watermark.
        supervisor
            .roster
            .lock()
            .unwrap()
            .note_worker_generation("seq-worker", "i2");
        let replacement = delta(&supervisor, "seq-token", "medium", Some(1), "i2").await;
        assert!(
            replacement.success,
            "the replacement applies: {replacement:?}"
        );
        assert_eq!(entry_level(), serde_json::json!("medium"));
        // The predecessor's delayed frames drop on the generation mismatch.
        let predecessor = delta(&supervisor, "seq-token", "high", Some(9_000_000), "i1").await;
        assert!(
            predecessor.success,
            "a superseded frame still answers success: {predecessor:?}"
        );
        assert_eq!(entry_level(), serde_json::json!("medium"));
        let mut predecessor_pull = summary("low");
        predecessor_pull["rosterDeltaSequence"] = serde_json::json!(9_000_000);
        predecessor_pull["workerInstanceId"] = serde_json::json!("i1");
        assert!(
            supervisor
                .write_roster_summary_for_resident(&resident, &predecessor_pull)
                .await
                .is_none(),
            "a superseded pull drops"
        );
        assert_eq!(entry_level(), serde_json::json!("medium"));
        // The predecessor's DELAYED zero-counter pull drops the same way.
        let mut predecessor_zero_pull = summary("low");
        predecessor_zero_pull["rosterDeltaSequence"] = serde_json::json!(0);
        predecessor_zero_pull["workerInstanceId"] = serde_json::json!("i1");
        assert!(
            supervisor
                .write_roster_summary_for_resident(&resident, &predecessor_zero_pull)
                .await
                .is_none(),
            "a superseded zero-counter pull drops"
        );
        assert_eq!(entry_level(), serde_json::json!("medium"));
        let unsequenced = delta(&supervisor, "seq-token", "low", None, "i2").await;
        assert!(unsequenced.success, "unsequenced applies: {unsequenced:?}");
        assert_eq!(entry_level(), serde_json::json!("low"));
        // A wrong token still fails authentication, before the gate.
        let rejected = delta(&supervisor, "wrong-token", "high", Some(9), "i2").await;
        assert!(
            !rejected.success,
            "authentication still gates: {rejected:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Wrap one delta's fields in the handler's frame shape (unsequenced:
    /// these tests exercise the batching cadence, not the stale-delta gate).
    fn delta_frame(worker_token: &str, summary: Value, removed: Vec<String>) -> WorkerRosterDelta {
        WorkerRosterDelta {
            worker_token: worker_token.to_string(),
            summary,
            removed,
            sequence: None,
            worker_instance_id: None,
        }
    }

    /// TS parity (`applyWorkerRosterDelta` + `scheduleRosterPush`): the
    /// entry write and removals batch into one coalesced flush.
    #[tokio::test]
    async fn worker_roster_delta_pushes_one_update_per_flip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let supervisor = Arc::new(supervisor_with_registered_worker(dir.path()).await);
        let mut events = supervisor.events.subscribe();

        supervisor
            .handle_worker_roster_delta(
                "d1",
                "worker_roster_delta",
                delta_frame("delta-token", flip_summary(dir.path(), true), Vec::new()),
            )
            .await;
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(pushes.len(), 1, "one roster_update per delta: {pushes:?}");
        assert_eq!(pushes[0]["changed"][0]["status"], "running");
        assert_eq!(
            pushes[0]["changed"][0]["summary"]["activeSessionId"],
            "a-delta"
        );

        supervisor
            .handle_worker_roster_delta(
                "d2",
                "worker_roster_delta",
                delta_frame("delta-token", flip_summary(dir.path(), false), Vec::new()),
            )
            .await;
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(pushes.len(), 1, "one roster_update per delta: {pushes:?}");
        assert_eq!(pushes[0]["changed"][0]["status"], "idle");
    }

    #[tokio::test]
    async fn worker_roster_delta_batches_removals_into_the_same_push() {
        let dir = tempfile::tempdir().expect("temp dir");
        let supervisor = Arc::new(supervisor_with_registered_worker(dir.path()).await);
        let mut events = supervisor.events.subscribe();

        // A child agent the delta will remove, keyed parent session path + child id.
        let child_summary = json!({
            "activity": "idle",
            "isSessionActive": false,
            "activeSessionId": "a-child",
            "sessionId": "s-child",
            "rlmChildId": "c-1",
            "parentSessionPath": dir
                .path()
                .join("session.jsonl")
                .to_string_lossy(),
            "runtimeKind": "subagent",
            "rlmDepth": 1,
        });
        supervisor
            .handle_worker_roster_delta(
                "d1",
                "worker_roster_delta",
                delta_frame("delta-token", child_summary, Vec::new()),
            )
            .await;
        let child_pushes = drain_roster_pushes(&mut events);
        assert_eq!(
            child_pushes.len(),
            1,
            "one roster_update per delta: {child_pushes:?}"
        );
        let child_agent_id = child_pushes[0]["changed"][0]["agentId"]
            .as_str()
            .expect("child agent id")
            .to_string();

        supervisor
            .handle_worker_roster_delta(
                "d2",
                "worker_roster_delta",
                delta_frame(
                    "delta-token",
                    flip_summary(dir.path(), true),
                    vec![child_agent_id.clone()],
                ),
            )
            .await;
        let pushes = drain_roster_pushes(&mut events);
        assert_eq!(
            pushes.len(),
            1,
            "removals batch into the delta push: {pushes:?}"
        );
        assert_eq!(pushes[0]["changed"].as_array().map(Vec::len), Some(1));
        assert_eq!(pushes[0]["removed"][0], json!(child_agent_id));
    }

    /// The busy/idle flip cadence benchmark: alternating deltas against a
    /// subscribed supervisor, counting pushes and serialized payloads; run
    /// with `cargo test -p pa-daemon roster_delta_push_benchmark -- --ignored --nocapture`.
    #[ignore = "manual roster delta push benchmark"]
    #[tokio::test]
    async fn roster_delta_push_benchmark() {
        const FLIPS: usize = 2000;
        const WARMUP_FLIPS: usize = 50;
        let dir = tempfile::tempdir().expect("temp dir");
        let supervisor = Arc::new(supervisor_with_registered_worker(dir.path()).await);
        let mut events = supervisor.events.subscribe();

        // Warm-up flips keep allocator noise out of the timed window.
        for i in 0..WARMUP_FLIPS {
            let summary = flip_summary(dir.path(), i % 2 == 0);
            supervisor
                .handle_worker_roster_delta(
                    "warm",
                    "worker_roster_delta",
                    delta_frame("delta-token", summary.clone(), Vec::new()),
                )
                .await;
            drain_roster_pushes(&mut events);
        }

        let mut pushes = 0usize;
        let mut payload_bytes = 0usize;
        let mut handler_nanos = 0u128;
        for i in 0..FLIPS {
            let summary = flip_summary(dir.path(), i % 2 == 0);
            let start = std::time::Instant::now();
            supervisor
                .handle_worker_roster_delta(
                    "b",
                    "worker_roster_delta",
                    delta_frame("delta-token", summary.clone(), Vec::new()),
                )
                .await;
            handler_nanos += start.elapsed().as_nanos();
            for push in drain_roster_pushes(&mut events) {
                pushes += 1;
                payload_bytes += serde_json::to_string(&push).map_or(0, |payload| payload.len());
            }
        }
        let flips = FLIPS as f64;
        println!("flips: {FLIPS}");
        println!(
            "roster_update pushes: {pushes} ({:.3}/flip)",
            pushes as f64 / flips
        );
        println!(
            "supervisor-side serializations: {} ({:.3}/flip; two per push)",
            2 * pushes,
            2.0 * pushes as f64 / flips
        );
        println!(
            "pushed payload bytes: {payload_bytes} ({:.0}/flip)",
            payload_bytes as f64 / flips
        );
        println!(
            "handler wall time: {:.2} us/flip",
            handler_nanos as f64 / flips / 1000.0
        );
    }
}
