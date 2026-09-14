//! Target-side bootstrap for durable managed-agent transfer delivery.
//!
//! The relay queues an accepted owner-start event while a target ACP process is
//! offline. Desktop listens as the target agent identity, validates the
//! secret-free event, and starts the existing runtime pair with the event in a
//! reserved environment variable. ACP then feeds it through its normal
//! transfer validation path. This module owns no transfer state transitions.

use std::{collections::HashMap, sync::atomic::Ordering, time::Duration};

use buzz_core_pkg::agent_transfer::{
    TransferCoordinatorEnvelope, TransferCoordinatorMessage, TransferOwnerRequest,
    TransferPhase, TRANSFER_COORDINATOR_EVENT_KIND,
};
use buzz_ws_client_pkg::{NostrWsConnection, RelayMessage};
use nostr::{Keys, Event};
use tauri::{AppHandle, Manager};
use tokio_util::sync::CancellationToken;

use crate::{
    app_state::AppState,
    managed_agents::{
        current_instance_id, load_managed_agents, start_managed_agent_runtime_pair_lazy,
        BackendKind, ManagedAgentRecord, ManagedAgentRuntimeKey,
    },
    relay::relay_ws_url_with_override,
};

const SUBSCRIPTION_ID_PREFIX: &str = "managed-agent-transfer-bootstrap";
const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// Start the supervisor once after the active workspace has been installed.
pub(crate) fn start(app: &AppHandle) {
    let state = app.state::<AppState>();
    if state
        .transfer_bootstrap_supervisor_started
        .swap(true, Ordering::AcqRel)
    {
        return;
    }
    let app = app.clone();
    tauri::async_runtime::spawn(async move { reconcile_loop(app).await });
}

async fn reconcile_loop(app: AppHandle) {
    loop {
        let state = app.state::<AppState>();
        if state.shutdown_started.load(Ordering::Acquire) {
            cancel_all_workers(&state);
            return;
        }

        let relay_url = relay_ws_url_with_override(&state);
        let records = match load_managed_agents(&app) {
            Ok(records) => records,
            Err(error) => {
                eprintln!("buzz-desktop: transfer bootstrap record load failed: {error}");
                tokio::time::sleep(RECONCILE_INTERVAL).await;
                continue;
            }
        };

        let mut desired = HashMap::new();
        for record in records
            .into_iter()
            .filter(|record| record.backend == BackendKind::Local)
        {
            if record.private_key_nsec.trim().is_empty() {
                continue;
            }
            desired.insert(
                record.pubkey.clone(),
                (format!("{relay_url}|{}", record.updated_at), relay_url.clone()),
            );
        }

        match state.transfer_bootstrap_workers.lock() {
            Ok(mut workers) => {
                let stale: Vec<String> = workers
                    .iter()
                    .filter_map(|(pubkey, (fingerprint, _))| {
                        desired
                            .get(pubkey)
                            .filter(|(next_fingerprint, _)| next_fingerprint == fingerprint)
                            .is_none()
                            .then_some(pubkey.clone())
                    })
                    .collect();
                for pubkey in stale {
                    if let Some((_, token)) = workers.remove(&pubkey) {
                        token.cancel();
                    }
                }

                for (pubkey, (fingerprint, worker_relay_url)) in desired {
                    if workers.contains_key(&pubkey) {
                        continue;
                    }
                    let cancel = CancellationToken::new();
                    workers.insert(pubkey.clone(), (fingerprint, cancel.clone()));
                    let worker_app = app.clone();
                    tauri::async_runtime::spawn(async move {
                        worker_loop(worker_app, pubkey, worker_relay_url, cancel).await;
                    });
                }
            }
            Err(error) => {
                eprintln!("buzz-desktop: transfer bootstrap worker lock failed: {error}");
            }
        }

        tokio::time::sleep(RECONCILE_INTERVAL).await;
    }
}

fn cancel_all_workers(state: &AppState) {
    if let Ok(mut workers) = state.transfer_bootstrap_workers.lock() {
        for (_, (_, token)) in workers.drain() {
            token.cancel();
        }
    }
}

async fn worker_loop(app: AppHandle, pubkey: String, relay_url: String, cancel: CancellationToken) {
    let subscription_id = format!("{SUBSCRIPTION_ID_PREFIX}-{pubkey}");
    let mut retry_delay = RETRY_BASE;

    loop {
        if cancel.is_cancelled() {
            return;
        }
        let record = match load_managed_agents(&app)
            .ok()
            .and_then(|records| records.into_iter().find(|record| record.pubkey == pubkey))
        {
            Some(record) => record,
            None => return,
        };
        let keys = match Keys::parse(record.private_key_nsec.trim()) {
            Ok(keys) => keys,
            Err(error) => {
                eprintln!("buzz-desktop: transfer bootstrap key for {pubkey} is invalid: {error}");
                wait_or_cancel(&cancel, retry_delay).await;
                retry_delay = (retry_delay * 2).min(RETRY_MAX);
                continue;
            }
        };
        if keys.public_key().to_hex() != pubkey {
            eprintln!("buzz-desktop: transfer bootstrap key does not match agent {pubkey}");
            wait_or_cancel(&cancel, retry_delay).await;
            retry_delay = (retry_delay * 2).min(RETRY_MAX);
            continue;
        }
        let auth_tag = record
            .auth_tag
            .as_deref()
            .and_then(|raw| buzz_sdk_pkg::nip_oa::parse_auth_tag(raw).ok());

        match NostrWsConnection::connect_authenticated(&relay_url, &keys, auth_tag.as_ref()).await {
            Ok(mut connection) => {
                retry_delay = RETRY_BASE;
                let filter = serde_json::json!({
                    "kinds": [TRANSFER_COORDINATOR_EVENT_KIND],
                    "#agent": [pubkey],
                });
                if connection
                    .send_raw(&serde_json::json!(["REQ", subscription_id, filter]))
                    .await
                    .is_err()
                {
                    let _ = connection.disconnect().await;
                    wait_or_cancel(&cancel, retry_delay).await;
                    continue;
                }

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            let _ = connection.disconnect().await;
                            return;
                        }
                        message = connection.next_event(READ_TIMEOUT) => {
                            match message {
                                // The relay's durable delivery worker sends a
                                // queued coordinator event on its reserved
                                // delivery subscription id, not necessarily
                                // the id used by this local REQ. This socket
                                // has only the transfer filter, and the
                                // validator below re-checks the target and
                                // signature, so accept any event frame here.
                                Ok(RelayMessage::Event { event, .. }) => {
                                        if let Err(error) = handle_event(&app, &record, &relay_url, *event).await {
                                            eprintln!("buzz-desktop: transfer bootstrap event ignored: {error}");
                                        }
                                }
                                Ok(RelayMessage::Closed { subscription_id: id, .. }) if id == subscription_id => break,
                                Ok(_) => {}
                                Err(_) => break,
                            }
                        }
                    }
                }
                let _ = connection.disconnect().await;
            }
            Err(error) => {
                eprintln!("buzz-desktop: transfer bootstrap relay connection failed: {error}");
            }
        }
        wait_or_cancel(&cancel, retry_delay).await;
        retry_delay = (retry_delay * 2).min(RETRY_MAX);
    }
}

async fn wait_or_cancel(cancel: &CancellationToken, delay: Duration) {
    tokio::select! {
        _ = cancel.cancelled() => {}
        _ = tokio::time::sleep(delay) => {}
    }
}

async fn handle_event(
    app: &AppHandle,
    record: &ManagedAgentRecord,
    relay_url: &str,
    event: Event,
) -> Result<(), String> {
    validate_owner_start_event(&event, &record.pubkey, &current_instance_id(app))?;
    let key = ManagedAgentRuntimeKey::new(record.pubkey.clone(), relay_url)?;
    let raw = serde_json::to_string(&event).map_err(|error| error.to_string())?;
    {
        let state = app.state::<AppState>();
        let mut pending = state
            .transfer_bootstrap_events
            .lock()
            .map_err(|error| error.to_string())?;
        pending.entry(key).or_insert(raw);
    }

    let app = app.clone();
    let pubkey = record.pubkey.clone();
    let relay_url = relay_url.to_string();
    tokio::task::spawn_blocking(move || {
        start_managed_agent_runtime_pair_lazy(pubkey, relay_url, app)
            .map(|_| ())
            .map_err(|error| format!("target runtime start failed: {error}"))
    })
    .await
    .map_err(|error| format!("target runtime start task failed: {error}"))??;
    Ok(())
}

fn validate_owner_start_event(
    event: &Event,
    agent_pubkey: &str,
    local_instance_id: &str,
) -> Result<(), String> {
    if event.kind.as_u16() as u32 != TRANSFER_COORDINATOR_EVENT_KIND {
        return Err("event kind is not the transfer coordinator kind".into());
    }
    buzz_core_pkg::verify_event(event)
        .map_err(|error| format!("signature verification failed: {error}"))?;

    let mut owner = None;
    let mut tagged_agent = None;
    for tag in event.tags.iter() {
        let values = tag.as_slice();
        match values.first().map(String::as_str) {
            Some("p") if values.len() == 2 && owner.is_none() => owner = Some(values[1].clone()),
            Some("agent") if values.len() == 2 && tagged_agent.is_none() => {
                tagged_agent = Some(values[1].clone())
            }
            _ => return Err("routing tags are not the canonical p/agent pair".into()),
        }
    }
    if event.tags.len() != 2 || owner.is_none() || tagged_agent.as_deref() != Some(agent_pubkey) {
        return Err("event is not addressed to this agent".into());
    }
    if event.pubkey.to_hex() != owner.as_deref().unwrap_or_default() {
        return Err("owner routing tag does not match event signer".into());
    }

    let envelope = TransferCoordinatorEnvelope::from_json(&event.content)
        .map_err(|error| format!("transfer envelope is invalid: {error}"))?;
    let TransferCoordinatorMessage::OwnerRequest {
        request: TransferOwnerRequest::Start { transfer },
    } = envelope.message
    else {
        return Err("event is not an owner-start request".into());
    };
    transfer
        .validate()
        .map_err(|error| format!("transfer record is invalid: {error}"))?;
    if transfer.agent_pubkey != agent_pubkey {
        return Err("transfer record targets another agent".into());
    }
    if transfer.target.instance_id != local_instance_id {
        return Err("transfer target is another Desktop instance".into());
    }
    if transfer.phase != TransferPhase::Preparing {
        return Err("owner-start transfer is not in preparing phase".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core_pkg::agent_transfer::{
        Executor, TransferCoordinatorEnvelope, TransferCoordinatorMessage, TransferOwnerRequest,
        TransferRecord,
    };
    use nostr::{EventBuilder, Tag};

    fn owner_start(agent: &Keys, owner: &Keys, target: &str) -> Event {
        let record = TransferRecord::new(
            "community",
            agent.public_key().to_hex(),
            "operation",
            Executor::new("source", "server").unwrap(),
            Executor::new(target, "computer").unwrap(),
            1,
        )
        .unwrap();
        let content = TransferCoordinatorEnvelope::new(
            "message",
            TransferCoordinatorMessage::OwnerRequest {
                request: TransferOwnerRequest::Start {
                    transfer: Box::new(record),
                },
            },
        )
        .unwrap()
        .to_json()
        .unwrap();
        EventBuilder::new(
            nostr::Kind::Custom(TRANSFER_COORDINATOR_EVENT_KIND as u16),
            content,
        )
        .tags([
            Tag::parse(["p", &owner.public_key().to_hex()]).unwrap(),
            Tag::parse(["agent", &agent.public_key().to_hex()]).unwrap(),
        ])
        .allow_self_tagging()
        .sign_with_keys(owner)
        .unwrap()
    }

    #[test]
    fn validates_only_preparing_transfer_for_this_instance() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let event = owner_start(&agent, &owner, "target");
        assert!(validate_owner_start_event(&event, &agent.public_key().to_hex(), "target").is_ok());
        assert!(validate_owner_start_event(&event, &agent.public_key().to_hex(), "other").is_err());
    }

    #[test]
    fn rejects_non_owner_start_commands() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let mut event = owner_start(&agent, &owner, "target");
        event.content = "{}".into();
        assert!(validate_owner_start_event(&event, &agent.public_key().to_hex(), "target").is_err());
    }
}
