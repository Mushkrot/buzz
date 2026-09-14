//! Retry worker for signed managed-agent transfer coordinator delivery.
//!
//! The worker only transports an already-authenticated event to a live socket.
//! It never changes transfer state and never starts or stops a process. That
//! separation keeps relay durability and runtime supervision independently
//! testable and makes duplicate delivery safe at the event-id boundary.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{error, info, warn};
use uuid::Uuid;

use buzz_db::managed_agent_transfer_deliveries::ManagedAgentTransferDelivery;

use crate::agent_transfer::COORDINATOR_DELIVERY_SUB_ID;
use crate::state::AppState;

const LEASE_SECS: i64 = 30;
const BATCH_SIZE: i64 = 16;

/// Run the retry worker. Intended to be started once per relay process.
pub async fn run(state: Arc<AppState>) {
    let worker_id = format!("agent-transfer-delivery-{}", Uuid::new_v4());
    info!(worker_id = %worker_id, "Managed-agent transfer delivery worker started");
    let mut idle_delay = Duration::from_millis(500);

    loop {
        let lease_until = Utc::now() + chrono::Duration::seconds(LEASE_SECS);
        let batch = match state
            .db
            .claim_pending_managed_agent_transfer_deliveries(&worker_id, lease_until, BATCH_SIZE)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                error!(worker_id = %worker_id, "managed-agent transfer delivery claim failed: {error}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if batch.is_empty() {
            tokio::time::sleep(idle_delay).await;
            idle_delay = (idle_delay * 2).min(Duration::from_secs(10));
            continue;
        }
        idle_delay = Duration::from_millis(500);

        for row in batch {
            deliver_one(&state, &row).await;
        }
    }
}

async fn deliver_one(state: &Arc<AppState>, row: &ManagedAgentTransferDelivery) {
    match try_deliver(state, row) {
        Ok(()) => match state
            .db
            .mark_managed_agent_transfer_delivery_delivered(row.id, row.claim_token)
            .await
        {
            Ok(true) => info!(
                delivery_id = %row.id,
                operation_id = %row.operation_id,
                revision = row.revision,
                "managed-agent transfer coordinator delivery completed"
            ),
            Ok(false) => warn!(
                delivery_id = %row.id,
                "managed-agent transfer delivery lease was lost before completion"
            ),
            Err(error) => {
                warn!(delivery_id = %row.id, "transfer delivery completion failed: {error}")
            }
        },
        Err(error) => {
            warn!(
                delivery_id = %row.id,
                operation_id = %row.operation_id,
                attempt = row.attempt_count,
                error = %error,
                "managed-agent transfer coordinator delivery deferred"
            );
            if let Err(db_error) = state
                .db
                .fail_managed_agent_transfer_delivery(row.id, row.claim_token, &error)
                .await
            {
                error!(delivery_id = %row.id, "transfer delivery failure update failed: {db_error}");
            }
        }
    }
}

fn try_deliver(state: &Arc<AppState>, row: &ManagedAgentTransferDelivery) -> Result<(), String> {
    let event: nostr::Event = serde_json::from_value(row.event.clone())
        .map_err(|error| format!("stored event is invalid: {error}"))?;
    buzz_core::verify_event(&event)
        .map_err(|error| format!("stored event signature is invalid: {error}"))?;
    if event.id.to_hex() != row.event_id {
        return Err("stored event id does not match delivery row".into());
    }
    if event.kind.as_u16() as u32 != buzz_core::kind::KIND_AGENT_TRANSFER_COORDINATOR {
        return Err("stored event has the wrong transfer coordinator kind".into());
    }
    let agent = nostr::PublicKey::from_hex(&row.agent_pubkey)
        .map_err(|_| "delivery row has an invalid target agent key".to_owned())?;
    let sent = state
        .conn_manager
        .connection_ids_for_pubkey_in_community(row.community_id, &agent.to_bytes())
        .into_iter()
        .filter(|conn_id| {
            state.conn_manager.send_to(
                *conn_id,
                crate::protocol::RelayMessage::event(COORDINATOR_DELIVERY_SUB_ID, &event),
            )
        })
        .count();
    if sent == 0 {
        return Err("target runtime is not connected".into());
    }
    Ok(())
}
