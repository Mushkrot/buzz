//! Relay-side coordination boundary for managed-agent transfers.
//!
//! This module deliberately stops at durable coordination. It authenticates
//! owner reads/creation, validates executor identity against the fenced record,
//! and delegates state changes to `buzz-db`. It does not start processes,
//! copy keys, or treat a text response as proof that a runtime is quiescent.

use std::sync::Arc;

use buzz_core::agent_transfer::{
    TransferCoordinatorEnvelope, TransferCoordinatorMessage, TransferExecutorCommand,
    TransferOwnerRequest, TransferRecord, TransferWireError, TransferWireErrorCode,
    TransferWireMessage, TransferWireResponse, WireError,
};
use buzz_core::verification::verify_event;
use buzz_core::CommunityId;
use buzz_db::managed_agent_transfers::{
    CreateManagedAgentTransferResult, ManagedAgentTransferJournalEntry,
};
use buzz_db::{Db, DbError};
use thiserror::Error;

/// Relay coordinator errors for the managed-agent transfer seam.
#[derive(Debug, Error)]
pub enum TransferCoordinatorError {
    /// A database operation failed.
    #[error("transfer database error: {0}")]
    Database(#[from] DbError),
    /// The agent identity was not a canonical 32-byte lowercase hex pubkey.
    #[error("invalid managed-agent public key")]
    InvalidAgentPubkey,
    /// The caller is not the registered owner of the agent.
    #[error("managed-agent owner authorization required")]
    Unauthorized,
    /// The executor identity does not match the current fenced authority.
    #[error("executor does not hold the current transfer authority")]
    ExecutorMismatch,
    /// The signed event identity does not match the agent command target.
    #[error("executor event identity does not match the managed agent")]
    ExecutorIdentityMismatch,
    /// A response payload cannot be submitted as a coordinator request.
    #[error("transfer response is not a coordinator request")]
    UnsupportedMessage,
    /// The wire command failed structural validation before the database CAS.
    #[error("invalid transfer executor command: {0}")]
    InvalidCommand(#[from] WireError),
}

/// Durable relay coordinator for one or more managed-agent transfers.
#[derive(Clone, Debug)]
pub struct TransferCoordinator {
    db: Db,
}

/// Fenced command envelope reported by one executor instance.
///
/// This alias keeps the relay coordinator and the encrypted Nostr/WebSocket
/// payload on one JSON contract instead of maintaining two subtly different
/// request shapes.
pub type ExecutorCommandRequest = TransferExecutorCommand;

/// Subscription identifier used for the best-effort live delivery of a
/// coordinator request to the target runtime. The event is not persisted or
/// broadcast as a timeline event; an offline target is handled by the durable
/// coordinator state and a future outbox/recovery pass.
const COORDINATOR_DELIVERY_SUB_ID: &str = "agent-transfer-coordinator";

/// Handle one relay-readable signed transfer request.
///
/// The request is intentionally ephemeral: it is authenticated, authorized,
/// dispatched to the durable coordinator, and answered through the sender's
/// NIP-01 `OK` frame. It is never stored or fanned out as a public timeline
/// event. Only public state-machine metadata is accepted; secrets remain on
/// the encrypted observer-frame path.
pub async fn handle_coordinator_event(
    event: nostr::Event,
    event_id_hex: &str,
    conn: Arc<crate::connection::ConnectionState>,
    state: Arc<crate::state::AppState>,
) {
    let event_clone = event.clone();
    let verify_result = tokio::task::spawn_blocking(move || verify_event(&event_clone)).await;
    if !matches!(verify_result, Ok(Ok(()))) {
        let message = match verify_result {
            Ok(Err(error)) => format!("invalid: {error}"),
            _ => "error: internal error".to_owned(),
        };
        conn.send(crate::protocol::RelayMessage::ok(
            event_id_hex,
            false,
            &message,
        ));
        return;
    }

    let now = chrono::Utc::now().timestamp();
    let event_ts = event.created_at.as_secs() as i64;
    if (event_ts - now).unsigned_abs() > 300 {
        conn.send(crate::protocol::RelayMessage::ok(
            event_id_hex,
            false,
            "invalid: transfer coordinator timestamp outside ±5 minute freshness window",
        ));
        return;
    }

    let envelope = match TransferCoordinatorEnvelope::from_json(&event.content) {
        Ok(envelope) => envelope,
        Err(error) => {
            conn.send(crate::protocol::RelayMessage::ok(
                event_id_hex,
                false,
                &format!("invalid: transfer coordinator payload: {error}"),
            ));
            return;
        }
    };
    let route = match coordinator_route(&event, &envelope) {
        Ok(route) => route,
        Err(message) => {
            conn.send(crate::protocol::RelayMessage::ok(
                event_id_hex,
                false,
                &format!("invalid: {message}"),
            ));
            return;
        }
    };

    let registered_owner = state
        .db
        .is_agent_owner(
            conn.tenant.community(),
            &route.agent.to_bytes(),
            &route.owner.to_bytes(),
        )
        .await;
    match registered_owner {
        Ok(true) => {}
        Ok(false) => {
            let response = rejected_response(
                TransferWireErrorCode::Unauthorized,
                "the signed owner is not registered for this agent",
            );
            send_coordinator_response(&conn, event_id_hex, false, response);
            return;
        }
        Err(error) => {
            tracing::warn!(event_id = %event_id_hex, "transfer owner lookup failed: {error}");
            conn.send(crate::protocol::RelayMessage::ok(
                event_id_hex,
                false,
                "error: internal server error",
            ));
            return;
        }
    }

    let deliver_to_target = matches!(
        &envelope.message,
        TransferCoordinatorMessage::OwnerRequest {
            request: TransferOwnerRequest::Start { .. }
        }
    );
    let response = state
        .transfer_coordinator
        .dispatch_wire(
            conn.tenant.community(),
            &event.pubkey.to_bytes(),
            envelope.into_wire_message(),
        )
        .await;
    match response {
        Ok(response) => {
            if deliver_to_target {
                let delivered = deliver_to_target_connections(
                    &state,
                    conn.tenant.community(),
                    &route.agent,
                    &event,
                );
                tracing::info!(
                    event_id = %event_id_hex,
                    agent = %route.agent,
                    delivered,
                    "transfer coordinator request accepted; live target delivery attempted"
                );
            }
            send_coordinator_response(&conn, event_id_hex, true, response)
        }
        Err(error) => match map_coordinator_error(error) {
            Some(response) => send_coordinator_response(&conn, event_id_hex, false, response),
            None => {
                conn.send(crate::protocol::RelayMessage::ok(
                    event_id_hex,
                    false,
                    "error: internal server error",
                ));
            }
        },
    }
}

/// Deliver a coordinator start request to active target sockets.
///
/// This is intentionally a narrow adapter: the relay does not infer process
/// state and does not claim delivery when the target is offline. Durable
/// recovery for that case belongs to the outbox/supervisor seam that consumes
/// the transfer record.
fn deliver_to_target_connections(
    state: &crate::state::AppState,
    community: buzz_core::CommunityId,
    agent: &nostr::PublicKey,
    event: &nostr::Event,
) -> usize {
    let frame = crate::protocol::RelayMessage::event(COORDINATOR_DELIVERY_SUB_ID, event);
    state
        .conn_manager
        .connection_ids_for_pubkey_in_community(community, &agent.to_bytes())
        .into_iter()
        .filter(|conn_id| state.conn_manager.send_to(*conn_id, frame.clone()))
        .count()
}

#[derive(Debug, Clone, Copy)]
struct CoordinatorRoute {
    owner: nostr::PublicKey,
    agent: nostr::PublicKey,
}

fn coordinator_route(
    event: &nostr::Event,
    envelope: &TransferCoordinatorEnvelope,
) -> Result<CoordinatorRoute, String> {
    let (owner, agent) = strict_coordinator_tags(event)?;
    let content_agent = match &envelope.message {
        TransferCoordinatorMessage::OwnerRequest { request } => match request {
            TransferOwnerRequest::Start { transfer } => &transfer.agent_pubkey,
            TransferOwnerRequest::Status { agent_pubkey } => agent_pubkey,
        },
        TransferCoordinatorMessage::ExecutorCommand { command } => &command.agent_pubkey,
    };
    if content_agent != &agent.to_hex() {
        return Err("agent tag does not match the request body".into());
    }

    match &envelope.message {
        TransferCoordinatorMessage::OwnerRequest { .. } if event.pubkey != owner => {
            Err("owner request must be signed by the tagged owner".into())
        }
        TransferCoordinatorMessage::ExecutorCommand { .. } if event.pubkey != agent => {
            Err("executor command must be signed by the managed agent".into())
        }
        _ => Ok(CoordinatorRoute { owner, agent }),
    }
}

fn strict_coordinator_tags(
    event: &nostr::Event,
) -> Result<(nostr::PublicKey, nostr::PublicKey), String> {
    let mut owner = None;
    let mut agent = None;
    for tag in event.tags.iter() {
        let parts = tag.as_slice();
        if parts.len() != 2 {
            return Err("transfer coordinator tags must contain exactly two values".into());
        }
        let slot = match parts[0].as_str() {
            "p" => &mut owner,
            "agent" => &mut agent,
            _ => return Err("transfer coordinator contains an unsupported tag".into()),
        };
        if slot.is_some() {
            return Err(format!(
                "transfer coordinator has duplicate `{}` tag",
                parts[0]
            ));
        }
        let key = nostr::PublicKey::from_hex(&parts[1])
            .map_err(|_| format!("transfer coordinator `{}` tag is not a pubkey", parts[0]))?;
        if key.to_hex() != parts[1] {
            return Err(format!(
                "transfer coordinator `{}` tag must use lowercase canonical hex",
                parts[0]
            ));
        }
        *slot = Some(key);
    }
    let owner = owner.ok_or_else(|| "transfer coordinator is missing the `p` tag".to_owned())?;
    let agent =
        agent.ok_or_else(|| "transfer coordinator is missing the `agent` tag".to_owned())?;
    Ok((owner, agent))
}

fn rejected_response(code: TransferWireErrorCode, detail: &str) -> TransferWireResponse {
    TransferWireResponse::Rejected {
        error: TransferWireError {
            code,
            detail: detail.to_owned(),
        },
    }
}

fn map_coordinator_error(error: TransferCoordinatorError) -> Option<TransferWireResponse> {
    let (code, detail) = match error {
        TransferCoordinatorError::Unauthorized => (
            TransferWireErrorCode::Unauthorized,
            "owner authorization is required",
        ),
        TransferCoordinatorError::ExecutorIdentityMismatch => (
            TransferWireErrorCode::Unauthorized,
            "signed identity does not match the managed agent",
        ),
        TransferCoordinatorError::ExecutorMismatch => (
            TransferWireErrorCode::StaleState,
            "transfer state is stale; refresh status before retrying",
        ),
        TransferCoordinatorError::InvalidAgentPubkey => (
            TransferWireErrorCode::InvalidRequest,
            "managed agent public key is invalid",
        ),
        TransferCoordinatorError::UnsupportedMessage => (
            TransferWireErrorCode::InvalidRequest,
            "unsupported transfer coordinator message",
        ),
        TransferCoordinatorError::InvalidCommand(_) => (
            TransferWireErrorCode::InvalidRequest,
            "executor command is invalid",
        ),
        TransferCoordinatorError::Database(DbError::NotFound(_)) => (
            TransferWireErrorCode::Conflict,
            "transfer operation was not found",
        ),
        TransferCoordinatorError::Database(DbError::InvalidData(_)) => (
            TransferWireErrorCode::Conflict,
            "transfer request conflicts with current state",
        ),
        TransferCoordinatorError::Database(_) => return None,
    };
    Some(rejected_response(code, detail))
}

fn send_coordinator_response(
    conn: &crate::connection::ConnectionState,
    event_id_hex: &str,
    accepted: bool,
    response: TransferWireResponse,
) {
    let message = match response.to_json() {
        Ok(message) => message,
        Err(error) => {
            tracing::error!(event_id = %event_id_hex, "transfer response serialization failed: {error}");
            "error: internal server error".to_owned()
        }
    };
    conn.send(crate::protocol::RelayMessage::ok(
        event_id_hex,
        accepted,
        &message,
    ));
}

impl TransferCoordinator {
    /// Construct a coordinator over the relay's authoritative database handle.
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Start or idempotently recover a transfer requested by the agent owner.
    pub async fn start_for_owner(
        &self,
        community_id: CommunityId,
        actor_pubkey: &[u8],
        record: &TransferRecord,
    ) -> Result<CreateManagedAgentTransferResult, TransferCoordinatorError> {
        let agent_pubkey = canonical_agent_pubkey(&record.agent_pubkey)?;
        self.require_owner(community_id, &agent_pubkey, actor_pubkey)
            .await?;
        Ok(self
            .db
            .create_managed_agent_transfer(community_id, record)
            .await?)
    }

    /// Read the current transfer state after owner authorization.
    pub async fn status_for_owner(
        &self,
        community_id: CommunityId,
        actor_pubkey: &[u8],
        agent_pubkey: &str,
    ) -> Result<Option<TransferRecord>, TransferCoordinatorError> {
        let agent_pubkey = canonical_agent_pubkey(agent_pubkey)?;
        self.require_owner(community_id, &agent_pubkey, actor_pubkey)
            .await?;
        let agent_pubkey = hex::encode(agent_pubkey);
        Ok(self
            .db
            .get_managed_agent_transfer(community_id, &agent_pubkey)
            .await?)
    }

    /// Read the accepted transition journal after owner authorization.
    pub async fn journal_for_owner(
        &self,
        community_id: CommunityId,
        actor_pubkey: &[u8],
        agent_pubkey: &str,
        limit: i64,
    ) -> Result<Vec<ManagedAgentTransferJournalEntry>, TransferCoordinatorError> {
        let agent_pubkey = canonical_agent_pubkey(agent_pubkey)?;
        self.require_owner(community_id, &agent_pubkey, actor_pubkey)
            .await?;
        let agent_pubkey = hex::encode(agent_pubkey);
        Ok(self
            .db
            .list_managed_agent_transfer_journal(community_id, &agent_pubkey, limit)
            .await?)
    }

    /// Apply a command reported by the executor currently holding authority.
    ///
    /// The row-locked database transition remains the final compare-and-swap
    /// fence. The pre-read here adds the explicit instance check so a delayed
    /// runtime cannot submit a valid revision for the wrong machine.
    pub async fn apply_from_executor(
        &self,
        community_id: CommunityId,
        request: ExecutorCommandRequest,
    ) -> Result<TransferRecord, TransferCoordinatorError> {
        request.validate()?;
        let agent_pubkey = canonical_agent_pubkey(&request.agent_pubkey)?;
        let agent_pubkey_hex = hex::encode(agent_pubkey);
        let current = self
            .db
            .get_managed_agent_transfer(community_id, &agent_pubkey_hex)
            .await?
            .ok_or_else(|| DbError::NotFound("managed agent transfer".to_owned()))?;
        if current.operation_id != request.operation_id
            || current.revision != request.expected_revision
            || current.epoch != request.expected_epoch
            || current
                .active_executor
                .as_ref()
                .map(|executor| executor.instance_id.as_str())
                != Some(request.executor_instance_id.as_str())
        {
            return Err(TransferCoordinatorError::ExecutorMismatch);
        }

        Ok(self
            .db
            .apply_managed_agent_transfer(
                community_id,
                &agent_pubkey_hex,
                &request.operation_id,
                request.expected_revision,
                request.expected_epoch,
                request.command,
            )
            .await?)
    }

    /// Dispatch one authenticated wire request into the durable coordinator.
    ///
    /// The caller supplies the already authenticated Nostr event identity.
    /// Owner requests are checked through the managed-agent ownership table;
    /// executor commands must be signed by the agent identity named in the
    /// command and then pass the existing instance/revision/epoch fence.
    /// Responses are data only: this method never starts or stops a process.
    pub async fn dispatch_wire(
        &self,
        community_id: CommunityId,
        actor_pubkey: &[u8],
        message: TransferWireMessage,
    ) -> Result<TransferWireResponse, TransferCoordinatorError> {
        match message {
            TransferWireMessage::OwnerRequest { request } => match request {
                TransferOwnerRequest::Start { transfer } => {
                    let result = self
                        .start_for_owner(community_id, actor_pubkey, &transfer)
                        .await?;
                    let transfer = match result {
                        CreateManagedAgentTransferResult::Created(record)
                        | CreateManagedAgentTransferResult::AlreadyExists(record) => record,
                    };
                    Ok(TransferWireResponse::Accepted {
                        transfer: Box::new(transfer),
                    })
                }
                TransferOwnerRequest::Status { agent_pubkey } => {
                    let transfer = self
                        .status_for_owner(community_id, actor_pubkey, &agent_pubkey)
                        .await?;
                    Ok(TransferWireResponse::Status {
                        transfer: transfer.map(Box::new),
                    })
                }
            },
            TransferWireMessage::ExecutorCommand { command } => {
                require_executor_identity(actor_pubkey, &command)?;
                let transfer = self.apply_from_executor(community_id, command).await?;
                Ok(TransferWireResponse::Applied {
                    transfer: Box::new(transfer),
                })
            }
            TransferWireMessage::Response { .. } => {
                Err(TransferCoordinatorError::UnsupportedMessage)
            }
        }
    }

    async fn require_owner(
        &self,
        community_id: CommunityId,
        agent_pubkey: &[u8],
        actor_pubkey: &[u8],
    ) -> Result<(), TransferCoordinatorError> {
        if actor_pubkey.len() != 32
            || !self
                .db
                .is_agent_owner(community_id, agent_pubkey, actor_pubkey)
                .await?
        {
            return Err(TransferCoordinatorError::Unauthorized);
        }
        Ok(())
    }
}

fn canonical_agent_pubkey(value: &str) -> Result<[u8; 32], TransferCoordinatorError> {
    let bytes = hex::decode(value).map_err(|_| TransferCoordinatorError::InvalidAgentPubkey)?;
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| TransferCoordinatorError::InvalidAgentPubkey)?;
    if hex::encode(key) != value {
        return Err(TransferCoordinatorError::InvalidAgentPubkey);
    }
    Ok(key)
}

fn require_executor_identity(
    actor_pubkey: &[u8],
    command: &ExecutorCommandRequest,
) -> Result<(), TransferCoordinatorError> {
    let agent_pubkey = canonical_agent_pubkey(&command.agent_pubkey)?;
    if actor_pubkey != agent_pubkey.as_slice() {
        return Err(TransferCoordinatorError::ExecutorIdentityMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::agent_transfer::TRANSFER_COORDINATOR_EVENT_KIND;

    #[test]
    fn canonical_agent_pubkey_requires_lowercase_32_byte_hex() {
        let value = "ab".repeat(32);
        assert_eq!(canonical_agent_pubkey(&value).unwrap().len(), 32);
        assert!(canonical_agent_pubkey(&value.to_ascii_uppercase()).is_err());
        assert!(canonical_agent_pubkey("not-a-pubkey").is_err());
        assert!(canonical_agent_pubkey(&"ab".repeat(31)).is_err());
    }

    #[test]
    fn executor_command_must_be_signed_by_the_named_agent() {
        let command = ExecutorCommandRequest {
            agent_pubkey: "ab".repeat(32),
            operation_id: "operation-1".into(),
            executor_instance_id: "server-1".into(),
            expected_revision: 0,
            expected_epoch: 1,
            command: buzz_core::agent_transfer::TransferCommand::BeginDrain,
        };
        assert!(require_executor_identity(&[0xab; 32], &command).is_ok());
        assert!(matches!(
            require_executor_identity(&[0xcd; 32], &command),
            Err(TransferCoordinatorError::ExecutorIdentityMismatch)
        ));
    }

    #[test]
    fn coordinator_route_requires_matching_owner_and_agent_tags() {
        let owner = nostr::Keys::generate();
        let agent = nostr::Keys::generate();
        let envelope = TransferCoordinatorEnvelope::new(
            "message-1",
            TransferCoordinatorMessage::OwnerRequest {
                request: TransferOwnerRequest::Status {
                    agent_pubkey: agent.public_key().to_hex(),
                },
            },
        )
        .expect("valid envelope");
        let event = nostr::EventBuilder::new(
            nostr::Kind::Custom(TRANSFER_COORDINATOR_EVENT_KIND as u16),
            envelope.to_json().expect("serialize envelope"),
        )
        .tags([
            nostr::Tag::parse(["p", &owner.public_key().to_hex()]).expect("owner tag"),
            nostr::Tag::parse(["agent", &agent.public_key().to_hex()]).expect("agent tag"),
        ])
        .allow_self_tagging()
        .sign_with_keys(&owner)
        .expect("sign event");

        let route = coordinator_route(&event, &envelope).expect("valid route");
        assert_eq!(route.owner, owner.public_key());
        assert_eq!(route.agent, agent.public_key());
    }

    #[test]
    fn coordinator_route_rejects_extra_tags_and_wrong_signer() {
        let owner = nostr::Keys::generate();
        let agent = nostr::Keys::generate();
        let stranger = nostr::Keys::generate();
        let envelope = TransferCoordinatorEnvelope::new(
            "message-1",
            TransferCoordinatorMessage::OwnerRequest {
                request: TransferOwnerRequest::Status {
                    agent_pubkey: agent.public_key().to_hex(),
                },
            },
        )
        .expect("valid envelope");
        let event = nostr::EventBuilder::new(
            nostr::Kind::Custom(TRANSFER_COORDINATOR_EVENT_KIND as u16),
            envelope.to_json().expect("serialize envelope"),
        )
        .tags([
            nostr::Tag::parse(["p", &owner.public_key().to_hex()]).expect("owner tag"),
            nostr::Tag::parse(["agent", &agent.public_key().to_hex()]).expect("agent tag"),
            nostr::Tag::parse(["unexpected", "value"]).expect("extra tag"),
        ])
        .allow_self_tagging()
        .sign_with_keys(&stranger)
        .expect("sign event");

        let error = coordinator_route(&event, &envelope).expect_err("route must reject");
        assert!(error.contains("unsupported tag"));
    }
}
