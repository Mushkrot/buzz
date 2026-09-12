//! Relay-side coordination boundary for managed-agent transfers.
//!
//! This module deliberately stops at durable coordination. It authenticates
//! owner reads/creation, validates executor identity against the fenced record,
//! and delegates state changes to `buzz-db`. It does not start processes,
//! copy keys, or treat a text response as proof that a runtime is quiescent.

use buzz_core::agent_transfer::{
    TransferExecutorCommand, TransferOwnerRequest, TransferRecord, TransferWireMessage,
    TransferWireResponse, WireError,
};
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
}
