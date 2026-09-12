//! Relay-side coordination boundary for managed-agent transfers.
//!
//! This module deliberately stops at durable coordination. It authenticates
//! owner reads/creation, validates executor identity against the fenced record,
//! and delegates state changes to `buzz-db`. It does not start processes,
//! copy keys, or treat a text response as proof that a runtime is quiescent.

use buzz_core::agent_transfer::{TransferCommand, TransferRecord};
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
}

/// Durable relay coordinator for one or more managed-agent transfers.
#[derive(Clone, Debug)]
pub struct TransferCoordinator {
    db: Db,
}

/// Fenced command envelope reported by one executor instance.
#[derive(Debug, Clone)]
pub struct ExecutorCommandRequest {
    /// Agent identity being moved.
    pub agent_pubkey: String,
    /// Transfer operation id.
    pub operation_id: String,
    /// Instance claiming to hold the current authority.
    pub executor_instance_id: String,
    /// Revision observed by the executor.
    pub expected_revision: u64,
    /// Fencing epoch observed by the executor.
    pub expected_epoch: u64,
    /// State-machine command being reported.
    pub command: TransferCommand,
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
}
