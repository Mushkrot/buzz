//! Durable coordinator state for moving managed-agent identities.
//!
//! The relay owns this row and is the only writer. The serialized record is
//! validated by `buzz-core` before it is inserted or replaced; this module does
//! not store private keys, credentials, process handles, or harness sessions.
//! Row locking makes a transition one-at-a-time while the core state machine
//! rejects stale revision and epoch observations.

use buzz_core::agent_transfer::{TransferCommand, TransferRecord};
use buzz_core::CommunityId;
use buzz_datastore_tracing::datastore_span;
use sqlx::Row as _;

use crate::error::{DbError, Result};
use crate::Db;

/// Result of creating a transfer operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateManagedAgentTransferResult {
    /// A new operation was durably created.
    Created(TransferRecord),
    /// The same operation id was already present; the existing record is returned.
    AlreadyExists(TransferRecord),
}

fn validate_scope(
    community_id: CommunityId,
    agent_pubkey: &str,
    record: &TransferRecord,
) -> Result<()> {
    record.validate()?;
    if record.community_id != community_id.to_string() {
        return Err(DbError::InvalidData(
            "transfer record community does not match the scoped database request".into(),
        ));
    }
    if record.agent_pubkey != agent_pubkey {
        return Err(DbError::InvalidData(
            "transfer record agent does not match the scoped database request".into(),
        ));
    }
    Ok(())
}

fn decode_record(
    community_id: CommunityId,
    agent_pubkey: &str,
    operation_id: Option<&str>,
    value: serde_json::Value,
) -> Result<TransferRecord> {
    let record: TransferRecord = serde_json::from_value(value)?;
    validate_scope(community_id, agent_pubkey, &record)?;
    if let Some(operation_id) = operation_id {
        if record.operation_id != operation_id {
            return Err(DbError::InvalidData(
                "stored transfer operation id does not match the requested operation".into(),
            ));
        }
    }
    Ok(record)
}

impl Db {
    /// Create or idempotently recover one transfer operation for an agent.
    ///
    /// A different operation cannot replace an existing row. This deliberately
    /// keeps one durable owner record per community/agent until a later history
    /// design adds archival rows.
    #[datastore_span(name = "managed_agent_transfer_create", system = "postgresql")]
    pub async fn create_managed_agent_transfer(
        &self,
        community_id: CommunityId,
        record: &TransferRecord,
    ) -> Result<CreateManagedAgentTransferResult> {
        validate_scope(community_id, &record.agent_pubkey, record)?;
        let json = serde_json::to_value(record)?;

        let inserted = sqlx::query(
            "INSERT INTO managed_agent_transfers
                (community_id, agent_pubkey, operation_id, record)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT DO NOTHING
             RETURNING record",
        )
        .bind(community_id.as_uuid())
        .bind(&record.agent_pubkey)
        .bind(&record.operation_id)
        .bind(json)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = inserted {
            let stored: serde_json::Value = row.try_get("record")?;
            return decode_record(community_id, &record.agent_pubkey, None, stored)
                .map(CreateManagedAgentTransferResult::Created);
        }

        let existing = sqlx::query(
            "SELECT record
             FROM managed_agent_transfers
             WHERE community_id = $1 AND agent_pubkey = $2",
        )
        .bind(community_id.as_uuid())
        .bind(&record.agent_pubkey)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = existing {
            let stored: serde_json::Value = row.try_get("record")?;
            let stored = decode_record(community_id, &record.agent_pubkey, None, stored)?;
            if stored.operation_id == record.operation_id {
                return Ok(CreateManagedAgentTransferResult::AlreadyExists(stored));
            }
            return Err(DbError::InvalidData(
                "another transfer operation already owns this agent".into(),
            ));
        }

        Err(DbError::InvalidData(
            "transfer operation conflicts with another agent".into(),
        ))
    }

    /// Read the durable transfer record for one community-scoped agent.
    #[datastore_span(name = "managed_agent_transfer_get", system = "postgresql")]
    pub async fn get_managed_agent_transfer(
        &self,
        community_id: CommunityId,
        agent_pubkey: &str,
    ) -> Result<Option<TransferRecord>> {
        let row = sqlx::query(
            "SELECT record
             FROM managed_agent_transfers
             WHERE community_id = $1 AND agent_pubkey = $2",
        )
        .bind(community_id.as_uuid())
        .bind(agent_pubkey)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let stored: serde_json::Value = row.try_get("record")?;
            decode_record(community_id, agent_pubkey, None, stored)
        })
        .transpose()
    }

    /// Apply one fenced transfer command and persist it atomically.
    ///
    /// The row lock serializes competing commands. The expected revision and
    /// epoch are still checked by the core state machine, so delayed callers
    /// receive a stale-command error rather than silently overwriting a newer
    /// transition.
    #[datastore_span(name = "managed_agent_transfer_apply", system = "postgresql")]
    pub async fn apply_managed_agent_transfer(
        &self,
        community_id: CommunityId,
        agent_pubkey: &str,
        operation_id: &str,
        expected_revision: u64,
        expected_epoch: u64,
        command: TransferCommand,
    ) -> Result<TransferRecord> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT record
             FROM managed_agent_transfers
             WHERE community_id = $1 AND agent_pubkey = $2
             FOR UPDATE",
        )
        .bind(community_id.as_uuid())
        .bind(agent_pubkey)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| DbError::NotFound("managed agent transfer".into()))?;

        let stored: serde_json::Value = row.try_get("record")?;
        let mut record = decode_record(community_id, agent_pubkey, Some(operation_id), stored)?;
        record.apply(expected_revision, expected_epoch, command)?;
        let json = serde_json::to_value(&record)?;

        sqlx::query(
            "UPDATE managed_agent_transfers
             SET record = $3, updated_at = clock_timestamp()
             WHERE community_id = $1 AND agent_pubkey = $2",
        )
        .bind(community_id.as_uuid())
        .bind(agent_pubkey)
        .bind(json)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::agent_transfer::Executor;
    use uuid::Uuid;

    fn record() -> TransferRecord {
        TransferRecord::new(
            Uuid::nil().to_string(),
            "agent-pubkey",
            "operation-1",
            Executor::new("mac-1", "Mac").unwrap(),
            Executor::new("server-1", "Server").unwrap(),
            3,
        )
        .unwrap()
    }

    #[test]
    fn scope_validation_binds_record_to_the_database_request() {
        let community = CommunityId::from_uuid(Uuid::nil());
        let record = record();
        assert!(validate_scope(community, "agent-pubkey", &record).is_ok());
        assert!(validate_scope(community, "other-agent", &record).is_err());
    }

    #[test]
    fn serialized_record_contains_no_private_material() {
        let json = serde_json::to_value(record()).unwrap();
        assert!(json.get("private_key").is_none());
        assert!(json.get("private_key_nsec").is_none());
    }
}
