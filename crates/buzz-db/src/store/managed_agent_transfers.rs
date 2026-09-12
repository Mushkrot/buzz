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
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row as _, Transaction};

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

/// The kind of accepted event recorded for a managed-agent transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedAgentTransferJournalEvent {
    /// The initial durable transfer record was created.
    Created,
    /// A state-machine command was accepted.
    Command(TransferCommand),
}

/// One append-only transition entry for a managed-agent transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedAgentTransferJournalEntry {
    /// Community containing the agent.
    pub community_id: CommunityId,
    /// Agent identity being moved.
    pub agent_pubkey: String,
    /// Idempotency key for the transfer operation.
    pub operation_id: String,
    /// State-machine revision after this event.
    pub revision: u64,
    /// Fencing epoch after this event.
    pub epoch: u64,
    /// Accepted event.
    pub event: ManagedAgentTransferJournalEvent,
    /// Full public transfer snapshot after this event.
    pub record: TransferRecord,
    /// Database insertion time.
    pub created_at: DateTime<Utc>,
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

        let mut tx = self.pool.begin().await?;
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
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = inserted {
            let stored: serde_json::Value = row.try_get("record")?;
            let stored = decode_record(community_id, &record.agent_pubkey, None, stored)?;
            append_journal(&mut tx, &stored, "created", None).await?;
            tx.commit().await?;
            return Ok(CreateManagedAgentTransferResult::Created(stored));
        }

        let existing = sqlx::query(
            "SELECT record
             FROM managed_agent_transfers
             WHERE community_id = $1 AND agent_pubkey = $2",
        )
        .bind(community_id.as_uuid())
        .bind(&record.agent_pubkey)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = existing {
            let stored: serde_json::Value = row.try_get("record")?;
            let stored = decode_record(community_id, &record.agent_pubkey, None, stored)?;
            if stored.operation_id == record.operation_id {
                tx.commit().await?;
                return Ok(CreateManagedAgentTransferResult::AlreadyExists(stored));
            }
            tx.commit().await?;
            return Err(DbError::InvalidData(
                "another transfer operation already owns this agent".into(),
            ));
        }

        tx.commit().await?;
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

    /// Read accepted transfer events in chronological order.
    #[datastore_span(name = "managed_agent_transfer_journal_list", system = "postgresql")]
    pub async fn list_managed_agent_transfer_journal(
        &self,
        community_id: CommunityId,
        agent_pubkey: &str,
        limit: i64,
    ) -> Result<Vec<ManagedAgentTransferJournalEntry>> {
        let limit = limit.clamp(1, 1_000);
        let rows = sqlx::query(
            "SELECT operation_id, revision, epoch, event_kind, command, record, created_at
             FROM managed_agent_transfer_journal
             WHERE community_id = $1 AND agent_pubkey = $2
             ORDER BY revision ASC
             LIMIT $3",
        )
        .bind(community_id.as_uuid())
        .bind(agent_pubkey)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let operation_id: String = row.try_get("operation_id")?;
                let revision: i64 = row.try_get("revision")?;
                let epoch: i64 = row.try_get("epoch")?;
                let event_kind: String = row.try_get("event_kind")?;
                let command: Option<serde_json::Value> = row.try_get("command")?;
                let record_value: serde_json::Value = row.try_get("record")?;
                let created_at: DateTime<Utc> = row.try_get("created_at")?;
                let record = decode_record(
                    community_id,
                    agent_pubkey,
                    Some(&operation_id),
                    record_value,
                )?;
                let event = decode_journal_event(&event_kind, command)?;
                if revision < 0 || epoch < 0 {
                    return Err(DbError::InvalidData(
                        "stored transfer journal counters cannot be negative".into(),
                    ));
                }
                if revision as u64 != record.revision || epoch as u64 != record.epoch {
                    return Err(DbError::InvalidData(
                        "stored transfer journal counters do not match the snapshot".into(),
                    ));
                }
                Ok(ManagedAgentTransferJournalEntry {
                    community_id,
                    agent_pubkey: agent_pubkey.to_owned(),
                    operation_id,
                    revision: revision as u64,
                    epoch: epoch as u64,
                    event,
                    record,
                    created_at,
                })
            })
            .collect()
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
        let command_json = serde_json::to_value(&command)?;
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
        append_journal(&mut tx, &record, "command", Some(command_json)).await?;
        tx.commit().await?;
        Ok(record)
    }
}

async fn append_journal(
    tx: &mut Transaction<'_, Postgres>,
    record: &TransferRecord,
    event_kind: &str,
    command: Option<serde_json::Value>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO managed_agent_transfer_journal
            (community_id, agent_pubkey, operation_id, revision, epoch, event_kind, command, record)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(
        record.community_id.parse::<uuid::Uuid>().map_err(|_| {
            DbError::InvalidData("transfer record community id is not a UUID".into())
        })?,
    )
    .bind(&record.agent_pubkey)
    .bind(&record.operation_id)
    .bind(
        i64::try_from(record.revision).map_err(|_| {
            DbError::InvalidData("transfer revision exceeds PostgreSQL bigint".into())
        })?,
    )
    .bind(
        i64::try_from(record.epoch)
            .map_err(|_| DbError::InvalidData("transfer epoch exceeds PostgreSQL bigint".into()))?,
    )
    .bind(event_kind)
    .bind(command)
    .bind(serde_json::to_value(record)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn decode_journal_event(
    event_kind: &str,
    command: Option<serde_json::Value>,
) -> Result<ManagedAgentTransferJournalEvent> {
    match (event_kind, command) {
        ("created", None) => Ok(ManagedAgentTransferJournalEvent::Created),
        ("command", Some(command)) => Ok(ManagedAgentTransferJournalEvent::Command(
            serde_json::from_value(command)?,
        )),
        _ => Err(DbError::InvalidData(
            "stored transfer journal event is malformed".into(),
        )),
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
