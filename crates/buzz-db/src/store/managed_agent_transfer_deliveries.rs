//! Durable delivery queue for managed-agent transfer coordinator requests.
//!
//! The queue stores only the signed, secret-free coordinator event that must
//! reach the target runtime. It is separate from the transfer state row: the
//! state machine is authoritative for lifecycle, while this table is only a
//! retryable transport adapter for an offline or reconnecting runtime.

use buzz_core::CommunityId;
use buzz_datastore_tracing::datastore_span;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row as _};
use uuid::Uuid;

use crate::error::Result;
use crate::Db;

/// A claimed managed-agent transfer delivery row.
#[derive(Debug, Clone)]
pub struct ManagedAgentTransferDelivery {
    /// Queue row id.
    pub id: Uuid,
    /// Community containing the target agent.
    pub community_id: CommunityId,
    /// Target agent public key in canonical lowercase hex.
    pub agent_pubkey: String,
    /// Transfer operation id.
    pub operation_id: String,
    /// State-machine revision represented by the event.
    pub revision: u64,
    /// Delivery class used to keep an owner start distinct from an executor
    /// command that observes the same state-machine revision.
    pub message_type: String,
    /// Signed coordinator event id in canonical lowercase hex.
    pub event_id: String,
    /// Serialized signed coordinator event.
    pub event: serde_json::Value,
    /// Delivery state (`pending`, `delivered`, or `failed`).
    pub state: String,
    /// Number of failed delivery attempts.
    pub attempt_count: i32,
    /// Last delivery error, if any.
    pub error_message: Option<String>,
    /// Lease fencing token assigned to this claim.
    pub claim_token: Uuid,
    /// Queue insertion time.
    pub created_at: DateTime<Utc>,
}

/// Maximum number of offline/malformed delivery attempts before terminal failure.
pub const TRANSFER_DELIVERY_MAX_ATTEMPTS: i32 = 10;

/// Enqueue one signed coordinator event exactly once for an operation revision
/// and delivery class.
///
/// Returning `true` means a new row was inserted. Retrying the same operation
/// revision and message type is a no-op, which makes owner retries safe after a
/// relay crash while keeping an executor command at the same revision distinct.
#[datastore_span(
    name = "managed_agent_transfer_delivery_enqueue",
    system = "postgresql"
)]
#[allow(clippy::too_many_arguments)]
pub async fn enqueue(
    pool: &PgPool,
    community_id: CommunityId,
    agent_pubkey: &str,
    operation_id: &str,
    revision: u64,
    message_type: &str,
    event_id: &str,
    event: serde_json::Value,
) -> Result<bool> {
    let revision = i64::try_from(revision).map_err(|_| {
        crate::error::DbError::InvalidData(
            "transfer delivery revision exceeds PostgreSQL bigint".into(),
        )
    })?;
    let result = sqlx::query(
        r#"
        INSERT INTO managed_agent_transfer_deliveries
            (community_id, agent_pubkey, operation_id, revision, message_type, event_id, event)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (community_id, agent_pubkey, operation_id, revision, message_type)
        DO NOTHING
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(agent_pubkey)
    .bind(operation_id)
    .bind(revision)
    .bind(message_type)
    .bind(event_id)
    .bind(event)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Claim pending delivery rows using short DB leases and per-row fencing tokens.
#[datastore_span(name = "managed_agent_transfer_delivery_claim", system = "postgresql")]
pub async fn claim_batch(
    pool: &PgPool,
    worker_id: &str,
    lease_until: DateTime<Utc>,
    batch_size: i64,
) -> Result<Vec<ManagedAgentTransferDelivery>> {
    let candidate_ids: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT id
        FROM managed_agent_transfer_deliveries
        WHERE state = 'pending'
          AND (lease_expires_at IS NULL OR lease_expires_at < now())
          AND (retry_after IS NULL OR retry_after <= now())
        ORDER BY retry_after NULLS FIRST, created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await?;

    let mut rows = Vec::with_capacity(candidate_ids.len());
    for id in candidate_ids {
        let claim_token = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            UPDATE managed_agent_transfer_deliveries
            SET held_by = $2, lease_expires_at = $3, claim_token = $4, updated_at = now()
            WHERE id = $1
              AND state = 'pending'
              AND (lease_expires_at IS NULL OR lease_expires_at < now())
            RETURNING id, community_id, agent_pubkey, operation_id, revision,
                      message_type, event_id, event, state, attempt_count, error_message,
                      claim_token, created_at
            "#,
        )
        .bind(id)
        .bind(worker_id)
        .bind(lease_until)
        .bind(claim_token)
        .fetch_optional(pool)
        .await?;
        if let Some(row) = row {
            rows.push(row_to_delivery(row)?);
        }
    }
    Ok(rows)
}

/// Mark a claimed delivery as complete, fenced by its claim token.
#[datastore_span(
    name = "managed_agent_transfer_delivery_delivered",
    system = "postgresql"
)]
pub async fn mark_delivered(pool: &PgPool, id: Uuid, claim_token: Uuid) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE managed_agent_transfer_deliveries
        SET state = 'delivered', held_by = NULL, lease_expires_at = NULL,
            claim_token = NULL, delivered_at = now(), updated_at = now()
        WHERE id = $1 AND claim_token = $2 AND state = 'pending'
        "#,
    )
    .bind(id)
    .bind(claim_token)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Record a failed delivery, applying bounded exponential retry backoff.
#[datastore_span(name = "managed_agent_transfer_delivery_failed", system = "postgresql")]
pub async fn fail(pool: &PgPool, id: Uuid, claim_token: Uuid, error: &str) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE managed_agent_transfer_deliveries
        SET attempt_count = attempt_count + 1,
            error_message = $3,
            state = CASE WHEN attempt_count + 1 >= $4 THEN 'failed' ELSE 'pending' END,
            retry_after = CASE WHEN attempt_count + 1 >= $4 THEN NULL
                               ELSE now() + (LEAST(POWER(2, attempt_count), 300) * INTERVAL '1 second')
                          END,
            held_by = NULL, lease_expires_at = NULL, claim_token = NULL, updated_at = now()
        WHERE id = $1 AND claim_token = $2 AND state = 'pending'
        "#,
    )
    .bind(id)
    .bind(claim_token)
    .bind(error)
    .bind(TRANSFER_DELIVERY_MAX_ATTEMPTS)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

fn row_to_delivery(row: sqlx::postgres::PgRow) -> Result<ManagedAgentTransferDelivery> {
    let revision: i64 = row.try_get("revision")?;
    let community_uuid: uuid::Uuid = row.try_get("community_id")?;
    Ok(ManagedAgentTransferDelivery {
        id: row.try_get("id")?,
        community_id: CommunityId::from_uuid(community_uuid),
        agent_pubkey: row.try_get("agent_pubkey")?,
        operation_id: row.try_get("operation_id")?,
        revision: u64::try_from(revision).map_err(|_| {
            crate::error::DbError::InvalidData(
                "stored transfer delivery revision cannot be negative".into(),
            )
        })?,
        message_type: row.try_get("message_type")?,
        event_id: row.try_get("event_id")?,
        event: row.try_get("event")?,
        state: row.try_get("state")?,
        attempt_count: row.try_get("attempt_count")?,
        error_message: row.try_get("error_message")?,
        claim_token: row
            .try_get::<Option<Uuid>, _>("claim_token")?
            .ok_or_else(|| {
                crate::error::DbError::InvalidData(
                    "claimed transfer delivery has no claim token".into(),
                )
            })?,
        created_at: row.try_get("created_at")?,
    })
}

impl Db {
    /// Enqueue one transfer coordinator event idempotently.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_managed_agent_transfer_delivery(
        &self,
        community_id: CommunityId,
        agent_pubkey: &str,
        operation_id: &str,
        revision: u64,
        message_type: &str,
        event_id: &str,
        event: serde_json::Value,
    ) -> Result<bool> {
        enqueue(
            &self.pool,
            community_id,
            agent_pubkey,
            operation_id,
            revision,
            message_type,
            event_id,
            event,
        )
        .await
    }

    /// Claim a batch of pending transfer coordinator deliveries.
    pub async fn claim_pending_managed_agent_transfer_deliveries(
        &self,
        worker_id: &str,
        lease_until: DateTime<Utc>,
        batch_size: i64,
    ) -> Result<Vec<ManagedAgentTransferDelivery>> {
        claim_batch(&self.pool, worker_id, lease_until, batch_size).await
    }

    /// Mark a transfer coordinator delivery as delivered.
    pub async fn mark_managed_agent_transfer_delivery_delivered(
        &self,
        id: Uuid,
        claim_token: Uuid,
    ) -> Result<bool> {
        mark_delivered(&self.pool, id, claim_token).await
    }

    /// Record a failed transfer coordinator delivery.
    pub async fn fail_managed_agent_transfer_delivery(
        &self,
        id: Uuid,
        claim_token: Uuid,
        error: &str,
    ) -> Result<bool> {
        fail(&self.pool, id, claim_token, error).await
    }
}
