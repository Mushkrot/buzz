-- Durable, retryable transport for signed managed-agent transfer coordinator
-- events. The transfer state and journal remain authoritative; this table only
-- prevents an offline runtime from missing an accepted coordinator event.

CREATE TABLE managed_agent_transfer_deliveries (
    id            UUID NOT NULL DEFAULT gen_random_uuid(),
    community_id  UUID NOT NULL REFERENCES communities(id),
    agent_pubkey  TEXT NOT NULL CHECK (length(agent_pubkey) BETWEEN 1 AND 256),
    operation_id  TEXT NOT NULL CHECK (length(operation_id) BETWEEN 1 AND 256),
    revision      BIGINT NOT NULL CHECK (revision >= 0),
    event_id      TEXT NOT NULL CHECK (length(event_id) = 64),
    event         JSONB NOT NULL CHECK (jsonb_typeof(event) = 'object'),
    state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending', 'delivered', 'failed')),
    attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    retry_after   TIMESTAMPTZ,
    held_by       TEXT,
    lease_expires_at TIMESTAMPTZ,
    claim_token   UUID,
    error_message TEXT,
    delivered_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, id),
    UNIQUE (community_id, agent_pubkey, operation_id, revision),
    UNIQUE (community_id, event_id)
);

CREATE INDEX managed_agent_transfer_deliveries_pending_idx
    ON managed_agent_transfer_deliveries (retry_after, created_at)
    WHERE state = 'pending';

CREATE INDEX managed_agent_transfer_deliveries_agent_idx
    ON managed_agent_transfer_deliveries (community_id, agent_pubkey, created_at DESC);

SELECT attach_community_write_fence('managed_agent_transfer_deliveries');
