-- Durable coordinator state for moving one managed agent identity between
-- trusted executors. The JSON record is the serialized buzz-core transfer
-- contract; private keys and runtime credentials never belong in this table.
--
-- One row per community/agent is intentional for the first coordinator slice:
-- a new operation cannot start while a previous record still owns the identity.
-- A retry of the same operation_id reads the existing row instead of creating a
-- second operation. A later history table can be added without changing this
-- safety fence.

CREATE TABLE managed_agent_transfers (
    community_id UUID NOT NULL REFERENCES communities(id),
    agent_pubkey TEXT NOT NULL CHECK (length(agent_pubkey) BETWEEN 1 AND 256),
    operation_id TEXT NOT NULL CHECK (length(operation_id) BETWEEN 1 AND 256),
    record      JSONB NOT NULL CHECK (jsonb_typeof(record) = 'object'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, agent_pubkey),
    UNIQUE (community_id, operation_id)
);

CREATE INDEX managed_agent_transfers_updated_idx
    ON managed_agent_transfers (community_id, updated_at DESC);

SELECT attach_community_write_fence('managed_agent_transfers');
