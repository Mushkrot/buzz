-- Durable transition journal for managed-agent transfers.
--
-- The current transfer row is the authority source. This append-only journal
-- keeps each accepted state transition and its resulting snapshot available
-- for recovery, diagnostics, and a future runtime supervisor. It contains no
-- private keys, credentials, process handles, or harness sessions.

CREATE TABLE managed_agent_transfer_journal (
    community_id UUID NOT NULL REFERENCES communities(id),
    agent_pubkey TEXT NOT NULL CHECK (length(agent_pubkey) BETWEEN 1 AND 256),
    operation_id TEXT NOT NULL CHECK (length(operation_id) BETWEEN 1 AND 256),
    revision    BIGINT NOT NULL CHECK (revision >= 0),
    epoch       BIGINT NOT NULL CHECK (epoch > 0),
    event_kind  TEXT NOT NULL CHECK (event_kind IN ('created', 'command')),
    command     JSONB CHECK (command IS NULL OR jsonb_typeof(command) = 'object'),
    record      JSONB NOT NULL CHECK (jsonb_typeof(record) = 'object'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, agent_pubkey, revision),
    UNIQUE (community_id, operation_id, revision),
    CHECK (
        (event_kind = 'created' AND command IS NULL)
        OR (event_kind = 'command' AND command IS NOT NULL)
    )
);

CREATE INDEX managed_agent_transfer_journal_created_idx
    ON managed_agent_transfer_journal (community_id, agent_pubkey, created_at DESC);

SELECT attach_community_write_fence('managed_agent_transfer_journal');
