-- Distinguish owner-start delivery from executor-command delivery. Both can
-- legitimately observe the same state-machine revision, so the original
-- revision-only uniqueness key is too coarse for command propagation.

ALTER TABLE managed_agent_transfer_deliveries
    ADD COLUMN IF NOT EXISTS message_type TEXT NOT NULL DEFAULT 'owner_start'
    CHECK (length(message_type) BETWEEN 1 AND 64);

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT conname
      INTO constraint_name
      FROM pg_constraint
     WHERE conrelid = 'managed_agent_transfer_deliveries'::regclass
       AND contype = 'u'
       AND pg_get_constraintdef(oid) LIKE
           'UNIQUE (community_id, agent_pubkey, operation_id, revision)%'
     LIMIT 1;

    IF constraint_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE managed_agent_transfer_deliveries DROP CONSTRAINT %I',
            constraint_name
        );
    END IF;
END $$;

ALTER TABLE managed_agent_transfer_deliveries
    ADD CONSTRAINT managed_agent_transfer_deliveries_delivery_key
    UNIQUE (community_id, agent_pubkey, operation_id, revision, message_type);
