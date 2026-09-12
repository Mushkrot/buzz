//! Pure lifecycle contract for moving one agent identity between executors.
//!
//! This module performs no I/O and carries no secret material. A relay adapter
//! owns persistence and uniqueness; a runtime adapter owns process draining.
//! Both use this state machine so stale commands cannot grant a second
//! executor the active epoch.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version of the transfer state-machine contract.
pub const TRANSFER_PROTOCOL_VERSION: u32 = 1;
const MAX_REASON_BYTES: usize = 512;

/// A concrete executor location participating in a transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Executor {
    /// Stable per-installation or per-deployment instance identifier.
    pub instance_id: String,
    /// Human-readable location label, never used as an authority check.
    pub location: String,
}

impl Executor {
    /// Construct an executor reference after validating its identifiers.
    pub fn new(instance_id: impl Into<String>, location: impl Into<String>) -> Result<Self, Error> {
        let executor = Self {
            instance_id: instance_id.into(),
            location: location.into(),
        };
        validate_identifier("instance_id", &executor.instance_id)?;
        validate_identifier("location", &executor.location)?;
        Ok(executor)
    }

    fn validate(&self) -> Result<(), Error> {
        validate_identifier("instance_id", &self.instance_id)?;
        validate_identifier("location", &self.location)
    }
}

/// Lifecycle phase of one transfer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferPhase {
    /// The target is being checked and no source work has been stopped.
    Preparing,
    /// The source no longer accepts new work and is finishing current work.
    Draining,
    /// The source confirmed that its executor tree is quiescent.
    SourceQuiesced,
    /// The target has received the new epoch and may prepare to run.
    Activating,
    /// The target reported a live runtime with the expected epoch.
    Verifying,
    /// The target is the confirmed active executor.
    Completed,
    /// The operation was cancelled before authority moved to the target.
    Cancelled,
    /// The outcome is uncertain and requires an explicit recovery decision.
    NeedsAttention,
}

impl TransferPhase {
    /// Whether no further ordinary transition is allowed from this phase.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::NeedsAttention
        )
    }
}

/// A command applied by the relay coordinator or a runtime supervisor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransferCommand {
    /// Source may stop accepting new work.
    BeginDrain,
    /// Source has no active work or child tools.
    ConfirmSourceQuiesced,
    /// Relay grants the target a new epoch.
    ActivateTarget,
    /// Target confirms the granted epoch and runtime configuration.
    ConfirmTarget,
    /// Complete a verified transfer.
    Complete,
    /// Cancel while authority has not moved to the target.
    Cancel,
    /// Resume the source after a pre-activation cancellation.
    ResumeSource,
    /// Preserve uncertainty instead of guessing after a broken handoff.
    MarkNeedsAttention {
        /// Sanitized diagnostic retained for operator recovery.
        reason: String,
    },
}

/// Durable state needed to coordinate one transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferRecord {
    /// State-machine protocol version.
    pub protocol_version: u32,
    /// Community scope containing the agent identity.
    pub community_id: String,
    /// Agent public key; private keys never occur in this record.
    pub agent_pubkey: String,
    /// Idempotency key for this transfer request.
    pub operation_id: String,
    /// Monotonic CAS revision for every successful transition.
    pub revision: u64,
    /// Fencing epoch checked immediately before executor actions.
    pub epoch: u64,
    /// Source executor for this operation.
    pub source: Executor,
    /// Target executor for this operation.
    pub target: Executor,
    /// Saved agent configuration revision being moved.
    pub config_revision: u64,
    /// Current lifecycle phase.
    pub phase: TransferPhase,
    /// Executor currently holding the recorded authority, if known.
    pub active_executor: Option<Executor>,
    /// Sanitized recovery explanation when phase is `NeedsAttention`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_reason: Option<String>,
}

impl TransferRecord {
    /// Create a new operation for the currently active source executor.
    pub fn new(
        community_id: impl Into<String>,
        agent_pubkey: impl Into<String>,
        operation_id: impl Into<String>,
        source: Executor,
        target: Executor,
        config_revision: u64,
    ) -> Result<Self, Error> {
        let record = Self {
            protocol_version: TRANSFER_PROTOCOL_VERSION,
            community_id: community_id.into(),
            agent_pubkey: agent_pubkey.into(),
            operation_id: operation_id.into(),
            revision: 0,
            epoch: 1,
            source: source.clone(),
            target,
            config_revision,
            phase: TransferPhase::Preparing,
            active_executor: Some(source),
            attention_reason: None,
        };
        record.validate()?;
        Ok(record)
    }

    /// Validate structural invariants without contacting a relay or executor.
    pub fn validate(&self) -> Result<(), Error> {
        validate_identifier("community_id", &self.community_id)?;
        validate_identifier("agent_pubkey", &self.agent_pubkey)?;
        validate_identifier("operation_id", &self.operation_id)?;
        self.source.validate()?;
        self.target.validate()?;
        if self.protocol_version != TRANSFER_PROTOCOL_VERSION {
            return Err(Error::UnsupportedProtocol(self.protocol_version));
        }
        if self.source.instance_id == self.target.instance_id {
            return Err(Error::SameExecutor);
        }
        if self.phase == TransferPhase::NeedsAttention && self.attention_reason.is_none() {
            return Err(Error::MissingAttentionReason);
        }
        if self.phase != TransferPhase::NeedsAttention && self.attention_reason.is_some() {
            return Err(Error::UnexpectedAttentionReason);
        }
        if self.phase == TransferPhase::Completed
            && self.active_executor.as_ref() != Some(&self.target)
        {
            return Err(Error::CompletedWithoutTarget);
        }
        match self.phase {
            TransferPhase::Preparing
            | TransferPhase::Draining
            | TransferPhase::SourceQuiesced
            | TransferPhase::Cancelled => {
                if self.active_executor.as_ref() != Some(&self.source) {
                    return Err(Error::InvalidActiveExecutor);
                }
            }
            TransferPhase::Activating | TransferPhase::Verifying | TransferPhase::Completed => {
                if self.active_executor.as_ref() != Some(&self.target) {
                    return Err(Error::InvalidActiveExecutor);
                }
            }
            TransferPhase::NeedsAttention => {
                if let Some(active) = self.active_executor.as_ref() {
                    if active != &self.source && active != &self.target {
                        return Err(Error::InvalidActiveExecutor);
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply a compare-and-swap command using an observed revision and epoch.
    /// Successful transitions increment `revision`; activation also advances
    /// `epoch`, fencing commands from the previous executor.
    pub fn apply(
        &mut self,
        expected_revision: u64,
        expected_epoch: u64,
        command: TransferCommand,
    ) -> Result<(), Error> {
        self.validate()?;
        if expected_revision != self.revision {
            return Err(Error::StaleRevision {
                expected: expected_revision,
                actual: self.revision,
            });
        }
        if expected_epoch != self.epoch {
            return Err(Error::StaleEpoch {
                expected: expected_epoch,
                actual: self.epoch,
            });
        }
        if self.revision == u64::MAX {
            return Err(Error::RevisionExhausted);
        }

        let next_phase = match (&self.phase, &command) {
            (TransferPhase::Preparing, TransferCommand::BeginDrain) => TransferPhase::Draining,
            (TransferPhase::Draining, TransferCommand::ConfirmSourceQuiesced) => {
                TransferPhase::SourceQuiesced
            }
            (TransferPhase::SourceQuiesced, TransferCommand::ActivateTarget) => {
                if self.epoch == u64::MAX {
                    return Err(Error::EpochExhausted);
                }
                self.epoch = self.epoch.checked_add(1).ok_or(Error::EpochExhausted)?;
                self.active_executor = Some(self.target.clone());
                TransferPhase::Activating
            }
            (TransferPhase::Activating, TransferCommand::ConfirmTarget) => TransferPhase::Verifying,
            (TransferPhase::Verifying, TransferCommand::Complete) => TransferPhase::Completed,
            (TransferPhase::Preparing, TransferCommand::Cancel) => TransferPhase::Cancelled,
            (TransferPhase::Draining, TransferCommand::Cancel) => {
                self.active_executor = Some(self.source.clone());
                TransferPhase::Preparing
            }
            (TransferPhase::SourceQuiesced, TransferCommand::ResumeSource) => {
                self.active_executor = Some(self.source.clone());
                TransferPhase::Preparing
            }
            (phase, TransferCommand::MarkNeedsAttention { reason }) if !phase.is_terminal() => {
                self.attention_reason = Some(sanitize_reason(reason)?);
                TransferPhase::NeedsAttention
            }
            _ => {
                return Err(Error::InvalidTransition {
                    phase: self.phase,
                    command: command.name(),
                })
            }
        };

        self.phase = next_phase;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(Error::RevisionExhausted)?;
        self.validate()
    }
}

impl TransferCommand {
    fn name(&self) -> &'static str {
        match self {
            Self::BeginDrain => "begin_drain",
            Self::ConfirmSourceQuiesced => "confirm_source_quiesced",
            Self::ActivateTarget => "activate_target",
            Self::ConfirmTarget => "confirm_target",
            Self::Complete => "complete",
            Self::Cancel => "cancel",
            Self::ResumeSource => "resume_source",
            Self::MarkNeedsAttention { .. } => "mark_needs_attention",
        }
    }
}

/// Errors returned before a state transition is committed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    /// A caller supplied an invalid or empty identifier.
    #[error("invalid {field}")]
    InvalidIdentifier {
        /// Identifier field that failed validation.
        field: &'static str,
    },
    /// Protocol version is not understood.
    #[error("unsupported transfer protocol version {0}")]
    UnsupportedProtocol(u32),
    /// Source and target cannot be the same executor.
    #[error("source and target must be different executors")]
    SameExecutor,
    /// The caller's compare-and-swap revision is stale.
    #[error("stale transfer revision: expected {expected}, actual {actual}")]
    StaleRevision {
        /// Revision supplied by the caller.
        expected: u64,
        /// Revision currently held by the coordinator.
        actual: u64,
    },
    /// The caller's fencing epoch is stale.
    #[error("stale transfer epoch: expected {expected}, actual {actual}")]
    StaleEpoch {
        /// Epoch supplied by the caller.
        expected: u64,
        /// Epoch currently held by the coordinator.
        actual: u64,
    },
    /// Command is not legal in the current phase.
    #[error("invalid transfer transition from {phase:?}: {command}")]
    InvalidTransition {
        /// Current lifecycle phase.
        phase: TransferPhase,
        /// Rejected command name.
        command: &'static str,
    },
    /// A diagnostic reason was absent or too large.
    #[error("invalid recovery reason")]
    InvalidReason,
    /// NeedsAttention must always explain recovery.
    #[error("needs-attention state requires a recovery reason")]
    MissingAttentionReason,
    /// A recovery reason was attached to a non-recovery state.
    #[error("recovery reason is only valid in needs-attention state")]
    UnexpectedAttentionReason,
    /// Completed state must name the target as authority holder.
    #[error("completed transfer does not have the target as active executor")]
    CompletedWithoutTarget,
    /// Non-recovery phases must have the expected authority holder.
    #[error("transfer phase has an invalid active executor")]
    InvalidActiveExecutor,
    /// Monotonic state counters cannot wrap.
    #[error("transfer epoch exhausted")]
    EpochExhausted,
    /// Monotonic state counters cannot wrap.
    #[error("transfer revision exhausted")]
    RevisionExhausted,
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(Error::InvalidIdentifier { field });
    }
    Ok(())
}

fn sanitize_reason(reason: &str) -> Result<String, Error> {
    if reason.is_empty() || reason.len() > MAX_REASON_BYTES || reason.chars().any(char::is_control)
    {
        return Err(Error::InvalidReason);
    }
    Ok(reason.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> TransferRecord {
        TransferRecord::new(
            "community-a",
            "agent-pubkey",
            "operation-1",
            Executor::new("mac-1", "Mac").unwrap(),
            Executor::new("server-1", "Server").unwrap(),
            7,
        )
        .unwrap()
    }

    fn apply(record: &mut TransferRecord, command: TransferCommand) {
        record
            .apply(record.revision, record.epoch, command)
            .unwrap();
    }

    #[test]
    fn happy_path_fences_target_with_a_new_epoch() {
        let mut record = record();
        assert_eq!(record.epoch, 1);
        apply(&mut record, TransferCommand::BeginDrain);
        apply(&mut record, TransferCommand::ConfirmSourceQuiesced);
        apply(&mut record, TransferCommand::ActivateTarget);
        assert_eq!(record.phase, TransferPhase::Activating);
        assert_eq!(record.epoch, 2);
        assert_eq!(
            record.active_executor.as_ref().unwrap().instance_id,
            "server-1"
        );
        apply(&mut record, TransferCommand::ConfirmTarget);
        apply(&mut record, TransferCommand::Complete);
        assert_eq!(record.phase, TransferPhase::Completed);
    }

    #[test]
    fn stale_commands_cannot_reuse_the_previous_epoch() {
        let mut record = record();
        apply(&mut record, TransferCommand::BeginDrain);
        apply(&mut record, TransferCommand::ConfirmSourceQuiesced);
        let revision = record.revision;
        apply(&mut record, TransferCommand::ActivateTarget);
        let error = record
            .apply(revision, 1, TransferCommand::ConfirmTarget)
            .unwrap_err();
        assert_eq!(
            error,
            Error::StaleRevision {
                expected: revision,
                actual: record.revision
            }
        );
    }

    #[test]
    fn a_lost_handoff_is_not_cancelled_by_guessing() {
        let mut record = record();
        apply(&mut record, TransferCommand::BeginDrain);
        apply(&mut record, TransferCommand::ConfirmSourceQuiesced);
        apply(
            &mut record,
            TransferCommand::MarkNeedsAttention {
                reason: "target acknowledgement lost".into(),
            },
        );
        assert_eq!(record.phase, TransferPhase::NeedsAttention);
        assert_eq!(
            record.active_executor.as_ref().unwrap().instance_id,
            "mac-1"
        );
        assert!(record
            .apply(record.revision, record.epoch, TransferCommand::Complete)
            .is_err());
    }

    #[test]
    fn source_can_resume_before_target_activation() {
        let mut record = record();
        apply(&mut record, TransferCommand::BeginDrain);
        apply(&mut record, TransferCommand::ConfirmSourceQuiesced);
        apply(&mut record, TransferCommand::ResumeSource);
        assert_eq!(record.phase, TransferPhase::Preparing);
        assert_eq!(
            record.active_executor.as_ref().unwrap().instance_id,
            "mac-1"
        );
    }

    #[test]
    fn cancelling_during_drain_returns_source_to_preparing() {
        let mut record = record();
        apply(&mut record, TransferCommand::BeginDrain);
        apply(&mut record, TransferCommand::Cancel);
        assert_eq!(record.phase, TransferPhase::Preparing);
        assert_eq!(
            record.active_executor.as_ref().unwrap().instance_id,
            "mac-1"
        );
    }

    #[test]
    fn same_instance_id_cannot_be_used_as_both_source_and_target() {
        let source = Executor::new("same", "Mac").unwrap();
        let target = Executor::new("same", "Server").unwrap();
        assert_eq!(
            TransferRecord::new(
                "community-a",
                "agent-pubkey",
                "operation-1",
                source,
                target,
                7,
            )
            .unwrap_err(),
            Error::SameExecutor
        );
    }

    #[test]
    fn exhausted_counters_leave_the_record_unchanged() {
        let mut revision_exhausted = record();
        revision_exhausted.revision = u64::MAX;
        let revision_snapshot = revision_exhausted.clone();
        assert_eq!(
            revision_exhausted.apply(
                u64::MAX,
                revision_exhausted.epoch,
                TransferCommand::BeginDrain,
            ),
            Err(Error::RevisionExhausted)
        );
        assert_eq!(revision_exhausted, revision_snapshot);

        let mut epoch_exhausted = record();
        epoch_exhausted.phase = TransferPhase::SourceQuiesced;
        epoch_exhausted.epoch = u64::MAX;
        let epoch_snapshot = epoch_exhausted.clone();
        assert_eq!(
            epoch_exhausted.apply(
                epoch_exhausted.revision,
                u64::MAX,
                TransferCommand::ActivateTarget,
            ),
            Err(Error::EpochExhausted)
        );
        assert_eq!(epoch_exhausted, epoch_snapshot);
    }

    #[test]
    fn json_round_trip_preserves_the_fencing_record() {
        let original = record();
        let json = serde_json::to_string(&original).unwrap();
        let decoded: TransferRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn json_round_trip_preserves_transfer_commands() {
        let commands = [
            TransferCommand::BeginDrain,
            TransferCommand::ConfirmSourceQuiesced,
            TransferCommand::ActivateTarget,
            TransferCommand::ConfirmTarget,
            TransferCommand::Complete,
            TransferCommand::Cancel,
            TransferCommand::ResumeSource,
            TransferCommand::MarkNeedsAttention {
                reason: "target acknowledgement lost".into(),
            },
        ];

        for original in commands {
            let json = serde_json::to_string(&original).unwrap();
            let decoded: TransferCommand = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, original);
        }
    }
}
