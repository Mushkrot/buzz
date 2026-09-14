//! Pure runtime-supervisor decision seam for managed-agent transfers.
//!
//! This module intentionally has no process, filesystem, or relay side
//! effects. It turns the durable transfer snapshot and the local executor
//! instance id into a bounded intent. A later adapter can implement the
//! intent with the actual ACP pool lifecycle and report a fenced command back
//! to the relay.

use buzz_core::agent_transfer::{TransferPhase, TransferRecord};

/// Which executor role this runtime has in a transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeRole {
    /// The current runtime is the source being drained.
    Source,
    /// The current runtime is the target being activated.
    Target,
    /// The snapshot does not address this runtime instance.
    Unrelated,
}

/// Side-effect-free intent returned to the future lifecycle adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SupervisorAction {
    /// Stop accepting new work, then measure and report quiescence.
    DrainSource,
    /// Wait until the coordinator records source quiescence.
    WaitForSource,
    /// Wait for the coordinator to grant the target epoch.
    WaitForActivation,
    /// Commit the source-side transition that grants the target a new epoch.
    GrantTargetActivation,
    /// Start or restore the target runtime for the granted epoch.
    ActivateTarget,
    /// Verify the target runtime before reporting it to the coordinator.
    VerifyTarget,
    /// No lifecycle action is required at this runtime.
    Idle,
    /// Stop ordinary automation and require an explicit recovery decision.
    NeedsAttention,
}

/// Decision made for one transfer snapshot and one local executor instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SupervisorDecision {
    /// Role of the local runtime.
    pub role: RuntimeRole,
    /// Bounded intent for the lifecycle adapter.
    pub action: SupervisorAction,
}

/// Compute a supervisor intent without changing state or asserting readiness.
pub(crate) fn decide(record: &TransferRecord, local_instance_id: &str) -> SupervisorDecision {
    let role = if record.source.instance_id == local_instance_id {
        RuntimeRole::Source
    } else if record.target.instance_id == local_instance_id {
        RuntimeRole::Target
    } else {
        RuntimeRole::Unrelated
    };

    let action = match (role, record.phase) {
        (_, TransferPhase::NeedsAttention) => SupervisorAction::NeedsAttention,
        (_, TransferPhase::Cancelled | TransferPhase::Completed) => SupervisorAction::Idle,
        (RuntimeRole::Unrelated, _) => SupervisorAction::Idle,
        (RuntimeRole::Source, TransferPhase::Preparing) => SupervisorAction::DrainSource,
        (RuntimeRole::Source, TransferPhase::Draining) => SupervisorAction::DrainSource,
        (RuntimeRole::Source, TransferPhase::SourceQuiesced) => {
            SupervisorAction::GrantTargetActivation
        }
        (RuntimeRole::Source, TransferPhase::Activating | TransferPhase::Verifying) => {
            SupervisorAction::Idle
        }
        (RuntimeRole::Target, TransferPhase::Preparing | TransferPhase::Draining) => {
            SupervisorAction::WaitForSource
        }
        (RuntimeRole::Target, TransferPhase::SourceQuiesced) => SupervisorAction::WaitForActivation,
        (RuntimeRole::Target, TransferPhase::Activating) => SupervisorAction::ActivateTarget,
        (RuntimeRole::Target, TransferPhase::Verifying) => SupervisorAction::VerifyTarget,
    };

    SupervisorDecision { role, action }
}

/// Runtime facts needed before an executor may report a fenced transition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RuntimeObservation {
    /// Number of active turns and child activities still owned by this runtime.
    pub active_work: u32,
    /// Whether this runtime still accepts new work.
    pub accepting_work: bool,
    /// Whether the target runtime has started and passed its local readiness checks.
    pub ready: bool,
}

/// Select the next state-machine command that this runtime may report.
///
/// This is deliberately pure. The caller must provide a fresh transfer snapshot
/// and local observation, then send the returned command through the signed relay
/// coordinator. Returning `None` means the runtime must wait; it must not guess
/// or report readiness without the required local evidence.
pub(crate) fn command_for(
    record: &TransferRecord,
    local_instance_id: &str,
    observation: RuntimeObservation,
) -> Option<buzz_core::agent_transfer::TransferCommand> {
    let decision = decide(record, local_instance_id);
    match (decision.role, record.phase, decision.action) {
        (RuntimeRole::Source, TransferPhase::Preparing, SupervisorAction::DrainSource) => {
            Some(buzz_core::agent_transfer::TransferCommand::BeginDrain)
        }
        (RuntimeRole::Source, TransferPhase::Draining, SupervisorAction::DrainSource)
            if !observation.accepting_work && observation.active_work == 0 =>
        {
            Some(buzz_core::agent_transfer::TransferCommand::ConfirmSourceQuiesced)
        }
        (
            RuntimeRole::Source,
            TransferPhase::SourceQuiesced,
            SupervisorAction::GrantTargetActivation,
        ) => Some(buzz_core::agent_transfer::TransferCommand::ActivateTarget),
        (RuntimeRole::Target, TransferPhase::Activating, SupervisorAction::ActivateTarget)
            if observation.ready =>
        {
            Some(buzz_core::agent_transfer::TransferCommand::ConfirmTarget)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::agent_transfer::{Executor, TransferRecord};

    fn record() -> TransferRecord {
        TransferRecord::new(
            "community",
            "agent",
            "operation",
            Executor::new("source-1", "server").expect("source"),
            Executor::new("target-1", "computer").expect("target"),
            1,
        )
        .expect("record")
    }

    #[test]
    fn source_starts_with_drain_intent() {
        let decision = decide(&record(), "source-1");
        assert_eq!(decision.role, RuntimeRole::Source);
        assert_eq!(decision.action, SupervisorAction::DrainSource);
    }

    #[test]
    fn target_waits_then_activates_and_verifies() {
        let mut transfer = record();
        assert_eq!(
            decide(&transfer, "target-1").action,
            SupervisorAction::WaitForSource
        );
        transfer.phase = TransferPhase::Activating;
        assert_eq!(
            decide(&transfer, "target-1").action,
            SupervisorAction::ActivateTarget
        );
        transfer.phase = TransferPhase::Verifying;
        assert_eq!(
            decide(&transfer, "target-1").action,
            SupervisorAction::VerifyTarget
        );
    }

    #[test]
    fn source_grants_target_after_quiescence() {
        let mut transfer = record();
        transfer.phase = TransferPhase::Draining;
        assert_eq!(
            command_for(
                &transfer,
                "source-1",
                RuntimeObservation {
                    active_work: 1,
                    accepting_work: false,
                    ready: false,
                },
            ),
            None
        );
        assert_eq!(
            command_for(
                &transfer,
                "source-1",
                RuntimeObservation {
                    active_work: 0,
                    accepting_work: false,
                    ready: false,
                },
            ),
            Some(buzz_core::agent_transfer::TransferCommand::ConfirmSourceQuiesced)
        );
        transfer.phase = TransferPhase::SourceQuiesced;
        assert_eq!(
            command_for(&transfer, "source-1", RuntimeObservation::default()),
            Some(buzz_core::agent_transfer::TransferCommand::ActivateTarget)
        );
    }

    #[test]
    fn target_confirms_only_after_local_readiness() {
        let mut transfer = record();
        transfer.phase = TransferPhase::Activating;
        assert_eq!(
            command_for(
                &transfer,
                "target-1",
                RuntimeObservation {
                    ready: false,
                    ..RuntimeObservation::default()
                },
            ),
            None
        );
        assert_eq!(
            command_for(
                &transfer,
                "target-1",
                RuntimeObservation {
                    ready: true,
                    ..RuntimeObservation::default()
                },
            ),
            Some(buzz_core::agent_transfer::TransferCommand::ConfirmTarget)
        );
    }

    #[test]
    fn recovery_phase_blocks_automation_for_both_executors() {
        let mut transfer = record();
        transfer.phase = TransferPhase::NeedsAttention;
        transfer.attention_reason = Some("operator review".into());
        assert_eq!(
            decide(&transfer, "source-1").action,
            SupervisorAction::NeedsAttention
        );
        assert_eq!(
            decide(&transfer, "target-1").action,
            SupervisorAction::NeedsAttention
        );
    }

    #[test]
    fn unrelated_instance_is_idle() {
        let decision = decide(&record(), "unknown");
        assert_eq!(decision.role, RuntimeRole::Unrelated);
        assert_eq!(decision.action, SupervisorAction::Idle);
    }
}
