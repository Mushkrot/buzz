//! Owner-signed request to move one managed identity between prepared executors.

use buzz_core::agent_transfer::{
    Executor, TransferCoordinatorEnvelope, TransferCoordinatorMessage, TransferOwnerRequest,
    TransferRecord, TransferWireResponse,
};
use buzz_sdk::builders::build_transfer_coordinator_event;
use nostr::{Event, Keys};
use serde_json::json;

use crate::client::BuzzClient;
use crate::error::CliError;
use crate::validate::{validate_hex64, validate_uuid};
use crate::TransferStartArgs;

pub(super) async fn start(client: &BuzzClient, args: &TransferStartArgs) -> Result<(), CliError> {
    let event = build_owner_start(client.keys(), args)?;
    let event_id = event.id.to_hex();
    let raw = client.publish_ephemeral_event(event).await?;
    let response: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|error| CliError::Other(format!("invalid transfer response: {error}")))?;
    let message = response
        .get("message")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| CliError::Other("relay omitted transfer result".into()))?;
    let accepted: TransferWireResponse = serde_json::from_str(message)
        .map_err(|error| CliError::Other(format!("invalid transfer result: {error}")))?;
    match accepted {
        TransferWireResponse::Accepted { transfer } => {
            println!(
                "{}",
                json!({
                    "accepted": true,
                    "event_id": event_id,
                    "operation_id": transfer.operation_id,
                    "phase": transfer.phase,
                    "source_instance": transfer.source.instance_id,
                    "target_instance": transfer.target.instance_id,
                })
            );
            Ok(())
        }
        TransferWireResponse::Rejected { error } => Err(CliError::Relay {
            status: 400,
            body: error.detail,
        }),
        _ => Err(CliError::Other(
            "relay returned an unexpected transfer result".into(),
        )),
    }
}

fn build_owner_start(owner: &Keys, args: &TransferStartArgs) -> Result<Event, CliError> {
    validate_uuid(&args.community_id)?;
    validate_hex64(&args.agent_pubkey)?;
    if args.agent_pubkey != args.agent_pubkey.to_ascii_lowercase() {
        return Err(CliError::Usage("agent pubkey must be lowercase hex".into()));
    }
    let source = Executor::new(&args.source_instance, &args.source_location)
        .map_err(|error| CliError::Usage(format!("invalid source executor: {error}")))?;
    let target = Executor::new(&args.target_instance, &args.target_location)
        .map_err(|error| CliError::Usage(format!("invalid target executor: {error}")))?;
    let transfer = TransferRecord::new(
        &args.community_id,
        &args.agent_pubkey,
        &args.operation_id,
        source,
        target,
        args.config_revision,
    )
    .map_err(|error| CliError::Usage(format!("invalid transfer: {error}")))?;
    let envelope = TransferCoordinatorEnvelope::new(
        args.operation_id.clone(),
        TransferCoordinatorMessage::OwnerRequest {
            request: TransferOwnerRequest::Start {
                transfer: Box::new(transfer),
            },
        },
    )
    .map_err(|error| CliError::Usage(format!("invalid transfer request: {error}")))?;
    build_transfer_coordinator_event(&owner.public_key().to_hex(), &args.agent_pubkey, &envelope)
        .map_err(|error| CliError::Usage(format!("invalid transfer event: {error}")))?
        .sign_with_keys(owner)
        .map_err(|error| CliError::Other(format!("cannot sign transfer request: {error}")))
}

#[cfg(test)]
mod tests {
    use buzz_core::agent_transfer::{
        TransferCoordinatorEnvelope, TransferCoordinatorMessage, TransferOwnerRequest,
        TRANSFER_COORDINATOR_EVENT_KIND,
    };
    use nostr::Keys;

    use super::*;

    fn args(agent: &Keys) -> crate::TransferStartArgs {
        crate::TransferStartArgs {
            community_id: "b487f805-9cde-4d29-b8dc-54d09cbfedc0".into(),
            agent_pubkey: agent.public_key().to_hex(),
            operation_id: "transfer-test-4271b4b0-dc31-4f6f-ab85-6ccbad9c3421".into(),
            source_instance: "desktop.example".into(),
            target_instance: "linux.example".into(),
            source_location: "computer".into(),
            target_location: "server".into(),
            config_revision: 1,
        }
    }

    #[test]
    fn signs_canonical_owner_start_for_selected_agent_and_target() {
        let owner = Keys::generate();
        let agent = Keys::generate();
        let event = build_owner_start(&owner, &args(&agent)).expect("build transfer start");
        buzz_core::verify_event(&event).expect("signed event");
        assert_eq!(event.kind.as_u16() as u32, TRANSFER_COORDINATOR_EVENT_KIND);
        assert_eq!(event.pubkey, owner.public_key());
        let envelope = TransferCoordinatorEnvelope::from_json(&event.content).unwrap();
        let TransferCoordinatorMessage::OwnerRequest {
            request: TransferOwnerRequest::Start { transfer },
        } = envelope.message
        else {
            panic!("expected owner start");
        };
        assert_eq!(transfer.agent_pubkey, agent.public_key().to_hex());
        assert_eq!(transfer.source.instance_id, "desktop.example");
        assert_eq!(transfer.target.instance_id, "linux.example");
    }

    #[test]
    fn refuses_same_source_and_target_instance() {
        let owner = Keys::generate();
        let agent = Keys::generate();
        let mut input = args(&agent);
        input.target_instance = input.source_instance.clone();
        assert!(build_owner_start(&owner, &input).is_err());
    }

    #[test]
    fn transfer_start_cli_requires_explicit_coordinates() {
        use clap::Parser;

        let parsed = crate::Cli::try_parse_from([
            "buzz",
            "agents",
            "transfer-start",
            "--community-id",
            "b487f805-9cde-4d29-b8dc-54d09cbfedc0",
            "--agent-pubkey",
            &"a".repeat(64),
            "--operation-id",
            "transfer-test-4271b4b0-dc31-4f6f-ab85-6ccbad9c3421",
            "--source-instance",
            "desktop.example",
            "--target-instance",
            "linux.example",
            "--source-location",
            "computer",
            "--target-location",
            "server",
            "--config-revision",
            "1",
        ])
        .expect("parse transfer-start command");
        assert!(matches!(
            parsed.command,
            crate::Cmd::Agents(crate::AgentsCmd::TransferStart(_))
        ));
    }
}
