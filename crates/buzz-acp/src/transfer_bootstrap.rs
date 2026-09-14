//! Validation for a transfer event supplied by the local process supervisor.
//!
//! A target ACP process can be started after the relay has already accepted an
//! owner-start event. The desktop supervisor passes that event through a
//! reserved environment variable; this module keeps the bootstrap boundary
//! just as strict as the live relay boundary before the event enters the
//! normal observer-control queue.

use buzz_core::agent_transfer::TRANSFER_COORDINATOR_EVENT_KIND;
use nostr::Event;

/// Parse and authenticate one desktop-supplied transfer event.
pub(crate) fn parse_event(raw: &str, agent_pubkey_hex: &str) -> Result<Event, String> {
    let event: Event = serde_json::from_str(raw)
        .map_err(|error| format!("transfer bootstrap event is invalid JSON: {error}"))?;
    if event.kind.as_u16() as u32 != TRANSFER_COORDINATOR_EVENT_KIND {
        return Err("transfer bootstrap event has the wrong kind".into());
    }
    buzz_core::verify_event(&event)
        .map_err(|error| format!("transfer bootstrap event signature is invalid: {error}"))?;

    let mut agent = None;
    for tag in event.tags.iter() {
        let values = tag.as_slice();
        match values.first().map(String::as_str) {
            Some("agent") if values.len() == 2 && agent.is_none() => {
                agent = Some(values[1].clone());
            }
            Some("p") if values.len() == 2 => {}
            _ => return Err("transfer bootstrap event has invalid routing tags".into()),
        }
    }
    if event.tags.len() != 2 || agent.as_deref() != Some(agent_pubkey_hex) {
        return Err("transfer bootstrap event is not addressed to this agent".into());
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::agent_transfer::{
        Executor, TransferCoordinatorEnvelope, TransferCoordinatorMessage, TransferOwnerRequest,
        TransferRecord,
    };
    use nostr::{EventBuilder, Keys, Tag};

    fn event(agent: &Keys, owner: &Keys) -> Event {
        let record = TransferRecord::new(
            "community",
            agent.public_key().to_hex(),
            "operation",
            Executor::new("source", "server").unwrap(),
            Executor::new("target", "computer").unwrap(),
            1,
        )
        .unwrap();
        let content = TransferCoordinatorEnvelope::new(
            "message",
            TransferCoordinatorMessage::OwnerRequest {
                request: TransferOwnerRequest::Start {
                    transfer: Box::new(record),
                },
            },
        )
        .unwrap()
        .to_json()
        .unwrap();
        EventBuilder::new(
            nostr::Kind::Custom(TRANSFER_COORDINATOR_EVENT_KIND as u16),
            content,
        )
        .tags([
            Tag::parse(["p", &owner.public_key().to_hex()]).unwrap(),
            Tag::parse(["agent", &agent.public_key().to_hex()]).unwrap(),
        ])
        .allow_self_tagging()
        .sign_with_keys(owner)
        .unwrap()
    }

    #[test]
    fn accepts_signed_event_for_target_agent() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let parsed = parse_event(
            &serde_json::to_string(&event(&agent, &owner)).unwrap(),
            &agent.public_key().to_hex(),
        );
        assert!(parsed.is_ok());
    }

    #[test]
    fn rejects_event_for_another_agent() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let other = Keys::generate();
        let parsed = parse_event(
            &serde_json::to_string(&event(&agent, &owner)).unwrap(),
            &other.public_key().to_hex(),
        );
        assert!(parsed.is_err());
    }

    #[test]
    fn rejects_extra_routing_tag() {
        let agent = Keys::generate();
        let owner = Keys::generate();
        let mut signed = event(&agent, &owner);
        signed.tags.push(Tag::parse(["extra", "value"]).unwrap());
        let parsed = parse_event(
            &serde_json::to_string(&signed).unwrap(),
            &agent.public_key().to_hex(),
        );
        assert!(parsed.is_err());
    }
}
