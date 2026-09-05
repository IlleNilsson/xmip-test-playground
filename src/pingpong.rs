//! The ping-pong scenario: send a payload, catch it, check it came back whole.
//!
//! A test scenario, not a protocol — it runs over any [`Transport`]. Send the
//! contract's payload, receive, compare to what went out. The transport is the
//! variable; the scenario is the constant. ADR-0028.

use xmip_transport::Transport;

use crate::verdict::{Contract, Outcome, Verdict};

/// Run ping-pong once for one transport and one contract, over an endpoint the
/// caller has already stood up. The endpoint is the transport's own address —
/// a directory for file, a socket for tcp — so a probe over a self-contained
/// transport (send and receive to the same place) is one call.
///
/// A transport that cannot both send and receive is not exercised end to end;
/// the verdict says so and is yellow, not red. ADR-0028 clause 5.
#[must_use]
pub fn ping_pong<T: Transport>(transport: &T, contract: Contract, now: i64) -> Verdict {
    let name = transport.name().to_string();
    let directions = transport.directions();

    if !(directions.receives() && directions.sends()) {
        let side = if directions.sends() {
            "send"
        } else {
            "receive"
        };

        return Verdict {
            transport: name,
            contract,
            outcome: Outcome::OneSided(format!("{side} only; ping-pong needs both directions")),
            bytes: 0,
            observed_unix_nanos: now,
        };
    }

    let payload = contract.payload();
    let outcome = round_trip(transport, contract, &payload);
    let bytes = if matches!(outcome, Outcome::Delivered) {
        payload.len() as u64
    } else {
        0
    };

    Verdict {
        transport: name,
        contract,
        outcome,
        bytes,
        observed_unix_nanos: now,
    }
}

/// Send the payload, take what arrives, and judge it. The target is the
/// transport's own endpoint — for a self-contained transport that is where it
/// also receives, so the payload comes back to the same place.
fn round_trip<T: Transport>(transport: &T, contract: Contract, payload: &[u8]) -> Outcome {
    let target = format!("pingpong-{}", contract.name());

    if let Err(error) = transport.send(&target, payload) {
        return Outcome::Failed(format!("send failed: {error}"));
    }

    match transport.receive() {
        Ok(arrived) if arrived.is_empty() => {
            Outcome::Failed("sent, but nothing arrived".to_string())
        }
        Ok(arrived) => {
            if arrived.iter().any(|a| a.bytes == payload) {
                Outcome::Delivered
            } else {
                Outcome::Failed("what arrived did not match what was sent".to_string())
            }
        }
        Err(error) => Outcome::Failed(format!("receive failed: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmip_transport::FileTransport;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-pingpong-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn a_payload_that_makes_the_round_trip_is_delivered() {
        let dir = scratch("delivered");
        let transport = FileTransport::new(&dir);

        let verdict = ping_pong(&transport, Contract::Text, 1);

        assert_eq!(verdict.outcome, Outcome::Delivered);
        assert_eq!(verdict.bytes, Contract::Text.payload().len() as u64);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn binary_content_survives_the_round_trip_byte_for_byte() {
        // The bytes contract carries non-UTF-8 on purpose: a transport that
        // quietly mangles high bytes fails here rather than in production.
        let dir = scratch("bytes");
        let transport = FileTransport::new(&dir);

        let verdict = ping_pong(&transport, Contract::Bytes, 1);

        assert_eq!(verdict.outcome, Outcome::Delivered);
        std::fs::remove_dir_all(&dir).ok();
    }
}
