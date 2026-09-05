//! The pingpong scenario: one round over one transport, judged.
//!
//! A scenario, not a protocol — it runs over any [`RoundTrip`] adapter, so the
//! transport is the variable and the scenario is the constant. Send the
//! contract's Stream, take what returned, check it matches and the contract
//! holds over it. ADR-0028.

use xmip_core::StreamId;
use xmip_stream::Stream;

use crate::roundtrip::{Exchange, RoundTrip};
use crate::verdict::{Contract, Outcome, Verdict};

/// Run one pingpong round for one transport and one contract, and judge it.
///
/// The probe sends an actual Stream; on a clean round trip the arrived bytes are
/// rebuilt into a Stream and the real contract is run over it. Delivered means
/// both: the bytes came back whole and the contract held.
#[must_use]
pub fn ping_pong(transport: &dyn RoundTrip, contract: Contract, now: i64) -> Verdict {
    let stream = contract.stream();
    let payload = stream.bytes().to_vec();

    let outcome = match transport.exchange(&payload) {
        Exchange::Returned(back) if back == payload => {
            // It round-tripped; now the contract must hold over what arrived.
            let arrived = Stream::new(
                StreamId::new(1),
                back,
                Some(contract.shape().representation().to_string()),
            );
            match contract.validate(&arrived) {
                Ok(()) => Outcome::Delivered,
                Err(why) => Outcome::Failed(format!("contract not held: {why}")),
            }
        }
        Exchange::Returned(_) => {
            Outcome::Failed("what came back did not match what was sent".to_string())
        }
        Exchange::OneSided(why) => Outcome::OneSided(why),
        Exchange::Failed(why) => Outcome::Failed(why),
    };

    let bytes = if matches!(outcome, Outcome::Delivered) {
        payload.len() as u64
    } else {
        0
    };

    Verdict {
        transport: transport.transport().to_string(),
        contract,
        outcome,
        bytes,
        observed_unix_nanos: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roundtrip::{FileRoundTrip, TcpRoundTrip};

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xmip-pingpong-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn a_payload_that_makes_the_round_trip_over_file_is_delivered() {
        let dir = scratch("delivered");
        let verdict = ping_pong(&FileRoundTrip::new(&dir), Contract::Text, 1);

        assert_eq!(verdict.outcome, Outcome::Delivered);
        assert_eq!(verdict.bytes, Contract::Text.payload().len() as u64);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_same_scenario_runs_over_tcp() {
        // The point of the RoundTrip adapter: one scenario, a different
        // transport underneath, no change here.
        let verdict = ping_pong(&TcpRoundTrip::new(), Contract::Bytes, 1);

        assert_eq!(verdict.transport, "tcp");
        assert_eq!(verdict.outcome, Outcome::Delivered);
    }
}
