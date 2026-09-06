//! The pingpong scenario: one round over one transport, judged.
//!
//! A scenario, not a protocol — it runs over any [`RoundTrip`] adapter, so the
//! transport is the variable and the scenario is the constant. Send the
//! contract's Stream, take what returned, check it matches and the contract
//! holds over it. ADR-0028.
//!
//! It returns the *base* outcome of the real exchange and how many bytes moved.
//! The [`Schedule`](crate::Schedule) expands that into a verdict per message-path
//! stage — Receive, Process, Send — and injects the faults a real integration
//! suffers, since loopback itself never fails.

use stream::Stream;
use xcore::StreamId;

use crate::roundtrip::{Exchange, RoundTrip};
use crate::verdict::{Contract, Outcome};

/// Run one pingpong round for one transport and one contract, and judge it.
///
/// The probe sends an actual Stream; on a clean round trip the arrived bytes are
/// rebuilt into a Stream and the real contract is run over it. Returns the base
/// outcome and the bytes that moved (zero unless delivered).
#[must_use]
pub fn ping_pong(transport: &dyn RoundTrip, contract: Contract) -> (Outcome, u64) {
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

    (outcome, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roundtrip::{FileRoundTrip, TcpRoundTrip};
    use crate::support::scratch;

    #[test]
    fn a_payload_that_makes_the_round_trip_over_file_is_delivered() {
        let dir = scratch("delivered");
        let (outcome, bytes) = ping_pong(&FileRoundTrip::new(&dir), Contract::Text);

        assert_eq!(outcome, Outcome::Delivered);
        assert_eq!(bytes, Contract::Text.payload().len() as u64);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_same_scenario_runs_over_tcp() {
        // The point of the RoundTrip adapter: one scenario, a different
        // transport underneath, no change here.
        let (outcome, _) = ping_pong(&TcpRoundTrip::new(), Contract::Bytes);

        assert_eq!(outcome, Outcome::Delivered);
    }
}
