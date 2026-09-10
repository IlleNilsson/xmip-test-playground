//! Difficulty: how hard the playground leans on the estate.
//!
//! Loopback never fails, one payload never surprises, one round at a time
//! never contends. A [`Stress`] level turns each of those up together — the
//! fault rates, the payloads, how many pairs run at once, how many rounds a
//! test drives, and how many node processes a fleet spawns — so a scenario
//! at `Harsh` finds what the same scenario at `Calm` proves works. The owner,
//! 2026-09-09: *incorporate higher difficulty, stress on all tests; we need
//! about 10-40 processes emulating nodes.*
//!
//! The level is one thing read once — `XMIP_PLAYGROUND_STRESS` for a roll,
//! a constant in a test — so a scenario asks the level for its numbers and
//! never carries its own idea of "hard". `Realistic` is what every scenario
//! ran at before the axis existed; it is the default so nothing changed
//! quietly.

use crate::verdict::Contract;

/// How hard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stress {
    /// No injected faults, small payloads, one pair at a time. What a test
    /// that proves a mechanism runs at.
    Calm,
    /// The rates and sizes the runner used before the axis existed: mostly
    /// green, faults surfacing over time.
    Realistic,
    /// Fault rates tripled, payloads at the sizes protocols break on, four
    /// pairs at once, ten node processes.
    Harsh,
    /// Every fault rate at its ceiling, every edge payload every round, as
    /// many pairs at once as the machine has cores, forty node processes.
    Brutal,
}

impl Stress {
    /// The level a roll runs at: `XMIP_PLAYGROUND_STRESS` as one of the four
    /// names, `realistic` when unset or unrecognised.
    #[must_use]
    pub fn from_env() -> Self {
        std::env::var("XMIP_PLAYGROUND_STRESS")
            .ok()
            .and_then(|raw| Self::parse(&raw))
            .unwrap_or(Self::Realistic)
    }

    /// The level a name means, case-insensitively.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "calm" => Some(Self::Calm),
            "realistic" => Some(Self::Realistic),
            "harsh" => Some(Self::Harsh),
            "brutal" => Some(Self::Brutal),
            _ => None,
        }
    }

    /// The level's name, as the board and a scope show it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Calm => "calm",
            Self::Realistic => "realistic",
            Self::Harsh => "harsh",
            Self::Brutal => "brutal",
        }
    }

    /// What every fault rate is multiplied by, saturating at [`MAX_RATE`].
    #[must_use]
    pub const fn fault_multiplier(self) -> u8 {
        match self {
            Self::Calm => 0,
            Self::Realistic => 1,
            Self::Harsh => 3,
            Self::Brutal => 10,
        }
    }

    /// How many pairs a schedule runs at once.
    #[must_use]
    pub fn workers(self) -> usize {
        match self {
            Self::Calm | Self::Realistic => 1,
            Self::Harsh => 4,
            Self::Brutal => std::thread::available_parallelism().map_or(8, usize::from),
        }
    }

    /// How many rounds a test at this level drives.
    #[must_use]
    pub const fn rounds(self) -> u64 {
        match self {
            Self::Calm => 1,
            Self::Realistic => 3,
            Self::Harsh => 12,
            Self::Brutal => 60,
        }
    }

    /// How many node processes a fleet spawns.
    #[must_use]
    pub const fn nodes(self) -> usize {
        match self {
            Self::Calm => 1,
            Self::Realistic => 3,
            Self::Harsh => 10,
            Self::Brutal => 40,
        }
    }

    /// The payload sizes a level cycles through, one per round, in bytes.
    /// The larger levels sit on the sizes protocols break on: a datagram's
    /// MTU either side, the UDP maximum, sixty-four kibibytes plus one where
    /// a sixteen-bit length rolls over, and a mebibyte.
    #[must_use]
    pub const fn sizes(self) -> &'static [usize] {
        match self {
            Self::Calm => &[64],
            Self::Realistic => &[64, 1_000, 3_000],
            Self::Harsh => &[0, 1, 1_471, 1_472, 1_473, 8_192, 65_507, 65_537],
            Self::Brutal => &[
                0,
                1,
                255,
                256,
                1_472,
                1_473,
                65_507,
                65_536,
                65_537,
                1 << 20,
            ],
        }
    }

    /// The size this level uses in `round`, cycling through [`Self::sizes`].
    #[must_use]
    pub fn size_for(self, round: u64) -> usize {
        let sizes = self.sizes();
        sizes[usize::try_from(round % sizes.len() as u64).unwrap_or(0)]
    }
}

/// The highest rate a fault rule reaches under any multiplier: nine rounds in
/// ten, so a pair is never red every round and the board still moves.
pub const MAX_RATE: u8 = 90;

/// A rate under a level: multiplied and capped.
#[must_use]
pub fn scaled_rate(rate: u8, stress: Stress) -> u8 {
    rate.saturating_mul(stress.fault_multiplier()).min(MAX_RATE)
}

/// The payloads that break protocols, by name: the empty one, a single byte,
/// every byte value in order, a run of NULs, high bytes only, a CRLF storm,
/// and the sizes in [`Stress::sizes`] filled with a pattern a truncation or a
/// reorder would show. `ceiling` drops those a transport declares it cannot
/// carry — the caller judges those separately, as a refusal with a reason.
#[must_use]
pub fn edge_payloads(ceiling: Option<usize>) -> Vec<(&'static str, Vec<u8>)> {
    let mut named: Vec<(&'static str, Vec<u8>)> = vec![
        ("empty", Vec::new()),
        ("one byte", vec![0x2a]),
        ("every byte", (0..=255).collect()),
        ("nul run", vec![0; 512]),
        ("high bytes", vec![0xff; 512]),
        ("crlf storm", b"\r\n".repeat(400)),
        ("mtu minus one", patterned(1_471)),
        ("mtu", patterned(1_472)),
        ("mtu plus one", patterned(1_473)),
        ("udp maximum", patterned(65_507)),
        ("sixteen bits plus one", patterned(65_537)),
        ("a mebibyte", patterned(1 << 20)),
    ];
    if let Some(limit) = ceiling {
        named.retain(|(_, bytes)| bytes.len() <= limit);
    }
    named
}

/// `len` bytes that a truncation, a reorder or a duplicate would change:
/// each byte is its offset folded, so no two neighbouring runs repeat.
#[must_use]
pub fn patterned(len: usize) -> Vec<u8> {
    (0..len)
        .map(|at| u8::try_from((at * 31 + at / 251) % 256).unwrap_or(0))
        .collect()
}

/// A payload of about `size` bytes that still holds `contract` — the load
/// scenario's large payloads at the level's sizes — so a hard round proves
/// the contract at size as well as the bytes.
#[must_use]
pub fn payload(contract: Contract, size: usize) -> Vec<u8> {
    if size == 0 {
        return contract.payload();
    }
    crate::load::large_payload(contract, size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_levels_order_and_name_themselves() {
        assert!(Stress::Calm < Stress::Realistic && Stress::Harsh < Stress::Brutal);
        assert_eq!(Stress::parse("HARSH"), Some(Stress::Harsh));
        assert_eq!(Stress::parse("nope"), None);
        assert_eq!(Stress::Brutal.name(), "brutal");
        assert_eq!(Stress::Harsh.nodes(), 10);
        assert_eq!(Stress::Brutal.nodes(), 40);
    }

    #[test]
    fn rates_scale_and_cap() {
        assert_eq!(scaled_rate(5, Stress::Calm), 0);
        assert_eq!(scaled_rate(5, Stress::Realistic), 5);
        assert_eq!(scaled_rate(5, Stress::Harsh), 15);
        assert_eq!(scaled_rate(50, Stress::Brutal), MAX_RATE);
    }

    #[test]
    fn sizes_cycle_by_round_and_edges_respect_a_ceiling() {
        assert_eq!(Stress::Harsh.size_for(0), 0);
        assert_eq!(Stress::Harsh.size_for(8), 0);
        assert_eq!(Stress::Harsh.size_for(3), 1_472);
        let all = edge_payloads(None);
        assert_eq!(all.len(), 12);
        let small = edge_payloads(Some(65_507));
        assert!(small.iter().all(|(_, bytes)| bytes.len() <= 65_507));
        assert_eq!(small.len(), 10);
        assert_ne!(patterned(300)[..150], patterned(300)[150..]);
    }
}
