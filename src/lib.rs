#![forbid(unsafe_code)]

//! The Xmip Playground: the tool that exercises Xmip. ADR-0028.
//!
//! It runs the **pingpong test** — one integration test, over time, across the
//! whole estate: not a test per protocol or per contract but a single test
//! whose subject is every transport by every content contract at once. On a
//! Schedule it sends an actual Stream, catches it, checks it came back whole and
//! that the content contract holds over it — delivered means both — for every
//! pair, and folds each round into a running tally. Each pair is a leaf
//! of the one test and rolls up to a single state at
//! `xmip:///<node>/exercise`, so an operator sees one green — or the one pair
//! that broke. What it publishes is the record over time, not the last round;
//! one failure among thousands stays visible until a round passes again.
//! Receive and Send are the two ends each round drives; the transport is what
//! varies under the test.
//!
//! Xmip's own transports are the far end, so nothing external is stood up and
//! the Playground runs on a laptop with no network. Every implemented transport
//! ping-pongs today — file over a directory, tcp/http/smtp/websocket over a
//! loopback connection, udp over a loopback datagram — each behind one
//! [`RoundTrip`] adapter, so the scenario is one thing over all of them. A
//! transport declared but not yet implemented is a new adapter away, not a new
//! test.
//!
//! It runs more than one scenario over those adapters, each a different question
//! asked of the same estate, published under its own subtree of
//! `xmip:///playground`:
//!
//!   - **pingpong** — did it arrive whole and hold its contract, across the
//!     message-path stages, and does Receive run the identity pipeline and Send
//!     present identity (ADR-0019).
//!   - **furious** — did it arrive in time: round-trip latency against a budget,
//!     judged on p50/p99 over recent rounds.
//!   - **load** — a megabyte per pair: did it arrive byte-for-byte and still
//!     validate at size, and how fast.
//!   - **secretary** — retention and archiving: keep, archive and purge by age,
//!     driving the real retention policy and archive store.
//!
//! What it can do grows with the runtime and the transports. Created
//! 2026-09-05; named by the owner.

pub mod claim;
pub mod contracts;
pub mod fault;
pub mod furious;
pub mod identity;
pub mod load;
pub mod pingpong;
pub mod report;
pub mod roundtrip;
pub mod schedule;
pub mod secretary;
pub mod standing;
pub mod verdict;

pub use claim::Claim;
pub use contracts::{ContentContract, Shape};
pub use fault::{Fault, FaultKind, FaultPlan};
pub use furious::Furious;
pub use identity::{IdentityFaults, Step};
pub use load::Load;
pub use pingpong::ping_pong;
pub use report::{activity_toml, history_toml, to_toml, write_atomic};
pub use roundtrip::{
    Exchange, FileRoundTrip, HttpRoundTrip, RoundTrip, SmtpRoundTrip, TcpRoundTrip, UdpRoundTrip,
    WebSocketRoundTrip,
};
pub use schedule::{CONTRACTS, Schedule};
pub use secretary::Secretary;
pub use verdict::{Contract, Outcome, Stage, Verdict};
