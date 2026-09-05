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
//! What it can do grows with the runtime and the transports. Created
//! 2026-09-05; named by the owner.

pub mod contracts;
pub mod pingpong;
pub mod report;
pub mod roundtrip;
pub mod schedule;
pub mod verdict;

pub use contracts::{ContentContract, Shape};
pub use pingpong::ping_pong;
pub use report::{history_toml, to_toml, write_atomic};
pub use roundtrip::{
    Exchange, FileRoundTrip, HttpRoundTrip, RoundTrip, SmtpRoundTrip, TcpRoundTrip, UdpRoundTrip,
    WebSocketRoundTrip,
};
pub use schedule::{CONTRACTS, Schedule};
pub use verdict::{Contract, Outcome, Verdict};
