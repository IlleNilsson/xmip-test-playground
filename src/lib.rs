#![forbid(unsafe_code)]

//! The Xmip Playground: the tool that exercises Xmip. ADR-0028.
//!
//! It runs the **pingpong test** — one integration test, over time, across the
//! whole estate: not a test per protocol or per contract but a single test
//! whose subject is every transport by every content contract at once. On a
//! Schedule it sends a payload, catches it and checks it came back whole, for
//! every pair, and folds each round into a running tally. Each pair is a leaf
//! of the one test and rolls up to a single state at
//! `xmip:///<node>/exercise`, so an operator sees one green — or the one pair
//! that broke. What it publishes is the record over time, not the last round;
//! one failure among thousands stays visible until a round passes again.
//! Receive and Send are the two ends each round drives; the transport is what
//! varies under the test.
//!
//! Xmip's own transports are the far end, so nothing external is stood up and
//! the Playground runs on a laptop with no network. Every implemented transport
//! ping-pongs today — file over a directory, tcp/http/smtp over a loopback
//! connection, udp over a loopback datagram — each behind one [`RoundTrip`]
//! adapter, so the scenario is one thing over all of them. A transport declared
//! but not yet implemented is a new adapter away, not a new test.
//!
//! What it can do grows with the runtime and the transports. Created
//! 2026-09-05; named by the owner.

pub mod pingpong;
pub mod roundtrip;
pub mod schedule;
pub mod verdict;

pub use pingpong::ping_pong;
pub use roundtrip::{
    Exchange, FileRoundTrip, HttpRoundTrip, RoundTrip, SmtpRoundTrip, TcpRoundTrip, UdpRoundTrip,
};
pub use schedule::{CONTRACTS, Schedule};
pub use verdict::{Contract, Outcome, Verdict};
