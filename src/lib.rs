#![forbid(unsafe_code)]

//! The Xmip Playground: the tool that exercises Xmip. ADR-0028.
//!
//! It spawns Development nodes as System Processes on one machine — no
//! virtualisation — and drives every transport and every content contract
//! through them, continuously: a Receive Location for each transport fed with
//! generated Streams for each contract, a Send Location watched for what
//! arrives, and a verdict per (transport, contract) pair published as health
//! on `xmip:///<node>/exercise/<transport>/<contract>`.
//!
//! Xmip's own transports are the far end of every exchange, so nothing
//! external has to be stood up, and the Playground runs on a laptop with no
//! network.
//!
//! What it can do grows with the runtime. ADR-0018 gives a node nine startup
//! phases and the runtime performs the first three today — read, build the
//! execution tree, validate — so the first Playground spawns nodes that plan
//! and validate, and says so. Phases four to nine are what it needs next.
//!
//! Created 2026-09-05. Named by the owner.
