# xmip-test-playground

**The Xmip Playground.** The tool that exercises Xmip — every transport, every
content contract, all the time.

It spawns Development nodes as processes on one machine and drives the message
path through them: Receive Locations fed with generated Streams, Send Locations
watched for what comes out, and a verdict per (transport, contract) pair
published as health, so the pair that breaks turns red on the same page as
everything else. No virtualisation: the operating system isolates processes,
and Development is a node role, not a test harness.

Xmip's own transports are the counterparty. `xmip-core-transport` carries http,
smtp, tcp, udp and file as both server and client, so a Receive Location is fed
by an Xmip Send Location and nothing external is stood up.

It is also where the numbers come from. Streams in, Journeys through, Messages
out — per stage, per pair — into the snapshot the operator boundary reads.
Until a Playground runs, a throughput card on the GUI shows a dash.

## State

Created 2026-09-05 from the Rust template; ADR-0028 records the decision and
terminology.md the word. It holds no code yet beyond its own description.

The runtime performs startup phases 1–3 today — read, build, validate — and
nothing after. The first Playground therefore spawns nodes that plan and
validate, and grows with the runtime as phases 4–9 land.
