# xmip-test-playground

**The Xmip Playground.** One integration test — the **pingpong test** — over
the whole estate, over time.

Its scenario is a round trip: send a payload, catch it, check it came back
whole. It runs that over every transport by every content contract, on a
Schedule, and never stops. Each round folds into a running tally per pair, so a
pair is judged by its record over time — one failure among thousands stays
visible until a round passes again. Every pair rolls up to one state at
`xmip:///<node>/exercise`, so an operator sees one green or the one pair that
broke. ADR-0028.

Ping-pong is the scenario, not a protocol; the transport is what varies under
it. Xmip's own transports are both ends, so nothing external is stood up.

## State

Created 2026-09-05. The file transport runs today — self-contained, no port to
coordinate — over the bytes and text contracts, with the tally and the health
roll-up in place. The socket transports (tcp, udp, http, smtp) join as the
scenario learns each one's bind-and-accept; a transport that supports only one
direction is reported yellow, exercised as far as its one side allows, per
ADR-0028 clause 5.

`Schedule::tick()` runs one round and returns the snapshot to publish; the
running thread belongs to whatever hosts it.
