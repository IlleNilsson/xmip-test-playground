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

## Running it

Nothing here starts on its own. Xmip provides its tests as suites, and the
Playground is the first: a person starts a run of it (a roll), a set of
emulated nodes or the web monitor, sees what is running, and stops it — Start,
Get and Stop for each, from the estate's PowerShell module. `-Suite Playground`
is the default while it is the only suite; a transport's or a contract's own
suite joins as another value:

    Import-Module ./Xmip/Xmip.psd1

    Start-XmipTest                                        # Playground, realistic, every test, until stopped
    Start-XmipTest -Suite Playground -Test HeavyLoad, LowLatency -Stress Harsh -Rounds 20
    Start-XmipTest -Stress Brutal -Nodes 20 -OnlineNodes 5 -PassThru | Start-XmipWeb
    Start-XmipTest -Duration 00:15:00 -TimeFactor 9.5e-6  # three simulated years

    Get-XmipTestStatus                                    # what runs, at what, how it stands
    Get-XmipTestResult | Where-Object State -ne fine      # every scope that is not green
    Get-XmipTestResult -Test RoundTrip -Worst
    Get-XmipHistory -Counted bytes

    Start-XmipTestNode -Count 5 -OnlineNodes 2            # five emulated nodes, two online, no roll
    Get-XmipTestNode
    Get-XmipTestNode | Where-Object Online | Stop-XmipTestNode

    Get-XmipWeb                                           # the monitor's address and surface
    Stop-XmipTest                                         # nodes first, then the roll
    Stop-XmipWeb

    Start-XmipTest -Suite Estate                          # the estate's Pester suite, here and now
    Start-XmipTest -Suite Estate -Test Rust.Style, XmipTest

The Playground's tests, by the name a person asks for and the scenario the roll
drives: RoundTrip is pingpong, LowLatency is furious, HeavyLoad is load,
Retention is secretary, Filing is filing, ExclusiveClaim is claim, DailyBacklog
is daily. `-Test` tab-completes them, and the estate's Pester files when the
suite is Estate.

Every Start and Stop takes `-WhatIf`. A roll's switches reach it through its
own environment, never yours: `-Stress` is `XMIP_PLAYGROUND_STRESS`
(`calm`, `realistic`, `harsh`, `brutal`), `-Test` is
`XMIP_PLAYGROUND_SCENARIOS` (the scenario names above; unset means all),
`-Nodes` is `XMIP_PLAYGROUND_NODES` (0 for no fleet), `-OnlineNodes` is
`XMIP_PLAYGROUND_ONLINE_NODES` (the first that many nodes may assume the
internet, ADR-0045; unset, every node reads `XMIP_ONLINE`), `-Duration` is
`XMIP_PLAYGROUND_MAX_SECONDS`, `-TimeFactor` is `XMIP_PLAYGROUND_TIME_FACTOR`
and `-LoadBytes` is `XMIP_PLAYGROUND_LOAD_BYTES`. A roll started by hand —
`cargo run --bin roll [rounds]` with those variables set — is the same roll,
and `Get-XmipTestStatus` lists it too.

Everything a run writes on this machine goes under `.local-work/playground`
at the repository root: `playground-snapshot.toml`, `playground-history.toml`
and `playground-activity.toml` for the monitors, `roll-<pid>.toml` saying what
each roll was started with, the roll's own lines in `roll-<start time>.log`,
and under `node/` each hand-started node's snapshot and log. The folder is
device-local and ignored by git.
