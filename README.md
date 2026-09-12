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

## The scenarios

Moved here from ADR-0028 on 2026-09-12; the record keeps the decisions.


Pingpong is the first scenario, not the only one. Each scenario asks a different
question of the same estate over the same [`RoundTrip`] adapters, and publishes
under its own subtree of `xmip:///playground`, merged into one snapshot so the
rollup covers them all and an operator drills scenario → detail → the failing
leaf. Named in the shortest singular form, the owner's convention:

- **pingpong** — did it arrive whole and hold its contract, across the stages.
- **furious** — did it arrive in time: round-trip latency against a per-transport
  budget, judged on the p50/p99 of recent rounds (cold-start rounds skipped).
- **load** — a large payload per pair (a megabyte by default, **gigabytes** on
  demand): did it arrive byte-for-byte and, below a parse ceiling, still validate
  at size; and at what throughput. Above the ceiling the structural contract is
  not parsed — a gigabyte parse allocates a second copy and proves nothing the
  byte check does not — so the claim is byte integrity at scale. A UDP datagram
  cannot hold a megabyte, and that real ceiling shows as red with no injection.
  Peak memory is roughly twice the size per pair, pairs run one at a time; true
  multi-gigabyte without that doubling wants a streaming round trip, queued.
- **secretary** — retention and archiving: retain, then archive by age, driving
  the estate's real `RetentionPolicy` and `ArchiveStore` over a logical clock; a
  missed sweep under pressure surfaces as a retention leak. There is no third
  act — Xmip retains and archives, it does not delete (ADR-0040).
- **filing** — *added 2026-09-09.* Does the archive hold what it was handed:
  one probe item per contract filed through every archive technology on main
  — parquet, sqlite, file, sql, postgresql, mssql, mysql, s3, azure-blob, gcs —
  archived and restored, judged equal or not, under
  `xmip:///playground/filing/<technology>/<contract>`. The secretary proves the
  lifecycle over one store; filing proves every store. Each technology gets a
  `Cabinet` adapter the way each transport gets a `RoundTrip`, so a new archive
  technology is a new adapter, not a new scenario. Under pressure a filing is
  skipped now and then and reported as a fault.
- **storm** — *added 2026-09-09.* Every transport by every contract at a stress
  level, many pairs at once, harsh faults, the level's payloads cycling by
  round — and its subject is the invariants that must survive that: a tick
  finishes within its bound, every failure carries a reason, a red leaf
  reaches the root, nothing panics. Pingpong proves the pair; storm proves the
  playground and the estate under it do not lie when leaned on.
- **claim** — exclusive pickup: a dropped item is read by exactly one holder,
  under real thread contention, across the **execution style** it declares —
  Sequential, Parallel, Concurrent (runtime-model.md). Sequential additionally
  keeps order per key. The claim is the estate's (ADR-0024), taken by atomically
  creating a lock (`create_new`/`O_EXCL`, which is exclusive under concurrency
  where a rename to a per-reader name is not). Under pressure the atomic claim is
  dropped and a second reader takes the same item — a `Contended` red, the
  duplicate-pickup bug. It runs over the file substrate; **no protocol is named
  in the code** (the owner's rule) — any other pollable transport joins by adding
  a `RoundTrip` adapter, which is when SFTP, FTPS and FTP get this exercise.
- **daily** — a day's backlog drained as fast as possible: many files arrive at
  once and a node clears what its capacity allows. When arrivals outpace it the
  backlog climbs and the scenario escalates as an operator would — first a
  **tweak** (raise the node's concurrency), then, if that only slows the rise,
  **add a node** to share the backlog through the claim. The board shows the
  backlog climb, names the action taken, and shows it fall. The backlog is real
  files on disk, so the queue depth is a real count.

Each injects its own pressure (faults, latency spikes, dropped transfers, missed
sweeps, dropped claims) so the board is realistic rather than uniformly green;
`file` is left clean in every one.

## Two time limits bound any roll

Every roll honors two limits, one `Budget` shared by all scenarios rather than
per-scenario knobs. A **maximum time** is a wall-clock ceiling: when it is
reached the roll stops, whatever the round count — `XMIP_PLAYGROUND_MAX_SECONDS`.
The ceiling is checked between rounds, so a long tick runs to completion rather
than being cut mid-round; the maximum bounds how long a run lasts, not how long
a single round takes.

A **factor on time** stretches a **simulated clock** against real time:
`1.0` mimics real time — one simulated second per real second — and *retracting*
it below one runs simulated time faster than real, so a long horizon plays out in
a short run. Fifteen real minutes over three simulated years is `MAX_SECONDS=900`
with `TIME_FACTOR ≈ 9.5e-6` (900 real seconds ÷ three years) —
`XMIP_PLAYGROUND_TIME_FACTOR`. The round cadence stays real; the factor stretches
*simulated* time, not the wait between rounds. Scenarios that age on a clock —
the secretary's retention lifecycle, retained 90 days then archived — read
simulated elapsed from the `Budget`, so an operator watches records born, live
out their retention, and cross into the archive over a horizon far longer than
the run. Rate- and latency-based scenarios ignore it; they answer in real time.

## Difficulty

### The stress level


Loopback never fails, one payload never surprises, one round at a time never
contends. A **stress level** — `calm`, `realistic`, `harsh`, `brutal` — turns
each of those up together, and every scenario takes the level rather than its
own idea of hard: the fault rates (none, as written, tripled, at the ceiling of
nine rounds in ten), the payload sizes (a few bytes; then the sizes protocols
break on — a datagram's MTU either side, the UDP maximum, sixty-four kibibytes
plus one, a mebibyte), how many pairs run at once (one, one, four, every core),
how many rounds a test drives, and how many node processes a fleet spawns (one,
three, ten, forty). `realistic` is what the runner ran at before the axis
existed and is the default, so nothing changed quietly; `XMIP_PLAYGROUND_STRESS`
sets it for a roll.

### Every transport declares its ceiling

A `RoundTrip` adapter answers `ceiling()`: the largest payload its protocol
carries whole in one round, or none. Every adapter is judged on the edge
payloads — empty, one byte, every byte value, a NUL run, high bytes, a CRLF
storm, and the sizes above filled with a pattern a truncation or reorder would
show: under the ceiling they come back whole, above it the adapter refuses with
a reason, and no round exceeds three times the timeout. A ceiling is a fact
about the protocol, written where it comes from; it is never set to make a test
pass.

### Tests at every level

Every scenario keeps its calm tests and gains a `harsh` test over three
transports for the level's rounds, asserting the scenario's own invariant under
faults, contention and the edge payloads, and an ignored `brutal` test over the
whole matrix for the runner to fire. The default suite stays a suite — minutes,
not hours; the brutal runs are what a roll is for.

### The tests use half of what is free when they start, 2026-09-11

The stress level sized `brutal` to every core and to forty node processes,
`harsh` to ten. The owner ruled on 2026-09-11, watching a brutal roll: the
tests may use half of the resources left when they start. A machine a quarter
busy has three quarters free; the Playground takes half of that, three eighths
of the machine, and the other half of what was free stays with whatever else
the machine is doing — and that other work moves, so the measure is taken
again before every round. `headroom.rs` reads processor time less what the
roll and its fleet burn themselves — the Windows performance counters,
`/proc/stat` on Linux, free assumed elsewhere — and every count that would
take the whole machine is scaled to that budget, never below one: `brutal`
drives pairs from the budgeted cores and spawns forty nodes' worth of budget,
`harsh` four cores and ten nodes' worth. The fleet is sized when it is
spawned; the pairs follow the budget round by round, and the roll prints the
budget beside each round.

### The far end moved into the transport, 2026-09-11

Clause 5 and *Every transport declares its ceiling* above are read through
ADR-0051 since 2026-09-11: the dance that makes a transport its own far end,
its ceiling and its refusals are written in the technology's crate as the
capability's `Loopback`, and the Playground drives every transport through
one adapter over it. "A new transport is a new adapter, not a new scenario"
becomes "a new transport is its own loopback, and a line in the list".

### Nodes are processes, now

Decision 2 said nodes run as System Processes, and until this day no scenario
spawned one. The **fleet** does: `node`, a second binary, is one emulated node
that runs the claim and daily scenarios over a directory the whole fleet shares
and publishes its own snapshot under `xmip:///playground/node/<name>`; the
fleet spawns the level's count of them, merges their snapshots each round, adds
the cluster rollup the surface owes (ADR-0027 decision 8), and kills and
restarts a node whose snapshot stops moving — a recorded yellow, never silent.
Exclusive pickup and backlog draining are thereby contended by real processes,
which is the property ADR-0024's claim exists to prove and a thread could only
imitate. `XMIP_PLAYGROUND_NODES` or a harsh or brutal level puts the fleet on
the board beside the in-process scenarios.



## Running it

Nothing here starts on its own. Xmip provides its tests as suites, and the
Playground is the first: a person starts a run of it (a roll), a set of
emulated nodes or the web monitor, sees what is running, and stops it — Start,
Get and Stop for each, from the estate's PowerShell module. `-Suite Playground`
is the default while it is the only suite; a transport's or a contract's own
suite joins as another value:

    Import-Module -Name ./Xmip/Xmip.psd1

    Start-XmipTest                                        # Playground, realistic, every test, until stopped
    Start-XmipTest -Suite Playground -Test HeavyLoad, LowLatency -Stress Harsh -Rounds 20
    Start-XmipTest -Suite Playground -Test HeavyLoad -Nodes alpha, beta, gamma -OnlineNodes alpha -PassThru | Start-XmipWeb
    Start-XmipTest -Suite Playground -Duration 00:15:00 -TimeFactor 9.5e-6  # three simulated years

    Get-XmipTestStatus                                    # what runs, at what, how it stands
    Get-XmipTestResult | Where-Object -Property State -NE -Value fine   # every scope that is not green
    Get-XmipTestResult -Test RoundTrip -Worst
    Get-XmipHistory -Counted bytes

    Start-XmipTestNode -Nodes alpha, beta -OnlineNodes alpha  # two emulated nodes, one online, no roll
    Get-XmipTestNode
    Get-XmipTestNode | Where-Object -Property Online -EQ -Value $true | Stop-XmipTestNode

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
`-Nodes` is `XMIP_PLAYGROUND_NODE_NAMES` (the nodes to simulate, by name, one
process each; an empty list is `XMIP_PLAYGROUND_NODES=0`, no fleet; omitted, the
level's own numbered fleet), `-OnlineNodes` is `XMIP_PLAYGROUND_ONLINE_NODES`
(which of them may assume the internet, by name, ADR-0045; unset, every node
reads `XMIP_ONLINE`), `-Duration` is
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
