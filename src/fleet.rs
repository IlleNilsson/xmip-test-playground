//! The fleet: node processes emulating a cluster, spawned and watched.
//!
//! ADR-0028 clause 2: nodes run as System Processes, and a process that hangs
//! is killed and restarted like any other Host Service. Until 2026-09-09 no
//! scenario spawned one; the owner's requirement — *about 10-40 processes
//! emulating nodes* — is this. A [`Fleet`] starts [`Stress::nodes`] copies of
//! the `node` binary beside the current executable, each running the claim and
//! daily scenarios over one shared directory, so the contention is between
//! processes, and each publishing its own snapshot file.
//!
//! [`Fleet::tick`] is the surface's half of ADR-0027 decision 8: a node answers
//! for itself, and the cluster view is assembled by whoever asks each node.
//! Every node's latest file is read and merged — scopes are disjoint per node —
//! and the fleet adds what no node can say about itself: a health record per
//! node (alive, exited with its code, or hung) and the rollup at
//! `xmip:///playground/fleet`, worst of all. A node whose snapshot has not
//! changed for longer than three rounds is hung: it is killed and restarted,
//! and the restart is recorded as a fault — a yellow that stays for the fleet's
//! life — never silently.

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use observe::{Health, HealthRecord, Snapshot};

use crate::report::from_toml;
use crate::stress::Stress;
use crate::support::now_unix_nanos;

/// Where every playground scenario publishes.
pub const ROOT: &str = "xmip:///playground";
/// Rounds a node may stay silent before it is hung.
const SILENT_ROUNDS: u32 = 3;
/// How long a tick waits for every live node to publish something new before
/// it judges with what it has.
const GRACE: Duration = Duration::from_secs(2);
/// How long `stop` waits for the nodes to leave on their own.
const STOP_WAIT: Duration = Duration::from_secs(5);

/// The node processes, and what the fleet knows about each.
pub struct Fleet {
    binary: PathBuf,
    shared: PathBuf,
    stress: Stress,
    rounds: u64,
    nodes: Vec<Node>,
}

/// One node process and its published state.
struct Node {
    name: String,
    child: Option<Child>,
    exit: Option<ExitStatus>,
    path: PathBuf,
    text: String,
    published: Snapshot,
    fresh: bool,
    silent: u32,
    restarts: u32,
}

impl Fleet {
    /// Spawn `stress.nodes()` node processes over `shared`, each publishing
    /// to `snapshots/<name>.toml`, each running `rounds` rounds (`0` runs until
    /// stopped).
    ///
    /// # Errors
    ///
    /// When the `node` binary cannot be found or a process cannot be started.
    pub fn spawn(stress: Stress, shared: &Path, snapshots: &Path, rounds: u64) -> io::Result<Self> {
        Self::spawn_binary(
            &node_binary()?,
            stress,
            stress.nodes(),
            shared,
            snapshots,
            rounds,
        )
    }

    /// The same with the binary named and the count chosen — a test names
    /// `env!("CARGO_BIN_EXE_node")`, and a roll may override the count.
    ///
    /// # Errors
    ///
    /// When a process cannot be started.
    pub fn spawn_binary(
        binary: &Path,
        stress: Stress,
        count: usize,
        shared: &Path,
        snapshots: &Path,
        rounds: u64,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(shared)?;
        std::fs::create_dir_all(snapshots)?;
        std::fs::remove_file(shared.join("stop")).ok();

        let mut fleet = Self {
            binary: binary.to_path_buf(),
            shared: shared.to_path_buf(),
            stress,
            rounds,
            nodes: Vec::new(),
        };
        for index in 1..=count {
            let name = format!("node-{index:02}");
            let path = snapshots.join(format!("{name}.toml"));
            let child = fleet.start(&name, &path)?;
            fleet.nodes.push(Node {
                name,
                child: Some(child),
                exit: None,
                path,
                text: String::new(),
                published: Snapshot::new(),
                fresh: false,
                silent: 0,
                restarts: 0,
            });
        }
        Ok(fleet)
    }

    fn start(&self, name: &str, path: &Path) -> io::Result<Child> {
        Command::new(&self.binary)
            .args(["--name", name, "--stress", self.stress.name()])
            .args(["--rounds", &self.rounds.to_string()])
            .arg("--shared")
            .arg(&self.shared)
            .arg("--snapshot")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
    }

    /// One round of the surface: wait, within a grace period, for every live
    /// node to publish anew; merge what each published; restart the hung; and
    /// add the per-node health and the rollup.
    pub fn tick(&mut self) -> Snapshot {
        let started = Instant::now();
        for node in &mut self.nodes {
            node.fresh = false;
        }
        loop {
            self.read_all();
            let all_fresh = self.nodes.iter().all(|node| node.fresh || !node.alive());
            if all_fresh || started.elapsed() > GRACE {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        for index in 0..self.nodes.len() {
            self.judge_process(index);
        }

        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();
        for node in &self.nodes {
            merge(&mut snapshot, &node.published);
            snapshot.record_health(node.process_record(now));
        }
        snapshot.record_health(rollup(&snapshot, self.nodes.len(), now));
        snapshot
    }

    /// Read every node's file; a changed one is parsed and marks the node fresh.
    fn read_all(&mut self) {
        for node in &mut self.nodes {
            let Ok(text) = std::fs::read_to_string(&node.path) else {
                continue;
            };
            if text == node.text {
                continue;
            }
            if let Ok(snapshot) = from_toml(&text) {
                node.published = snapshot;
                node.text = text;
                node.fresh = true;
            }
        }
    }

    /// Reap an exit, count silence, and restart a node silent too long.
    fn judge_process(&mut self, index: usize) {
        let node = &mut self.nodes[index];
        if let Some(child) = node.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            node.exit = Some(status);
            node.child = None;
        }
        if node.fresh {
            node.silent = 0;
        } else {
            node.silent += 1;
        }
        if node.alive() && node.silent > SILENT_ROUNDS {
            node.kill();
            node.restarts += 1;
            node.silent = 0;
            let (name, path) = (node.name.clone(), node.path.clone());
            match self.start(&name, &path) {
                Ok(child) => {
                    let node = &mut self.nodes[index];
                    node.child = Some(child);
                    node.exit = None;
                }
                Err(error) => eprintln!("fleet: could not restart {name}: {error}"),
            }
        }
    }

    /// Ask every node to leave — the stop file — wait for them, and kill what
    /// remains after the wait.
    pub fn stop(&mut self) {
        std::fs::write(self.shared.join("stop"), b"stop").ok();
        let started = Instant::now();
        while self.alive() > 0 && started.elapsed() < STOP_WAIT {
            for node in &mut self.nodes {
                node.reap();
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        for node in &mut self.nodes {
            node.kill();
        }
    }

    /// How many node processes are still running.
    #[must_use]
    pub fn alive(&self) -> usize {
        self.nodes.iter().filter(|node| node.alive()).count()
    }

    /// The nodes' names, `node-01` up.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.name.as_str())
    }

    /// How many restarts the fleet has recorded, over every node.
    #[must_use]
    pub fn restarts(&self) -> u32 {
        self.nodes.iter().map(|node| node.restarts).sum()
    }
}

impl Drop for Fleet {
    /// A failing test leaves no orphans.
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }
}

impl Node {
    fn alive(&self) -> bool {
        self.child.is_some()
    }

    fn reap(&mut self) {
        if let Some(child) = self.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            self.exit = Some(status);
            self.child = None;
        }
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().ok();
            self.exit = child.wait().ok();
        }
    }

    /// What the fleet knows about the process itself, which the node cannot
    /// say: alive, exited with its code, starting, or restarted after hanging.
    fn process_record(&self, now: i64) -> HealthRecord {
        let restarted = format!(
            "restarted {} time(s) after {SILENT_ROUNDS} silent rounds (hung)",
            self.restarts
        );
        let (health, severity, evidence) = match (&self.exit, self.restarts) {
            (Some(status), _) if !status.success() => {
                (Health::Done, 90, format!("exited with {status}"))
            }
            (Some(_), 0) => (Health::Fine, 0, "exited 0 after its rounds".to_string()),
            (Some(_), _) => (Health::Stressed, 60, format!("exited 0; {restarted}")),
            (None, 0) if self.text.is_empty() => {
                (Health::Working, 20, "starting, no snapshot yet".to_string())
            }
            (None, 0) => (Health::Fine, 0, "alive".to_string()),
            (None, _) => (Health::Stressed, 60, format!("alive; {restarted}")),
        };
        HealthRecord {
            scope: format!("{ROOT}/node/{}/process", self.name),
            health,
            severity,
            evidence,
            observed_unix_nanos: now,
        }
    }
}

/// The cluster rollup the surface owes: the worst leaf across every node, and
/// which node carries it.
fn rollup(snapshot: &Snapshot, count: usize, now: i64) -> HealthRecord {
    let records = snapshot.health(&format!("{ROOT}/node"));
    let worst = records.first();
    let fine = records
        .iter()
        .filter(|record| record.health == Health::Fine)
        .count();
    let (health, severity, evidence) = match worst {
        Some(record) if record.health != Health::Fine => (
            record.health,
            record.severity,
            format!(
                "{count} nodes, {fine} of {} leaves fine; worst {}: {}",
                records.len(),
                record.scope.trim_start_matches(&format!("{ROOT}/node/")),
                record.evidence
            ),
        ),
        Some(_) => (
            Health::Fine,
            0,
            format!("{count} nodes, all {} leaves fine", records.len()),
        ),
        None => (
            Health::Working,
            20,
            format!("{count} nodes, nothing published yet"),
        ),
    };
    HealthRecord {
        scope: format!("{ROOT}/fleet"),
        health,
        severity,
        evidence,
        observed_unix_nanos: now,
    }
}

/// Copy every health record and count from one snapshot into another. Scopes
/// are disjoint per scenario and per node, so nothing collides.
pub fn merge(into: &mut Snapshot, from: &Snapshot) {
    for record in from.health_records() {
        into.record_health(record.clone());
    }
    for count in from.all_counts() {
        into.record_count(count.clone());
    }
}

/// The `node` binary: `XMIP_PLAYGROUND_NODE` if set, else beside the current
/// executable, else one directory up from it (a test runs from `deps/`).
///
/// # Errors
///
/// When none of those is a file.
pub fn node_binary() -> io::Result<PathBuf> {
    if let Some(named) = std::env::var_os("XMIP_PLAYGROUND_NODE") {
        return Ok(PathBuf::from(named));
    }
    let current = std::env::current_exe()?;
    let file = format!("node{}", std::env::consts::EXE_SUFFIX);
    let beside = current.parent().map(|dir| dir.join(&file));
    let above = current
        .parent()
        .and_then(Path::parent)
        .map(|dir| dir.join(&file));
    [beside, above]
        .into_iter()
        .flatten()
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "no node binary beside the playground; build it, or set XMIP_PLAYGROUND_NODE",
            )
        })
}

/// The `node` binary, built now so a test never runs a stale one. Cargo sets
/// `CARGO_BIN_EXE_<name>` for integration tests only, never for a library's
/// unit tests, so a unit test builds the binary itself: a no-op when it is
/// fresh, and cargo has released the build lock by the time tests run.
#[cfg(test)]
pub(crate) fn built_node_binary() -> PathBuf {
    let status = Command::new(env!("CARGO"))
        .args(["build", "-q", "--bin", "node"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("cargo runs");
    assert!(status.success(), "the node binary builds");
    node_binary().expect("the node binary is beside the test executable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;

    fn spawn(stress: Stress, count: usize, rounds: u64) -> (PathBuf, Fleet) {
        let dir = scratch("fleet");
        let fleet = Fleet::spawn_binary(
            &built_node_binary(),
            stress,
            count,
            &dir.join("shared"),
            &dir.join("snapshots"),
            rounds,
        )
        .expect("the fleet spawns");
        (dir, fleet)
    }

    /// Every node's snapshot arrives, the rollup exists, no claim across
    /// processes was double-picked, and stop leaves no child running.
    #[test]
    fn a_realistic_fleet_publishes_rolls_up_and_stops_clean() {
        let (dir, mut fleet) = spawn(Stress::Calm, Stress::Realistic.nodes(), 0);
        assert_eq!(fleet.alive(), 3);

        let mut snapshot = fleet.tick();
        snapshot = merge_into(snapshot, &fleet.tick());

        for name in ["node-01", "node-02", "node-03"] {
            let claim = format!("{ROOT}/node/{name}/claim/file");
            let verdicts = snapshot.health(&claim);
            assert_eq!(verdicts.len(), 3, "{name}: three styles published");
            for record in verdicts {
                assert!(
                    !record.evidence.contains("holders at once"),
                    "{}: {}",
                    record.scope,
                    record.evidence
                );
            }
            assert!(
                snapshot
                    .worst(&format!("{ROOT}/node/{name}/daily"))
                    .is_some(),
                "{name}: the daily drain published"
            );
            assert_eq!(
                snapshot.worst(&format!("{ROOT}/node/{name}/process")),
                Some(Health::Fine),
                "{name} is alive"
            );
        }
        let rollup = snapshot.health(&format!("{ROOT}/fleet"));
        assert_eq!(rollup.len(), 1, "the rollup exists");
        assert!(
            rollup[0].evidence.starts_with("3 nodes"),
            "{}",
            rollup[0].evidence
        );

        fleet.stop();
        assert_eq!(fleet.alive(), 0, "stop leaves no child running");
        assert_eq!(fleet.restarts(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_node_that_exits_is_reported_with_its_code_and_the_fleet_stays_up() {
        let (dir, mut fleet) = spawn(Stress::Calm, 1, 1);
        let mut last = fleet.tick();
        for _ in 0..8 {
            last = fleet.tick();
            if fleet.alive() == 0 {
                break;
            }
        }
        let process = snapshot_record(&last, &format!("{ROOT}/node/node-01/process"));
        assert_eq!(process.health, Health::Fine, "{}", process.evidence);
        assert!(
            process.evidence.starts_with("exited 0"),
            "{}",
            process.evidence
        );
        fleet.stop();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_silent_node_is_restarted_and_the_restart_is_a_yellow() {
        let (dir, mut fleet) = spawn(Stress::Calm, 1, 0);
        // Freeze the node's file at what it first published by pointing the
        // fleet at a copy it will never update; the real node is then "hung".
        fleet.tick();
        let frozen = dir.join("frozen.toml");
        std::fs::copy(&fleet.nodes[0].path, &frozen).expect("a frozen copy");
        fleet.nodes[0].path = frozen;
        let mut last = fleet.tick();
        for _ in 0..=SILENT_ROUNDS {
            last = fleet.tick();
        }
        assert_eq!(fleet.restarts(), 1, "silent past three rounds is hung");
        let process = snapshot_record(&last, &format!("{ROOT}/node/node-01/process"));
        assert_eq!(process.health, Health::Stressed, "{}", process.evidence);
        assert!(process.evidence.contains("hung"), "{}", process.evidence);
        let rollup = snapshot_record(&last, &format!("{ROOT}/fleet"));
        assert_ne!(rollup.health, Health::Fine, "the rollup carries the fault");
        fleet.stop();
        assert_eq!(fleet.alive(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Forty processes, the owner's ceiling. For the runner, not the gate.
    #[test]
    #[ignore = "forty processes; run on purpose"]
    fn brutal_fleet_of_forty() {
        let (dir, mut fleet) = spawn(Stress::Brutal, Stress::Brutal.nodes(), 0);
        let mut snapshot = fleet.tick();
        for _ in 0..4 {
            snapshot = merge_into(snapshot, &fleet.tick());
        }
        let published = fleet
            .names()
            .filter(|name| {
                snapshot
                    .worst(&format!("{ROOT}/node/{name}/claim"))
                    .is_some()
            })
            .count();
        assert_eq!(published, 40, "every one of forty nodes published");
        fleet.stop();
        assert_eq!(fleet.alive(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn merge_into(mut into: Snapshot, from: &Snapshot) -> Snapshot {
        merge(&mut into, from);
        into
    }

    fn snapshot_record(snapshot: &Snapshot, scope: &str) -> HealthRecord {
        snapshot
            .health(scope)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("{scope} is recorded"))
    }
}
