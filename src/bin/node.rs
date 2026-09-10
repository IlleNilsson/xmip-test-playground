//! One emulated node: a System Process the fleet spawns (ADR-0028 clause 2).
//!
//! ```text
//! node --name <name> --shared <dir> --stress <level> --rounds <n> --snapshot <path>
//!      [--interval-ms <ms>]
//! ```
//!
//! It runs, in-process, the **claim** and **daily** scenarios over a directory
//! the whole fleet shares — `<shared>/claim` and `<shared>/daily` — so exclusive
//! pickup and backlog draining are contended by real processes, not threads:
//! the property ADR-0024's claim exists to prove (`create_new`, `O_EXCL`,
//! across processes). Each round it publishes its own snapshot, under
//! `xmip:///playground/node/<name>/...`, atomically to `<path>`; the fleet
//! merges every node's file and adds the cluster rollup the surface owes
//! (ADR-0027 decision 8).
//!
//! It exits after `<n>` rounds — `0` means until stopped — or as soon as
//! `<shared>/stop` appears, checked between rounds. Another node deleting or
//! claiming what this one was about to take is a lost race and a normal
//! outcome; nothing here treats it as an error. The stress level sets the
//! claim's injected breach rate (none at `calm`), and the interval is the pause
//! between rounds, a quarter of a second unless given.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use observe::Snapshot;
use xmip_test_playground::fleet::{ROOT, merge};
use xmip_test_playground::{Claim, Daily, Stress, to_toml, write_atomic};

/// What the command line said.
struct Arguments {
    name: String,
    shared: PathBuf,
    stress: Stress,
    rounds: u64,
    snapshot: PathBuf,
    interval: Duration,
}

fn main() -> ExitCode {
    let arguments = match parse(std::env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(problem) => {
            eprintln!("node: {problem}");
            eprintln!(
                "usage: node --name <name> --shared <dir> --stress <level> --rounds <n> \
                 --snapshot <path> [--interval-ms <ms>]"
            );
            return ExitCode::from(2);
        }
    };

    let node = format!("{ROOT}/node/{}", arguments.name);
    let mut claim =
        Claim::shared(format!("{node}/claim"), arguments.shared.join("claim")).at(arguments.stress);
    let mut daily = Daily::shared(format!("{node}/daily"), arguments.shared.join("daily"));
    let stop = arguments.shared.join("stop");

    let mut round = 0;
    while arguments.rounds == 0 || round < arguments.rounds {
        if stop.exists() {
            break;
        }
        round += 1;

        let mut snapshot = Snapshot::new();
        merge(&mut snapshot, &claim.tick());
        merge(&mut snapshot, &daily.tick());

        if let Err(error) = write_atomic(&arguments.snapshot, &to_toml(&node, &snapshot)) {
            eprintln!(
                "node {}: could not publish to {}: {error}",
                arguments.name,
                arguments.snapshot.display()
            );
        }

        if !arguments.interval.is_zero() && (arguments.rounds == 0 || round < arguments.rounds) {
            std::thread::sleep(arguments.interval);
        }
    }

    ExitCode::SUCCESS
}

/// `--flag value` pairs, every required one present and well-formed.
fn parse(args: impl Iterator<Item = String>) -> Result<Arguments, String> {
    let mut name = None;
    let mut shared = None;
    let mut stress = None;
    let mut rounds = None;
    let mut snapshot = None;
    let mut interval = Duration::from_millis(250);

    let mut args = args.peekable();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--name" => name = Some(value),
            "--shared" => shared = Some(PathBuf::from(value)),
            "--stress" => {
                stress = Some(Stress::parse(&value).ok_or(format!("unknown stress {value}"))?);
            }
            "--rounds" => rounds = Some(number(&flag, &value)?),
            "--snapshot" => snapshot = Some(PathBuf::from(value)),
            "--interval-ms" => interval = Duration::from_millis(number(&flag, &value)?),
            other => return Err(format!("unknown flag {other}")),
        }
    }

    Ok(Arguments {
        name: name.ok_or("--name is required")?,
        shared: shared.ok_or("--shared is required")?,
        stress: stress.ok_or("--stress is required")?,
        rounds: rounds.ok_or("--rounds is required")?,
        snapshot: snapshot.ok_or("--snapshot is required")?,
        interval,
    })
}

fn number(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} wants a number, not {value}"))
}
