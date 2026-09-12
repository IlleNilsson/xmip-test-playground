//! The switches a cluster and a node carry, and the one the tests read:
//! `online`, whether a route to the internet may be assumed (ADR-0045).
//!
//! False unless set. Nothing in the estate reaches out at runtime, so at
//! every stress level the default is that no emulated node may; the switch
//! exists so the first test that genuinely needs the internet reads it and
//! stays silent without it, rather than bringing the suite online with it.
//! `XMIP_ONLINE=true` is how an operator or a roll says the world is there.

/// What a node may assume.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Switches {
    /// A route to the internet may be assumed.
    pub online: bool,
}

impl Switches {
    /// The switches a roll runs with: `XMIP_ONLINE` as `true` or `false`,
    /// false when unset or unrecognised.
    #[must_use]
    pub fn from_env() -> Self {
        Self { online: online() }
    }

    /// The switches as the node binary's flags.
    #[must_use]
    pub fn flags(self) -> Vec<String> {
        vec!["--online".to_string(), self.online.to_string()]
    }

    /// The word a health record carries for it.
    #[must_use]
    pub const fn word(self) -> &'static str {
        if self.online { "online" } else { "offline" }
    }
}

impl Switches {
    /// The switches the fleet's node at 1-based `index` runs with: online for
    /// the first `XMIP_PLAYGROUND_ONLINE_NODES` nodes when that is set, else
    /// whatever `XMIP_ONLINE` says for every node.
    #[must_use]
    pub fn for_node(index: usize) -> Self {
        Self {
            online: node_online(index, online_nodes(), online()),
        }
    }
}

/// Whether this process may assume the internet: `XMIP_ONLINE=true`.
#[must_use]
pub fn online() -> bool {
    std::env::var("XMIP_ONLINE").is_ok_and(|raw| parse(&raw) == Some(true))
}

/// How many of a fleet's nodes, counting from the first, may assume the
/// internet: `XMIP_PLAYGROUND_ONLINE_NODES` as a count, or none set.
#[must_use]
pub fn online_nodes() -> Option<usize> {
    std::env::var("XMIP_PLAYGROUND_ONLINE_NODES")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
}

/// The rule behind [`Switches::for_node`], with the environment already read:
/// a count names the first that many nodes; no count leaves it to `all`.
#[must_use]
pub fn node_online(index: usize, count: Option<usize>, all: bool) -> bool {
    count.map_or(all, |count| index <= count)
}

/// The switch a word means: `true`, `false`, `yes`, `no`, `on`, `off`.
#[must_use]
pub fn parse(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Some(true),
        "false" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// For a test that needs the internet: `true` when it may run, and when it
/// may not, the line to print in its place so a reader sees it was skipped
/// on purpose, not lost.
#[must_use]
pub fn needs_internet(what: &str) -> bool {
    if online() {
        return true;
    }
    println!("skipped offline: {what} needs the internet; set XMIP_ONLINE=true to run it");
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_is_off_unless_said_and_reads_the_usual_words() {
        assert_eq!(parse("TRUE"), Some(true));
        assert_eq!(parse("off"), Some(false));
        assert_eq!(parse("maybe"), None);
        assert_eq!(Switches::default().word(), "offline");
        assert_eq!(Switches { online: true }.word(), "online");
        assert_eq!(Switches { online: true }.flags(), ["--online", "true"]);
    }

    #[test]
    fn a_count_puts_the_first_nodes_online_and_no_count_defers_to_all() {
        assert!(node_online(1, Some(2), false));
        assert!(node_online(2, Some(2), false));
        assert!(!node_online(3, Some(2), true));
        assert!(!node_online(1, Some(0), true));
        assert!(node_online(7, None, true));
        assert!(!node_online(7, None, false));
    }

    #[test]
    fn a_test_that_needs_the_internet_reads_the_switch() {
        // The suite runs offline; this proves the gate says so and yields.
        if needs_internet("this very test") {
            assert!(online());
        } else {
            assert!(!online());
        }
    }
}
