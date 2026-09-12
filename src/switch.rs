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
    /// The switches the fleet's node called `name` runs with: online when
    /// `XMIP_PLAYGROUND_ONLINE_NODES` names it, else — the variable unset —
    /// whatever `XMIP_ONLINE` says for every node.
    #[must_use]
    pub fn for_node(name: &str) -> Self {
        Self {
            online: node_online(name, online_nodes().as_deref(), online()),
        }
    }
}

/// Whether this process may assume the internet: `XMIP_ONLINE=true`.
#[must_use]
pub fn online() -> bool {
    std::env::var("XMIP_ONLINE").is_ok_and(|raw| parse(&raw) == Some(true))
}

/// The fleet's nodes that may assume the internet, by name:
/// `XMIP_PLAYGROUND_ONLINE_NODES`, comma separated; `None` when unset. Set and
/// empty means none of them.
#[must_use]
pub fn online_nodes() -> Option<Vec<String>> {
    std::env::var("XMIP_PLAYGROUND_ONLINE_NODES")
        .ok()
        .map(|raw| names(&raw))
}

/// A comma-separated list of node names, trimmed, empties dropped.
#[must_use]
pub fn names(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

/// The rule behind [`Switches::for_node`], with the environment already read:
/// a list names the online nodes, case-insensitively; no list leaves it to `all`.
#[must_use]
pub fn node_online(name: &str, online: Option<&[String]>, all: bool) -> bool {
    online.map_or(all, |online| {
        online.iter().any(|one| one.eq_ignore_ascii_case(name))
    })
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
    fn a_list_names_the_online_nodes_and_no_list_defers_to_all() {
        let online = names(" alpha, Beta ,, ");
        assert_eq!(online, ["alpha", "Beta"]);
        assert!(node_online("alpha", Some(&online), false));
        assert!(node_online("beta", Some(&online), false));
        assert!(!node_online("gamma", Some(&online), true));
        assert!(!node_online("alpha", Some(&[]), true));
        assert!(node_online("gamma", None, true));
        assert!(!node_online("gamma", None, false));
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
