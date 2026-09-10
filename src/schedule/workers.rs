//! The pairs of a round driven from several threads at once.
//!
//! A [`Stress`](crate::stress::Stress) level says how many pairs run at once;
//! this is the one driver that runs them, for the schedule and the storm
//! alike, with the order of the verdicts kept whatever the threads did.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::roundtrip::RoundTrip;
use crate::schedule::CONTRACTS;
use crate::verdict::Contract;

/// Every (transport, contract) pair judged by `judge` from `workers` threads
/// at once, the results in the pairs' original order whichever thread reached
/// them first — so a verdict list is deterministic however the threads
/// interleaved. Pairs are handed out from one counter, not in strides, so a
/// round waited on to its timeout holds up one worker rather than a whole
/// stride of pairs behind it. Shared with the storm, which judges the same
/// pairs by its own question.
///
/// # Panics
///
/// When a pair's judge panics: a scheduled round that panics is a defect to
/// fail on loudly, not a verdict to fold in. The storm catches per pair
/// before reaching here.
pub(crate) fn drive_pairs<T, F>(
    transports: &[Box<dyn RoundTrip>],
    workers: usize,
    judge: F,
) -> Vec<T>
where
    T: Send,
    F: Fn(&dyn RoundTrip, Contract) -> T + Sync,
{
    let pairs: Vec<(usize, Contract)> = (0..transports.len())
        .flat_map(|at| CONTRACTS.iter().map(move |&contract| (at, contract)))
        .collect();
    let workers = workers.clamp(1, pairs.len().max(1));
    let next = AtomicUsize::new(0);

    let mut judged: Vec<(usize, T)> = std::thread::scope(|scope| {
        let hands: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut mine = Vec::new();
                    loop {
                        let at = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(transport, contract)) = pairs.get(at) else {
                            break;
                        };
                        mine.push((at, judge(transports[transport].as_ref(), contract)));
                    }
                    mine
                })
            })
            .collect();
        hands
            .into_iter()
            .flat_map(|hand| hand.join().expect("a pair's judge panicked"))
            .collect()
    });

    judged.sort_by_key(|(at, _)| *at);
    judged.into_iter().map(|(_, verdict)| verdict).collect()
}
