//! CPUs for `bench`: the split between the harness and the candidate, and the bench's CPU time.
//!
//! The split is derived from the box, never typed in. The online CPUs are grouped into physical
//! cores by their hyperthread siblings (sysfs `thread_siblings_list`). The first half of the cores,
//! rounded down, serve the harness (the load generator and the fakes); the rest serve the
//! candidate. Each core stays whole on one side, so the two never share a core.
//!
//! Nothing is pinned: the fence (see `fence`) keeps the candidate on its CPUs and every other
//! process, the bench included, on the rest.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use nix::sys::resource::{UsageWho, getrusage};
use nix::sys::time::TimeVal;

use super::err;
use super::fakes::BoxError;

const ONLINE_CPUS: &str = "/sys/devices/system/cpu/online";

/// The two sets of CPUs a bench run uses, each sorted.
#[derive(Debug, PartialEq)]
pub struct Split {
    pub harness: Vec<usize>,
    pub candidate: Vec<usize>,
}

/// Splits the online CPUs of this machine between the harness and the candidate.
pub fn split_by_core() -> Result<Split, BoxError> {
    let online = read_cpu_list(Path::new(ONLINE_CPUS))?;
    let cores = group_into_cores(&online, |cpu| {
        read_cpu_list(Path::new(&format!(
            "/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
        )))
    })?;
    split_cores(cores)
}

/// Groups CPUs into physical cores with `siblings`, which gives the CPUs sharing one CPU's core
/// (including itself). Each core is sorted, and the cores are sorted by their lowest CPU.
pub fn group_into_cores(
    cpus: &[usize],
    siblings: impl Fn(usize) -> Result<Vec<usize>, BoxError>,
) -> Result<Vec<Vec<usize>>, BoxError> {
    let mut cores: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for &cpu in cpus {
        if !seen.insert(cpu) {
            continue;
        }
        let mut core = siblings(cpu)?;
        core.sort_unstable();
        // The sibling list names the CPU itself, so the core is never empty.
        let lowest = core[0];
        seen.extend(core.iter().copied());
        cores.insert(lowest, core);
    }
    Ok(cores.into_values().collect())
}

/// The first half of the cores, rounded down, to the harness; the rest to the candidate.
fn split_cores(cores: Vec<Vec<usize>>) -> Result<Split, BoxError> {
    if cores.len() < 2 {
        return Err(err(format!(
            "bench splits the box's cores between the harness and the candidate, and needs at \
             least 2; this box has {}",
            cores.len()
        )));
    }
    let (harness, candidate) = cores.split_at(cores.len() / 2);
    let sorted = |side: &[Vec<usize>]| {
        let mut cpus = side.concat();
        cpus.sort_unstable();
        cpus
    };
    Ok(Split {
        harness: sorted(harness),
        candidate: sorted(candidate),
    })
}

/// Reads a kernel CPU list, such as `0-3,8`, from `path`.
fn read_cpu_list(path: &Path) -> Result<Vec<usize>, BoxError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| err(format!("reading {}: {e}", path.display())))?;
    parse_cpu_list(text.trim()).map_err(|e| err(format!("{}: {e}", path.display())))
}

/// Parses a kernel CPU list: comma-separated numbers and ranges, sorted, with no CPU twice.
pub fn parse_cpu_list(text: &str) -> Result<Vec<usize>, String> {
    let bad = || format!("`{text}` is not a CPU list such as `0-3,8`");
    let mut cpus = BTreeSet::new();
    for item in text.split(',') {
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let first: usize = first.parse().map_err(|_| bad())?;
        let last: usize = last.parse().map_err(|_| bad())?;
        if first > last {
            return Err(bad());
        }
        for cpu in first..=last {
            if !cpus.insert(cpu) {
                return Err(format!("`{text}` lists CPU {cpu} twice"));
            }
        }
    }
    Ok(cpus.into_iter().collect())
}

/// CPU seconds the calling process has used, all its threads included, from getrusage.
pub fn self_cpu_secs() -> Result<f64, BoxError> {
    let usage = getrusage(UsageWho::RUSAGE_SELF).map_err(|e| err(format!("getrusage: {e}")))?;
    let secs = |t: TimeVal| t.tv_sec() as f64 + t.tv_usec() as f64 / 1e6;
    Ok(secs(usage.user_time()) + secs(usage.system_time()))
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[test]
    fn cpu_lists_take_numbers_and_ranges() {
        assert_eq!(parse_cpu_list("0-1,4").unwrap(), vec![0, 1, 4]);
        assert_eq!(parse_cpu_list("0,4").unwrap(), vec![0, 4]);
        assert_eq!(parse_cpu_list("7").unwrap(), vec![7]);
        for bad in ["", "a", "3-1", "1-", "1,1", "0-2,2"] {
            assert!(parse_cpu_list(bad).is_err(), "{bad} should be refused");
        }
    }

    /// Four cores of two hyperthreads each, numbered as this box numbers them: CPU N and N+4
    /// share a core.
    fn box_siblings(cpu: usize) -> Result<Vec<usize>, BoxError> {
        Ok((0..8).filter(|sibling| sibling % 4 == cpu % 4).collect())
    }

    #[test]
    fn the_first_half_of_the_cores_is_the_harness() {
        let online: Vec<usize> = (0..8).collect();
        let cores = group_into_cores(&online, box_siblings).unwrap();
        assert_eq!(cores, vec![vec![0, 4], vec![1, 5], vec![2, 6], vec![3, 7]]);
        let split = split_cores(cores).unwrap();
        assert_eq!(split.harness, vec![0, 1, 4, 5]);
        assert_eq!(split.candidate, vec![2, 3, 6, 7]);
    }

    #[test]
    fn a_box_needs_two_cores_to_split() {
        let error = split_cores(vec![vec![0, 1]]).unwrap_err().to_string();
        assert!(error.contains("needs at least 2"), "{error}");
        assert!(error.contains("this box has 1"), "{error}");
    }

    #[test]
    fn a_core_with_one_hyperthread_is_its_own_core() {
        let cores = group_into_cores(&[0, 1, 2], |cpu| Ok(vec![cpu])).unwrap();
        assert_eq!(cores, vec![vec![0], vec![1], vec![2]]);
        let split = split_cores(cores).unwrap();
        assert_eq!(split.harness, vec![0]);
        assert_eq!(split.candidate, vec![1, 2]);
    }
}
