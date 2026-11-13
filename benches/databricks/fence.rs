//! The fence: a cgroup-v2 cpuset partition that gives one candidate its CPUs to itself.
//!
//! A partition of type `root` keeps the candidate on its CPUs and every other process off them: the
//! kernel takes its CPUs from every other cgroup, so system services, the user's session and the
//! harness all run on the rest, and a process moved into it, with every thread it starts, runs on
//! its CPUs only. The scheduler still balances the candidate's threads across them. (Type
//! `isolated` would also turn that balancing off, leaving each thread on the CPU it started on.)
//! Per-CPU kernel threads and interrupts are not moved; only boot options do that.
//!
//! A partition must be a child of the root cgroup, and moving a process into it means writing to
//! a file only root may write, so every write here goes through `sudo -n`; reads need no
//! privilege. The fence is made per candidate, which also gives each its own `memory.peak`, and
//! removed after it, which hands the CPUs back. It lives in the kernel only, so a reboot removes it
//! too.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use super::cgroup::Cgroup;
use super::cpus::parse_cpu_list;
use super::err;
use super::fakes::BoxError;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
/// The fence's name under the root cgroup.
const FENCE_NAME: &str = "databricks-bench";

/// A `root` partition holding `cpus`, with a memory limit and no swap. Remove it with `remove`.
pub struct Fence {
    dir: PathBuf,
    cpus: Vec<usize>,
}

impl Fence {
    /// Makes the partition and checks that it took: that it is a valid `root` partition with
    /// exactly `cpus`, and that no other top-level cgroup can still run on any of them. Refuses if
    /// the fence already exists, since that is a run that did not clean up.
    pub fn create(cpus: &[usize], memory_max: &str) -> Result<Self, BoxError> {
        let dir = Path::new(CGROUP_ROOT).join(FENCE_NAME);
        if dir.exists() {
            return Err(err(format!(
                "{} already exists, left by a bench run that did not finish; remove it with:\n{}",
                dir.display(),
                removal_commands(&dir)
            )));
        }
        sudo(&["mkdir", &dir.display().to_string()])?;
        let fence = Self {
            dir,
            cpus: cpus.to_vec(),
        };
        if let Err(e) = fence.configure(memory_max) {
            return Err(match fence.remove() {
                Ok(()) => e,
                Err(cleanup) => err(format!("{e}\nand then: {cleanup}")),
            });
        }
        Ok(fence)
    }

    fn configure(&self, memory_max: &str) -> Result<(), BoxError> {
        self.write("cpuset.cpus", &cpu_list(&self.cpus))?;
        self.write("cpuset.cpus.partition", "root")?;
        self.write("memory.max", memory_max)
            .map_err(|e| err(format!("--candidate-memory `{memory_max}`: {e}")))?;
        self.write("memory.swap.max", "0")?;

        check_partition(&self.partition()?)
            .map_err(|e| err(format!("{}: {e}", self.dir.display())))?;
        let effective = cpus_effective(&self.dir)?;
        if effective != self.cpus {
            return Err(err(format!(
                "{} has CPUs {}, not {}",
                self.dir.display(),
                cpu_list(&effective),
                cpu_list(&self.cpus)
            )));
        }
        for entry in std::fs::read_dir(CGROUP_ROOT)? {
            let other = entry?.path();
            if other == self.dir || !other.join("cpuset.cpus.effective").exists() {
                continue;
            }
            let theirs = cpus_effective(&other)?;
            if theirs.iter().any(|cpu| self.cpus.contains(cpu)) {
                return Err(err(format!(
                    "{} can still run on CPUs {}, so the fence did not take",
                    other.display(),
                    cpu_list(&theirs)
                )));
            }
        }
        Ok(())
    }

    /// Moves process `pid`, with all its threads, into the fence, and checks it arrived.
    pub fn admit(&self, pid: u32) -> Result<(), BoxError> {
        self.write("cgroup.procs", &pid.to_string())?;
        let path = format!("/proc/{pid}/cgroup");
        let text = read(Path::new(&path))?;
        let want = format!("0::/{FENCE_NAME}");
        if text.lines().next() != Some(want.as_str()) {
            return Err(err(format!(
                "{path} reads `{}`, not `{want}`: the process is not in the fence",
                text.trim()
            )));
        }
        Ok(())
    }

    /// What `cpuset.cpus.partition` reads now. The kernel can invalidate a partition after it was
    /// made, such as when a CPU goes offline, so a run checks it again before trusting its numbers.
    pub fn partition(&self) -> Result<String, BoxError> {
        Ok(read(&self.dir.join("cpuset.cpus.partition"))?
            .trim()
            .to_owned())
    }

    /// The fence's cgroup, for its counters.
    pub fn cgroup(&self) -> Cgroup {
        Cgroup::at(self.dir.clone())
    }

    /// The fence's path, partition type, CPUs and limits, for the results.
    pub fn describe(&self) -> Result<FenceSettings, BoxError> {
        Ok(FenceSettings {
            cgroup: self.dir.clone(),
            partition: self.partition()?,
            cpus: self.cpus.clone(),
            memory_max: read(&self.dir.join("memory.max"))?.trim().to_owned(),
            swap_max: read(&self.dir.join("memory.swap.max"))?.trim().to_owned(),
        })
    }

    /// Turns the partition back into an ordinary cgroup and removes it, which hands its CPUs back.
    /// The candidate must have exited. On failure the error gives the commands that finish it.
    pub fn remove(&self) -> Result<(), BoxError> {
        let unfinished = |e: BoxError| {
            err(format!(
                "removing the fence {}: {e}; finish it with:\n{}",
                self.dir.display(),
                removal_commands(&self.dir)
            ))
        };
        let procs = read(&self.dir.join("cgroup.procs")).map_err(unfinished)?;
        if !procs.trim().is_empty() {
            return Err(unfinished(err(format!(
                "processes {} are still in it",
                procs.split_whitespace().collect::<Vec<_>>().join(", ")
            ))));
        }
        self.write("cpuset.cpus.partition", "member")
            .map_err(unfinished)?;
        sudo(&["rmdir", &self.dir.display().to_string()]).map_err(unfinished)
    }

    /// Writes `value` to one of the fence's files as root.
    fn write(&self, file: &str, value: &str) -> Result<(), BoxError> {
        let path = self.dir.join(file);
        let mut child = Command::new("sudo")
            .args(["-n", "tee"])
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| err(format!("running sudo: {e}")))?;
        child
            .stdin
            .take()
            .expect("stdin is piped")
            .write_all(value.as_bytes())?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(sudo_error(
                &format!("tee {} (writing `{value}`)", path.display()),
                &output,
            ));
        }
        Ok(())
    }
}

/// What `results.json` records about the fence a candidate ran in.
#[derive(serde::Serialize)]
pub struct FenceSettings {
    cgroup: PathBuf,
    partition: String,
    pub cpus: Vec<usize>,
    memory_max: String,
    swap_max: String,
}

fn cpus_effective(dir: &Path) -> Result<Vec<usize>, BoxError> {
    let path = dir.join("cpuset.cpus.effective");
    let text = read(&path)?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    parse_cpu_list(text.trim()).map_err(|e| err(format!("{}: {e}", path.display())))
}

/// Runs `sudo -n <args>`. `-n` fails at once rather than prompting for a password.
fn sudo(args: &[&str]) -> Result<(), BoxError> {
    let output = Command::new("sudo")
        .arg("-n")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| err(format!("running sudo: {e}")))?;
    if !output.status.success() {
        return Err(sudo_error(&args.join(" "), &output));
    }
    Ok(())
}

fn sudo_error(what: &str, output: &std::process::Output) -> BoxError {
    err(format!(
        "sudo -n {what} failed ({}): {}; bench needs passwordless sudo to fence the candidate's CPUs",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

/// The shell commands that remove a fence by hand.
fn removal_commands(dir: &Path) -> String {
    let dir = dir.display();
    format!("  echo member | sudo tee {dir}/cpuset.cpus.partition\n  sudo rmdir {dir}")
}

/// `cpuset.cpus.partition` must read exactly `root`. An invalid partition reads `root invalid
/// (<reason>)`, and its CPUs are not exclusive.
pub fn check_partition(text: &str) -> Result<(), String> {
    match text.trim() {
        "root" => Ok(()),
        other => Err(format!(
            "the partition is `{other}`, not a valid `root`, so its CPUs are not exclusive"
        )),
    }
}

fn cpu_list(cpus: &[usize]) -> String {
    cpus.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn read(path: &Path) -> Result<String, BoxError> {
    std::fs::read_to_string(path).map_err(|e| err(format!("reading {}: {e}", path.display())))
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[test]
    fn only_a_valid_root_partition_passes() {
        assert!(check_partition("root\n").is_ok());
        let invalid =
            check_partition("root invalid (Cpu list in cpuset.cpus not exclusive)\n").unwrap_err();
        assert!(invalid.contains("not exclusive)`"), "{invalid}");
        assert!(check_partition("member\n").is_err());
        assert!(check_partition("isolated\n").is_err());
    }
}
