//! Output directory check shared by the fakes.
//!
//! The fakes write an append-only index and capture files that are named by counter or by
//! object key. Reusing a directory would mix an old run's index rows with files a new run has
//! overwritten, so a fake refuses to start on a non-empty `--out` instead of guessing.

use std::path::Path;

use super::{CmdResult, err, io_at};

/// Creates `out` if it is missing and fails if it exists with anything inside. Never deletes.
pub fn claim_empty_out_dir(out: &Path) -> CmdResult {
    claim_empty_dir("--out", out)
}

/// `claim_empty_out_dir` for any directory flag: `flag` names the flag in the errors.
pub fn claim_empty_dir(flag: &str, dir: &Path) -> CmdResult {
    if !dir.exists() {
        io_at(
            std::fs::create_dir_all(dir),
            &format!("creating {flag}"),
            dir,
        )?;
        return Ok(true);
    }
    if !dir.is_dir() {
        return Err(err(format!(
            "{flag} {} exists and is not a directory; pick a fresh {flag}",
            dir.display()
        )));
    }
    let mut entries = Vec::new();
    for entry in io_at(std::fs::read_dir(dir), &format!("reading {flag}"), dir)? {
        let entry = io_at(entry, &format!("reading {flag}"), dir)?;
        entries.push(entry.file_name().to_string_lossy().into_owned());
    }
    if entries.is_empty() {
        return Ok(true);
    }
    entries.sort();
    entries.truncate(5);
    Err(err(format!(
        "{flag} {} is not empty (contains {}); pick a fresh {flag}, or remove the directory \
         yourself: the harness never deletes files",
        dir.display(),
        entries.join(", ")
    )))
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    #[test]
    fn creates_a_missing_out_dir() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("fresh");
        assert!(claim_empty_out_dir(&out).is_ok());
        assert!(out.is_dir());
    }

    #[test]
    fn accepts_an_empty_out_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(claim_empty_out_dir(dir.path()).is_ok());
    }

    #[test]
    fn refuses_a_dir_with_an_index() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("index.ndjson"), b"").unwrap();
        let error = claim_empty_out_dir(dir.path()).unwrap_err().to_string();
        assert!(error.contains(&dir.path().display().to_string()), "{error}");
        assert!(error.contains("index.ndjson"), "{error}");
        assert!(error.contains("pick a fresh --out"), "{error}");
    }

    #[test]
    fn refuses_a_dir_with_a_subdirectory_and_keeps_its_contents() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("cand-a")).unwrap();
        assert!(claim_empty_out_dir(dir.path()).is_err());
        assert!(dir.path().join("cand-a").is_dir());
    }

    #[test]
    fn io_errors_name_the_action_and_path() {
        let not_found = std::io::Error::from(std::io::ErrorKind::NotFound);
        let error =
            io_at::<()>(Err(not_found), "reading --schema", Path::new("/x/y.json")).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("reading --schema /x/y.json: "),
            "{error}"
        );
    }

    #[test]
    fn refuses_an_out_path_that_is_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"").unwrap();
        assert!(claim_empty_out_dir(&file).is_err());
    }
}
