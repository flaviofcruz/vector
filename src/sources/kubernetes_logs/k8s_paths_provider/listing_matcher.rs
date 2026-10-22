//! Discovery that reads each directory under a scan root at most once per `match_root` call and
//! matches every pattern against those listings in memory. Listings can also come from the
//! process-wide [`ListingCache`] shared by all sources.
//!
//! Running `glob::glob_with` once per pattern re-stats the scan root's literal prefix and re-reads
//! every directory for each of the (up to hundreds of) configured patterns, which on busy sandbox
//! hosts saturates the agent's CPU quota in kernel path walks (ES-2271421). This matcher
//! reproduces glob 0.3.3's per-pattern output exactly: matches within a directory in file-name
//! byte order, depth-first, patterns concatenated in order with duplicates kept, case-insensitive
//! wildcard segments, literal segments resolved by exact name, directory symlinks followed, a
//! trailing `/` restricting matches to directories, and unreadable directories contributing
//! nothing. Pattern shapes it cannot reproduce exactly are delegated to glob itself.

use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use super::{
    ScanRootMatcher, existing_subdirs_by_stat, glob_scan_root, listing_cache::ListingCache,
    real_glob,
};

/// Must stay identical to the options `real_glob` uses (note `case_sensitive: false`).
fn match_options() -> glob::MatchOptions {
    glob::MatchOptions {
        require_literal_separator: true,
        ..Default::default()
    }
}

fn is_literal(segment: &str) -> bool {
    !segment.contains(['*', '?', '['])
}

/// Whether `dir` is an absolute path of plain literal segments, so glob would resolve it by exact
/// name (one segment at a time) before applying the pattern.
fn is_plain_root(dir: &str) -> bool {
    dir.strip_prefix('/').is_some_and(|rest| {
        rest.split('/').all(|segment| {
            !segment.is_empty() && segment != "." && segment != ".." && is_literal(segment)
        })
    })
}

enum Segment {
    Literal(String),
    Wildcard(glob::Pattern),
}

enum CompiledPattern {
    Walk {
        segments: Vec<Segment>,
        require_dir: bool,
    },
    /// Shapes the walk does not model (`**`, `.`/`..`, empty segments, invalid syntax), which
    /// keep using glob.
    Glob(String),
}

impl CompiledPattern {
    fn new(pattern: &str) -> Self {
        let fallback = || Self::Glob(pattern.to_owned());
        if pattern.is_empty() || pattern.contains("**") || glob::Pattern::new(pattern).is_err() {
            return fallback();
        }
        let mut segments = Vec::new();
        for segment in pattern.split_terminator('/') {
            if segment.is_empty() || segment == "." || segment == ".." {
                return fallback();
            }
            if is_literal(segment) {
                segments.push(Segment::Literal(segment.to_owned()));
                continue;
            }
            let Ok(compiled) = glob::Pattern::new(segment) else {
                return fallback();
            };
            // glob additionally yields the `.` and `..` entries for wildcard segments that start
            // with a literal dot.
            if segment.starts_with('.')
                && (compiled.matches_with(".", match_options())
                    || compiled.matches_with("..", match_options()))
            {
                return fallback();
            }
            segments.push(Segment::Wildcard(compiled));
        }
        Self::Walk {
            segments,
            require_dir: pattern.ends_with('/'),
        }
    }
}

#[derive(Debug)]
pub(super) struct Entry {
    pub(super) name: OsString,
    path: PathBuf,
    is_dir: bool,
}

/// A directory's entries sorted by name, or the kind of error reading it failed with.
pub(super) type Listing = Result<Arc<[Entry]>, io::ErrorKind>;

pub(super) fn read_listing(dir: &Path) -> Listing {
    let mut entries = fs::read_dir(dir)
        .and_then(|entries| {
            entries
                .map(|entry| {
                    entry.map(|entry| {
                        let path = entry.path();
                        // Same as glob: trust the entry type unless it is a symlink (or unknown),
                        // in which case follow it.
                        let is_dir = entry
                            .file_type()
                            .ok()
                            .and_then(|file_type| {
                                (!file_type.is_symlink()).then(|| file_type.is_dir())
                            })
                            .or_else(|| fs::metadata(&path).map(|meta| meta.is_dir()).ok())
                            .unwrap_or(false);
                        Entry {
                            name: entry.file_name(),
                            path,
                            is_dir,
                        }
                    })
                })
                .collect::<io::Result<Vec<_>>>()
        })
        .map_err(|error| error.kind())?;
    entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    Ok(entries.into())
}

/// Whether a listing may be reused by later lookups. Errors such as `EMFILE` or `EIO` are
/// transient and per-call: reusing one would hide the directory from every later pattern (and,
/// through the shared cache, from every source at once, which makes the file server drop that
/// directory's archive watchers), so only stable outcomes are kept.
pub(super) const fn is_stable(listing: &Listing) -> bool {
    match listing {
        Ok(_) => true,
        Err(kind) => matches!(
            kind,
            io::ErrorKind::NotFound
                | io::ErrorKind::NotADirectory
                | io::ErrorKind::PermissionDenied
        ),
    }
}

/// The directory is definitely absent (or not a directory), so nothing below it exists.
const fn is_absent(kind: io::ErrorKind) -> bool {
    matches!(kind, io::ErrorKind::NotFound | io::ErrorKind::NotADirectory)
}

fn lookup<'a>(listing: &'a Listing, dir: &Path, name: &OsStr) -> Option<(Cow<'a, Path>, bool)> {
    match listing {
        Ok(entries) => entries
            .binary_search_by(|entry| entry.name.as_os_str().cmp(name))
            .ok()
            .map(|index| {
                (
                    Cow::Borrowed(entries[index].path.as_path()),
                    entries[index].is_dir,
                )
            }),
        Err(kind) if is_absent(*kind) => None,
        // Listing failed (e.g. no read permission) but the entry may still be reachable by name.
        Err(_) => {
            let path = dir.join(name);
            let metadata = fs::metadata(&path);
            let exists = metadata.is_ok() || fs::symlink_metadata(&path).is_ok();
            exists.then(|| (Cow::Owned(path), metadata.is_ok_and(|meta| meta.is_dir())))
        }
    }
}

/// Where listings come from: the filesystem directly, or a cache shared with other sources.
#[derive(Clone, Default)]
enum ListingSource {
    #[default]
    Direct,
    Cached {
        cache: Arc<ListingCache>,
        max_age: Duration,
    },
}

impl ListingSource {
    fn read(&self, dir: &Path) -> Listing {
        match self {
            Self::Direct => read_listing(dir),
            Self::Cached { cache, max_age } => cache.get(dir, *max_age),
        }
    }
}

/// The listings used for one scan root, so a directory reached by several patterns is read (or
/// looked up in the shared cache) once and every pattern sees the same snapshot. Keyed by path
/// bytes, which is cheaper than component-wise `Path` hashing and never conflates two paths.
/// Transient errors are not kept, so later patterns retry the read as glob would.
struct Listings {
    source: ListingSource,
    by_dir: HashMap<OsString, Listing>,
}

impl Listings {
    fn get(&mut self, dir: &Path) -> Listing {
        if let Some(listing) = self.by_dir.get(dir.as_os_str()) {
            return listing.clone();
        }
        let listing = self.source.read(dir);
        if is_stable(&listing) {
            self.by_dir
                .insert(dir.as_os_str().to_owned(), listing.clone());
        }
        listing
    }

    fn walk(
        &mut self,
        dir: &Path,
        segments: &[Segment],
        require_dir: bool,
        out: &mut Vec<PathBuf>,
    ) {
        let Some((segment, rest)) = segments.split_first() else {
            return;
        };
        match segment {
            Segment::Literal(name) => {
                let listing = self.get(dir);
                if let Some((path, is_dir)) = lookup(&listing, dir, OsStr::new(name)) {
                    self.descend(&path, is_dir, rest, require_dir, out);
                }
            }
            Segment::Wildcard(pattern) => {
                let Ok(entries) = self.get(dir) else {
                    return;
                };
                for entry in entries.iter() {
                    // glob skips non-UTF-8 names for wildcard segments.
                    let Some(name) = entry.name.to_str() else {
                        continue;
                    };
                    if pattern.matches_with(name, match_options()) {
                        self.descend(&entry.path, entry.is_dir, rest, require_dir, out);
                    }
                }
            }
        }
    }

    fn descend(
        &mut self,
        path: &Path,
        is_dir: bool,
        rest: &[Segment],
        require_dir: bool,
        out: &mut Vec<PathBuf>,
    ) {
        if rest.is_empty() {
            if !require_dir || is_dir {
                out.push(path.to_path_buf());
            }
        } else if is_dir {
            self.walk(path, rest, require_dir, out);
        }
    }
}

/// Lists each directory once per scan root for all patterns; see the module docs.
///
/// Without a shared cache it holds no listings across `match_root` calls, so every call observes
/// the current filesystem; with one, listings may be up to the cache `max_age` old.
#[derive(Default)]
pub(super) struct ListingScanRootMatcher {
    source: ListingSource,
    /// The patterns `compiled` was built from; a matcher is normally used with one pattern list.
    compiled_for: Vec<String>,
    compiled: Vec<CompiledPattern>,
}

impl ListingScanRootMatcher {
    pub(super) fn with_cache(cache: Arc<ListingCache>, max_age: Duration) -> Self {
        Self {
            source: ListingSource::Cached { cache, max_age },
            ..Self::default()
        }
    }
}

impl ScanRootMatcher for ListingScanRootMatcher {
    /// One listing of `base` instead of a stat per name; a pod without an emptyDir costs one
    /// failed open.
    fn existing_subdirs(&mut self, base: &Path, names: &[&str]) -> Vec<PathBuf> {
        let listing = self.source.read(base);
        match &listing {
            Err(kind) if !is_absent(*kind) => existing_subdirs_by_stat(base, names),
            Err(_) => Vec::new(),
            Ok(_) => names
                .iter()
                .filter(|name| {
                    lookup(&listing, base, OsStr::new(name)).is_some_and(|(_, is_dir)| is_dir)
                })
                .map(|name| base.join(name))
                .collect(),
        }
    }

    fn match_root(&mut self, dir: &str, patterns: &[String]) -> Vec<PathBuf> {
        if !is_plain_root(dir) {
            return glob_scan_root(real_glob, dir, patterns);
        }
        let root = Path::new(dir);
        let mut listings = Listings {
            source: self.source.clone(),
            by_dir: HashMap::new(),
        };
        let mut out = Vec::new();
        // Every pattern has at least one segment below the root, so a missing root matches nothing.
        if listings.get(root).is_err_and(is_absent) {
            return out;
        }
        if self.compiled_for.as_slice() != patterns {
            self.compiled = patterns
                .iter()
                .map(|pattern| CompiledPattern::new(pattern))
                .collect();
            self.compiled_for = patterns.to_vec();
        }
        for compiled in &self.compiled {
            match compiled {
                CompiledPattern::Walk {
                    segments,
                    require_dir,
                } => listings.walk(root, segments, *require_dir, &mut out),
                CompiledPattern::Glob(pattern) => {
                    out.extend(real_glob(&format!("{dir}/{pattern}")))
                }
            }
        }
        out
    }
}

#[cfg(test)]
pub(super) fn walks_without_glob(pattern: &str) -> bool {
    matches!(CompiledPattern::new(pattern), CompiledPattern::Walk { .. })
}
