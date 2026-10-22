//! Equivalence harness for log-path discovery.
//!
//! Discovery must keep returning exactly what the per-pattern `glob_with` reference returns:
//! the FileServer inserts newly found files into an insertion-ordered `fp_map` and resolves
//! duplicate fingerprints by first-seen path, so outputs are compared as ordered lists,
//! duplicates included.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use k8s_openapi::{
    api::core::v1::{Namespace, Pod},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::runtime::{reflector::store::Writer, watcher};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use tempfile::TempDir;

use super::{
    AnnotationSelector, CONTAINER_EXCLUSION_ANNOTATION_KEY,
    DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY, GlobScanRootMatcher, K8sPathsProvider,
    POD_NAME_ANNOTATION_KEY, ScanRootMatcher,
    listing_cache::{
        MAX_ENTRIES, MAX_LISTING_AGE,
        test_support::{ManualClock, cache_with_reader, counting_cache},
    },
    listing_matcher::{ListingScanRootMatcher, read_listing, walks_without_glob},
    real_glob,
};

fn reference_matcher() -> impl ScanRootMatcher {
    GlobScanRootMatcher(real_glob)
}

/// The matcher `K8sPathsProvider::paths` uses in production.
fn candidate_matcher() -> impl ScanRootMatcher {
    ListingScanRootMatcher::default()
}

/// The production matcher reading through a private (not process-wide) listing cache.
fn cached_matcher(clock: &ManualClock) -> ListingScanRootMatcher {
    ListingScanRootMatcher::with_cache(counting_cache(clock, MAX_ENTRIES).0, MAX_LISTING_AGE)
}

/// Maps the absolute paths discovery computes (`/var/lib/kubelet/pods/...`,
/// `/databricks/host-root/...`) into a temp root and strips the root from results, so the
/// whole provider can run against a fake host filesystem.
struct Rebased<'a, M> {
    root: &'a Path,
    inner: M,
}

impl<M: ScanRootMatcher> Rebased<'_, M> {
    fn rebase(&self, path: &Path) -> PathBuf {
        self.root.join(
            path.strip_prefix("/")
                .expect("discovery paths are absolute"),
        )
    }
}

impl<M: ScanRootMatcher> ScanRootMatcher for Rebased<'_, M> {
    fn existing_subdirs(&mut self, base: &Path, names: &[&str]) -> Vec<PathBuf> {
        let rebased = self.rebase(base);
        let root = self.root;
        self.inner
            .existing_subdirs(&rebased, names)
            .into_iter()
            .map(|path| Path::new("/").join(path.strip_prefix(root).unwrap()))
            .collect()
    }

    fn match_root(&mut self, dir: &str, patterns: &[String]) -> Vec<PathBuf> {
        let rebased = self.rebase(Path::new(dir));
        let root = self.root;
        self.inner
            .match_root(rebased.to_str().unwrap(), patterns)
            .into_iter()
            .map(|path| Path::new("/").join(path.strip_prefix(root).unwrap()))
            .collect()
    }
}

fn touch(root: &Path, rel: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, b"x").unwrap();
}

fn mkdir(root: &Path, rel: &str) {
    fs::create_dir_all(root.join(rel)).unwrap();
}

fn symlink(root: &Path, target: &str, link: &str) {
    let link = root.join(link);
    fs::create_dir_all(link.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(target, link).unwrap();
}

fn rel(root: &Path, paths: Vec<PathBuf>) -> Vec<String> {
    paths
        .into_iter()
        .map(|path| {
            path.strip_prefix(root)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn assert_match_root_equivalent(dir: &Path, patterns: &[String]) -> Vec<PathBuf> {
    let dir = dir.to_str().unwrap();
    let expected = reference_matcher().match_root(dir, patterns);
    let actual = candidate_matcher().match_root(dir, patterns);
    assert_eq!(actual, expected, "dir={dir} patterns={patterns:?}");
    // Through a cache: once populating it, once served entirely from it.
    let mut cached = cached_matcher(&ManualClock::new());
    for _ in 0..2 {
        assert_eq!(
            cached.match_root(dir, patterns),
            expected,
            "cached dir={dir} patterns={patterns:?}"
        );
    }
    for pattern in patterns {
        let single = std::slice::from_ref(pattern);
        assert_eq!(
            candidate_matcher().match_root(dir, single),
            reference_matcher().match_root(dir, single),
            "dir={dir} pattern={pattern}"
        );
    }
    expected
}

/// A tree covering the glob edge cases discovery depends on.
fn edge_case_tree() -> TempDir {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    for file in [
        "a.log",
        "b.log",
        "ab.log",
        "a.log.1",
        "a.log.2.gz",
        ".hidden.log",
        "[weird].log",
        "q.json",
        "c/stdout.log",
        "c/stderr.log",
        "c/.dot.log",
        "c/sub/deep.log",
        "c/sub/deeper/deepest.log",
        ".dotdir/inside.log",
        "x-y_z/0.log",
        "UPPER/A.LOG",
        "spaces dir/s.log",
    ] {
        touch(root, file);
    }
    // Directories whose names match file patterns are returned by glob too.
    mkdir(root, "dirlike.log");
    mkdir(root, "empty");
    symlink(root, "a.log", "link-to-file.log");
    symlink(root, "c", "link-to-dir");
    symlink(root, "does-not-exist", "dangling.log");
    tmp
}

const EDGE_CASE_PATTERNS: &[&str] = &[
    "*",
    "*.log",
    "*.log*",
    "?.log",
    "??.log",
    "[ab].log",
    "[!a]*.log",
    "[a-b]*",
    "[[]weird[]].log",
    "*/*.log",
    "*/*",
    "*/*/*.log",
    "*/sub/*",
    "c/*",
    "c/*.log",
    "c/sub/*/*.log",
    "*/",
    "c/",
    ".*",
    "*/.*",
    "link-to-dir/*.log",
    "UPPER/*.log",
    "spaces dir/*",
    "missing/*.log",
    "missing",
    "a.log",
    "**/*.log",
];

#[test]
fn golden_glob_semantics() {
    let tmp = edge_case_tree();
    let root = tmp.path();
    let run = |pattern: &str| {
        rel(
            root,
            reference_matcher().match_root(root.to_str().unwrap(), &strings(&[pattern])),
        )
    };

    // `*` does not cross `/`, matches dotfiles, and matches directories and symlinks.
    assert_eq!(
        run("*.log"),
        strings(&[
            ".hidden.log",
            "[weird].log",
            "a.log",
            "ab.log",
            "b.log",
            "dangling.log",
            "dirlike.log",
            "link-to-file.log",
        ])
    );
    assert_eq!(run("?.log"), strings(&["a.log", "b.log"]));
    assert_eq!(
        run("[!a]*.log"),
        strings(&[
            ".hidden.log",
            "[weird].log",
            "b.log",
            "dangling.log",
            "dirlike.log",
            "link-to-file.log"
        ])
    );
    assert_eq!(run("[[]weird[]].log"), strings(&["[weird].log"]));
    // Directory symlinks are followed.
    assert_eq!(
        run("link-to-dir/*.log"),
        strings(&[
            "link-to-dir/.dot.log",
            "link-to-dir/stderr.log",
            "link-to-dir/stdout.log"
        ])
    );
    // Matching is case-insensitive: `MatchOptions::default()` (unlike `MatchOptions::new()`)
    // sets `case_sensitive: false`.
    assert_eq!(run("UPPER/*.log"), strings(&["UPPER/A.LOG"]));
    // A trailing separator matches directories only.
    assert_eq!(run("c/"), strings(&["c"]));
    // Non-existent literals and roots yield nothing.
    assert_eq!(run("missing/*.log"), Vec::<String>::new());
    assert!(
        reference_matcher()
            .match_root(
                root.join("no-such-root").to_str().unwrap(),
                &strings(&["*"])
            )
            .is_empty()
    );
    // Overlapping patterns keep per-pattern order and duplicates.
    assert_eq!(
        rel(
            root,
            reference_matcher().match_root(root.to_str().unwrap(), &strings(&["?.log", "a.log"]))
        ),
        strings(&["a.log", "b.log", "a.log"])
    );
}

#[test]
fn equivalence_on_edge_case_tree() {
    let tmp = edge_case_tree();
    let patterns = strings(EDGE_CASE_PATTERNS);
    let found = assert_match_root_equivalent(tmp.path(), &patterns);
    assert!(found.len() > 40, "tree should exercise many matches");
    assert_match_root_equivalent(&tmp.path().join("missing-root"), &patterns);
    assert_match_root_equivalent(&tmp.path().join("a.log"), &patterns);
}

#[test]
fn equivalence_with_unreadable_directory() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = edge_case_tree();
    let locked = tmp.path().join("c/sub");
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    let patterns = strings(EDGE_CASE_PATTERNS);
    assert_match_root_equivalent(tmp.path(), &patterns);
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn equivalence_with_non_utf8_names_and_dangling_dir_links() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    for name in [&b"\xff.log"[..], b"a\xfe", b"\xfedir"] {
        let path = root.join(OsStr::from_bytes(name));
        if name.ends_with(b"dir") {
            fs::create_dir(&path).unwrap();
            fs::write(path.join("x"), b"x").unwrap();
        } else {
            fs::write(path, b"x").unwrap();
        }
    }
    touch(root, "a.log");
    touch(root, "d/x");
    symlink(root, "never-existed", "gone-link");
    mkdir(root, "removed");
    symlink(root, "removed", "removed-link");
    fs::remove_dir(root.join("removed")).unwrap();
    let patterns = strings(&[
        "*",
        "*.log",
        "?.log",
        "*/x",
        "*/",
        "a*",
        "gone-link/x",
        "d/x",
    ]);
    let found = assert_match_root_equivalent(root, &patterns);
    assert!(found.iter().any(|path| path.ends_with("gone-link")));
    assert!(found.iter().any(|path| path.ends_with("d/x")));
}

#[test]
fn equivalence_on_unnormalized_scan_roots() {
    let tmp = edge_case_tree();
    let patterns = strings(EDGE_CASE_PATTERNS);
    let root = tmp.path().to_str().unwrap();
    for dir in [
        format!("{root}/"),
        format!("{root}//c"),
        format!("{root}/c/.."),
        format!("{root}/./c"),
    ] {
        assert_match_root_equivalent(Path::new(&dir), &patterns);
    }
}

#[test]
fn new_subdir_and_file_appear_together_within_max_age() {
    use std::sync::atomic::Ordering;

    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    touch(root, "a/x.log");
    let dir = root.to_str().unwrap();
    let clock = ManualClock::new();
    let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);
    let age = MAX_LISTING_AGE;
    let mut cached = ListingScanRootMatcher::with_cache(Arc::clone(&cache), age);
    let patterns = strings(&["*/*.log"]);
    let reference = |patterns: &[String]| reference_matcher().match_root(dir, patterns);

    // Only the root is listed so far; `a` is listed fresh by the next pass.
    assert_eq!(
        cached.match_root(dir, &strings(&["*"])),
        reference(&strings(&["*"]))
    );
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    touch(root, "b/y.log");
    clock.advance(age);
    assert_eq!(rel(root, cached.match_root(dir, &patterns)), ["a/x.log"]);
    clock.advance(Duration::from_nanos(1));
    let found = cached.match_root(dir, &patterns);
    assert_eq!(found, reference(&patterns));
    assert_eq!(rel(root, found), ["a/x.log", "b/y.log"]);

    // A listed directory replaced by a file: once both listings expire, nothing phantom remains.
    fs::remove_dir_all(root.join("a")).unwrap();
    fs::write(root.join("a"), b"x").unwrap();
    clock.advance(age + Duration::from_nanos(1));
    let found = cached.match_root(dir, &patterns);
    assert_eq!(found, reference(&patterns));
    assert_eq!(rel(root, found), ["b/y.log"]);
}

#[test]
fn transient_listing_errors_are_retried_by_later_patterns() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let tmp = edge_case_tree();
    let dir = tmp.path().to_str().unwrap();
    let failed_once = Arc::new(AtomicBool::new(false));
    let cache = {
        let failed_once = Arc::clone(&failed_once);
        cache_with_reader(&ManualClock::new(), move |path| {
            if failed_once.swap(true, Ordering::SeqCst) {
                read_listing(path)
            } else {
                Err(std::io::ErrorKind::Other)
            }
        })
    };
    let mut matcher = ListingScanRootMatcher::with_cache(cache, MAX_LISTING_AGE);
    let patterns = strings(&["a.log", "*.log"]);
    // The first read of the root fails; the patterns still see the real listing.
    assert_eq!(
        matcher.match_root(dir, &patterns),
        reference_matcher().match_root(dir, &patterns)
    );
    assert!(failed_once.load(Ordering::SeqCst));
}

const NAME_ALPHABET: &[&str] = &[
    "a", "b", "ab", ".a", "_", "-", "0", "1", "x.log", "y.json", "A.LOG", ".log",
];
const SEGMENT_TOKENS: &[&str] = &[
    "*", "?", "a", "b", ".", "_", "-", "0", "[ab]", "[!a]", "[0-1]", ".log", ".json", "x", "A", "y",
];

fn random_name(rng: &mut SmallRng) -> String {
    (0..rng.random_range(1..=3))
        .map(|_| NAME_ALPHABET[rng.random_range(0..NAME_ALPHABET.len())])
        .collect()
}

fn random_tree(rng: &mut SmallRng, dir: &Path, depth: usize) {
    for _ in 0..rng.random_range(0..6) {
        let name = random_name(rng);
        let path = dir.join(&name);
        if path.exists() || fs::symlink_metadata(&path).is_ok() {
            continue;
        }
        match rng.random_range(0..10) {
            0..=4 => fs::write(&path, b"x").unwrap(),
            5..=7 if depth < 3 => {
                fs::create_dir(&path).unwrap();
                random_tree(rng, &path, depth + 1);
            }
            8 => std::os::unix::fs::symlink(random_name(rng), &path).unwrap(),
            _ => fs::create_dir(&path).unwrap(),
        }
    }
}

fn random_pattern(rng: &mut SmallRng) -> String {
    let segments: Vec<String> = (0..rng.random_range(1..=3))
        .map(|_| {
            if rng.random_range(0..12) == 0 {
                return "**".to_owned();
            }
            (0..rng.random_range(1..=3))
                .map(|_| SEGMENT_TOKENS[rng.random_range(0..SEGMENT_TOKENS.len())])
                .collect()
        })
        .collect();
    let mut pattern = segments.join("/");
    if rng.random_range(0..10) == 0 {
        pattern.push('/');
    }
    pattern
}

#[test]
fn randomized_equivalence() {
    let (mut walked, mut total) = (0, 0);
    for seed in 0..300_u64 {
        let mut rng = SmallRng::seed_from_u64(seed);
        let tmp = TempDir::new().unwrap();
        random_tree(&mut rng, tmp.path(), 0);
        let patterns: Vec<String> = (0..rng.random_range(1..=8))
            .map(|_| random_pattern(&mut rng))
            .filter(|pattern| glob::Pattern::new(pattern).is_ok())
            .collect();
        total += patterns.len();
        walked += patterns
            .iter()
            .filter(|pattern| walks_without_glob(pattern))
            .count();
        assert_match_root_equivalent(tmp.path(), &patterns);
    }
    // Most patterns must take the listing walk rather than the glob fallback.
    assert!(
        walked * 4 > total * 3,
        "walked {walked} of {total} patterns"
    );
}

#[test]
fn edge_case_patterns_mostly_walk() {
    let fallbacks: Vec<_> = EDGE_CASE_PATTERNS
        .iter()
        .filter(|pattern| !walks_without_glob(pattern))
        .collect();
    assert_eq!(fallbacks, [&".*", &"*/.*", &"**/*.log"]);
}

#[test]
fn missing_and_unlistable_scan_roots() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = edge_case_tree();
    let root = tmp.path();
    let patterns = strings(&[
        "*.log",
        "c/stdout.log",
        "c/*",
        "a.log",
        "dirlike.log/",
        "**/*.log",
    ]);
    // A file, a dangling symlink and a missing path as the scan root all match nothing.
    for missing in ["a.log", "dangling.log", "nope", "nope/deeper"] {
        assert!(assert_match_root_equivalent(&root.join(missing), &patterns).is_empty());
    }
    // A traversable but unlistable root still resolves literal segments by name.
    fs::set_permissions(root.join("c"), fs::Permissions::from_mode(0o311)).unwrap();
    let found = assert_match_root_equivalent(
        &root.join("c"),
        &strings(&["stdout.log", "sub/deep.log", "sub/*", "*.log"]),
    );
    fs::set_permissions(root.join("c"), fs::Permissions::from_mode(0o755)).unwrap();
    if !running_as_root() {
        assert_eq!(
            rel(root, found),
            strings(&[
                "c/stdout.log",
                "c/sub/deep.log",
                "c/sub/deep.log",
                "c/sub/deeper"
            ])
        );
    }
}

fn running_as_root() -> bool {
    std::fs::metadata("/proc/self").is_ok_and(|meta| {
        use std::os::unix::fs::MetadataExt;
        meta.uid() == 0
    })
}

#[test]
fn existing_subdirs_match_reference() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = edge_case_tree();
    let root = tmp.path();
    let names = [
        "c",
        "a.log",
        "dirlike.log",
        "link-to-dir",
        "link-to-file.log",
        "dangling.log",
        "nope",
    ];
    // Unlistable but traversable: lookups by name must still find the volume directory.
    let locked = root.join("locked");
    fs::create_dir_all(locked.join("c")).unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o311)).unwrap();
    let locked_is_unlistable = fs::read_dir(&locked).is_err();
    let clock = ManualClock::new();
    let mut results = Vec::new();
    for base in [
        root.to_path_buf(),
        root.join("c"),
        root.join("nope"),
        root.join("a.log"),
        locked.clone(),
    ] {
        let expected = GlobScanRootMatcher(real_glob).existing_subdirs(&base, &names);
        let mut cached = cached_matcher(&clock);
        let actual = [
            ListingScanRootMatcher::default().existing_subdirs(&base, &names),
            cached.existing_subdirs(&base, &names),
            cached.existing_subdirs(&base, &names),
        ];
        results.push((base, expected, actual));
    }
    // Restore before asserting so the temp dir can always be cleaned up.
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(running_as_root() || locked_is_unlistable);
    for (base, expected, actual) in &results {
        assert!(
            actual.iter().all(|actual| actual == expected),
            "{}: {actual:?} != {expected:?}",
            base.display()
        );
    }
    assert_eq!(results.last().unwrap().1, [locked.join("c")]);
}

fn pod(name: &str, uid: &str, annotations: &[(&str, &str)]) -> Pod {
    Pod {
        metadata: ObjectMeta {
            namespace: Some("ns".to_owned()),
            name: Some(name.to_owned()),
            uid: Some(uid.to_owned()),
            annotations: Some(
                annotations
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            ..ObjectMeta::default()
        },
        ..Pod::default()
    }
}

fn provider(
    pods: Vec<Pod>,
    patterns: &[&str],
    extract_databricks_logs: bool,
    annotation_selector: &str,
) -> K8sPathsProvider {
    let mut pod_writer = Writer::<Pod>::default();
    for pod in pods {
        pod_writer.apply_watcher_event(&watcher::Event::Apply(pod));
    }
    let namespace_writer = Writer::<Namespace>::default();
    K8sPathsProvider::new(
        pod_writer.as_reader(),
        namespace_writer.as_reader(),
        AnnotationSelector::parse(annotation_selector).unwrap(),
        strings(patterns),
        vec![glob::Pattern::new("/**").unwrap()],
        vec![glob::Pattern::new("/**/*.gz").unwrap()],
        false,
        extract_databricks_logs,
        Some(DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY.to_owned()),
    )
}

#[test]
fn provider_equivalence_on_databricks_tree() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    let empty_dir = |uid: &str, rest: &str| {
        format!("var/lib/kubelet/pods/{uid}/volumes/kubernetes.io~empty-dir/{rest}")
    };
    for file in [
        empty_dir("uid-a", "logs/svc/stdout.log"),
        empty_dir("uid-a", "logs/svc/stdout.log.1.gz"),
        empty_dir("uid-a", "logs/excluded/stdout.log"),
        empty_dir("uid-a", "data/app/access.log"),
        empty_dir("uid-a", "event-logs/e/events.json"),
        empty_dir("uid-a", "not-a-volume/x/stdout.log"),
        empty_dir("uid-b", "payload-logs/p/stdout.log"),
        "databricks/host-root/sandbox/pod-b/c/stdout.log".to_owned(),
        "databricks/host-root/sandbox/pod-b/c/.hidden.log".to_owned(),
        "databricks/host-root/static/c/access.log".to_owned(),
    ] {
        touch(root, &file);
    }
    // `logs` exists only as a file for uid-c; it is not a scan root.
    touch(root, &empty_dir("uid-c", "logs"));

    let pods = vec![
        pod(
            "pod-a",
            "uid-a",
            &[(CONTAINER_EXCLUSION_ANNOTATION_KEY, "excluded")],
        ),
        pod(
            "pod-b-10.0.0.1",
            "uid-b",
            &[
                (POD_NAME_ANNOTATION_KEY, "pod-b"),
                (
                    DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY,
                    "/sandbox/$POD_NAME",
                ),
            ],
        ),
        pod(
            "pod-c",
            "uid-c",
            &[(DATABRICKS_HOSTPATH_LOGGING_ANNOTATION_KEY, "static")],
        ),
        pod("pod-d", "uid-missing", &[]),
    ];
    let patterns = ["*/*.log*", "*/*.json", "*/stdout.log", "c/*", "**/*.log"];
    let provider = provider(pods, &patterns, true, "");

    let expected = provider.paths_with(&mut Rebased {
        root,
        inner: reference_matcher(),
    });
    let actual = provider.paths_with(&mut Rebased {
        root,
        inner: candidate_matcher(),
    });
    assert_eq!(actual, expected);

    let mut found: Vec<_> = expected
        .iter()
        .map(|(_, path)| path.to_str().unwrap())
        .collect();
    found.sort_unstable();
    found.dedup();
    let prefix = "/var/lib/kubelet/pods";
    let ed = "volumes/kubernetes.io~empty-dir";
    assert_eq!(
        found,
        [
            "/databricks/host-root/sandbox/pod-b/c/.hidden.log".to_owned(),
            "/databricks/host-root/sandbox/pod-b/c/stdout.log".to_owned(),
            "/databricks/host-root/static/c/access.log".to_owned(),
            format!("{prefix}/uid-a/{ed}/data/app/access.log"),
            format!("{prefix}/uid-a/{ed}/event-logs/e/events.json"),
            format!("{prefix}/uid-a/{ed}/logs/svc/stdout.log"),
            format!("{prefix}/uid-b/{ed}/payload-logs/p/stdout.log"),
        ]
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
    );
    // Overlapping patterns produce duplicates, which must be preserved.
    assert!(expected.len() > found.len());
}

#[test]
fn providers_share_listings_until_they_expire() {
    use std::sync::atomic::Ordering;

    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    let empty_dir = |uid: &str, rest: &str| {
        format!("var/lib/kubelet/pods/{uid}/volumes/kubernetes.io~empty-dir/{rest}")
    };
    touch(root, &empty_dir("uid-a", "logs/svc/stdout.log"));
    touch(root, &empty_dir("uid-a", "data/app/access.log"));
    let pods = || vec![pod("pod-a", "uid-a", &[]), pod("pod-x", "uid-missing", &[])];
    // Two sources with different patterns over the same pod store.
    let first = provider(pods(), &["*/*.log", "svc/*"], true, "");
    let second = provider(pods(), &["*/*.log", "app/*.log"], true, "");

    let clock = ManualClock::new();
    let (cache, reads) = counting_cache(&clock, MAX_ENTRIES);
    let age = Duration::from_secs(2);
    let run = |provider: &K8sPathsProvider| {
        let cached = provider.paths_with(&mut Rebased {
            root,
            inner: ListingScanRootMatcher::with_cache(Arc::clone(&cache), age),
        });
        let reference = provider.paths_with(&mut Rebased {
            root,
            inner: reference_matcher(),
        });
        assert_eq!(cached, reference);
        cached.len()
    };

    assert_eq!(run(&first), 3);
    let after_first = reads.load(Ordering::SeqCst);
    assert!(after_first > 0);
    assert_eq!(run(&second), 3);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        after_first,
        "second source re-read"
    );

    // A file created right after the listings were taken stays hidden for at most `age`.
    touch(root, &empty_dir("uid-a", "logs/svc/new.log"));
    clock.advance(age);
    let stale = first.paths_with(&mut Rebased {
        root,
        inner: ListingScanRootMatcher::with_cache(Arc::clone(&cache), age),
    });
    assert_eq!(stale.len(), 3);
    clock.advance(Duration::from_nanos(1));
    assert_eq!(run(&first), 5);
    assert_eq!(run(&second), 4);

    // A pod directory deleted from disk disappears once its listing expires.
    fs::remove_dir_all(root.join(empty_dir("uid-a", ""))).unwrap();
    clock.advance(age + Duration::from_nanos(1));
    assert_eq!(run(&first), 0);
    assert_eq!(run(&second), 0);
}

#[test]
fn provider_equivalence_on_kubelet_tree_with_selector() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    for file in [
        "var/log/pods/ns_pod-a_uid-a/c1/0.log",
        "var/log/pods/ns_pod-a_uid-a/c1/1.log",
        "var/log/pods/ns_pod-a_uid-a/c2/0.log",
        "var/log/pods/ns_pod-b_uid-b/c1/0.log",
    ] {
        touch(root, file);
    }
    let pods = vec![
        pod(
            "pod-a",
            "uid-a",
            &[("logs", "yes"), (CONTAINER_EXCLUSION_ANNOTATION_KEY, "c2")],
        ),
        pod("pod-b", "uid-b", &[]),
    ];
    let provider = provider(pods, &["*/*.log*", "c1/*"], false, "logs=yes");

    let expected = provider.paths_with(&mut Rebased {
        root,
        inner: reference_matcher(),
    });
    let actual = provider.paths_with(&mut Rebased {
        root,
        inner: candidate_matcher(),
    });
    assert_eq!(actual, expected);
    let found: Vec<_> = expected
        .iter()
        .map(|(info, path)| {
            (
                info.as_ref().unwrap().container_name.as_str(),
                path.to_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        found,
        vec![
            ("c1", "/var/log/pods/ns_pod-a_uid-a/c1/0.log"),
            ("c1", "/var/log/pods/ns_pod-a_uid-a/c1/1.log"),
            ("c1", "/var/log/pods/ns_pod-a_uid-a/c1/0.log"),
            ("c1", "/var/log/pods/ns_pod-a_uid-a/c1/1.log"),
        ]
    );
}
