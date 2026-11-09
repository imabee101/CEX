//! Project tooling for the Scion workspace, run with `cargo xtask`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

/// Seconds each fuzz target runs in the full tier.
const FUZZ_BUDGET_SECONDS: u64 = 30 * 60;

/// The crate whose release tags are the baseline for `buf breaking`.
const PROTO_CRATE: &str = "scion-proto";
const PROTO_DIR: &str = "crates/scion-proto/proto";
const PROTO_GEN_DIR: &str = "crates/scion-proto/src/gen";

/// Source files cargo-mutants never mutates: test support compiled only
/// with the `testing` feature.
const MUTANTS_EXCLUDE: &[&str] = &[
    "crates/store/src/testing.rs",
    "crates/store/src/fault.rs",
    "crates/store/src/conformance/**",
    "crates/store/src/blob/memory.rs",
    "crates/store/src/meta/memory.rs",
];

/// scion-core's language-neutral test vectors, used as fuzz seeds.
const VECTORS_FILE: &str = "crates/scion-core/vectors/v1.txt";

#[derive(Parser)]
#[command(about = "Scion project tooling")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start Postgres and the object store, waiting until both are healthy.
    DevUp,
    /// Stop the dev environment and discard its data.
    DevDown,
    /// Run the fast check tier.
    Ci {
        /// Run only these steps.
        #[arg(long, value_enum)]
        only: Vec<Step>,
    },
    /// Run the full check tier: the fast tier's steps and the full tier's own.
    CiFull {
        /// Run only these steps.
        #[arg(long, value_enum)]
        only: Vec<Step>,
        /// Run only shard K of N of the fuzz targets, written `K/N`, so that
        /// N machines share the fuzz step.
        #[arg(long, value_parser = parse_shard)]
        fuzz_shard: Option<Shard>,
    },
    /// Run cargo-mutants with nextest on the diff against main, testing each
    /// mutant with the whole workspace's fast tests.
    Mutants {
        /// Extra arguments for cargo-mutants.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<OsString>,
    },
    /// Regenerate scion-proto's committed code.
    Proto {
        /// Fail if the committed code is stale instead of rewriting it.
        #[arg(long)]
        check: bool,
    },
    /// Run one fuzz target for a duration such as `90s`, `30m`, or `2h`.
    Fuzz { target: String, duration: String },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum Step {
    Fmt,
    Build,
    Clippy,
    /// Tests outside the slow group.
    Test,
    /// The slow group.
    TestSlow,
    Doctest,
    Doc,
    Deny,
    Semver,
    Buf,
    ProtoCheck,
    Fuzz,
    Msrv,
    /// Clippy over the workspace, and the portable crates' tests and doc
    /// tests: the checks CI also runs on macOS and Windows, where the dev
    /// environment does not run.
    Portable,
}

const FAST_TIER: &[Step] = &[
    Step::Fmt,
    Step::Build,
    Step::Clippy,
    Step::Test,
    Step::Doctest,
    Step::Doc,
    Step::Deny,
    Step::Semver,
    Step::Buf,
    Step::ProtoCheck,
];

/// The steps the full tier adds to the fast tier's.
const FULL_ONLY: &[Step] = &[Step::TestSlow, Step::Fuzz, Step::Msrv, Step::Portable];

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Cmd::DevUp => dev_up(),
        Cmd::DevDown => dev_down(),
        Cmd::Ci { only } => run_tier("ci", FAST_TIER, &only, None),
        Cmd::CiFull { only, fuzz_shard } => {
            let steps: Vec<Step> = FAST_TIER.iter().chain(FULL_ONLY).copied().collect();
            if fuzz_shard.is_some() && !(only.is_empty() || only.contains(&Step::Fuzz)) {
                Err("ci-full: --fuzz-shard needs the fuzz step".into())
            } else {
                run_tier("ci-full", &steps, &only, fuzz_shard)
            }
        }
        Cmd::Mutants { args } => mutants(&args),
        Cmd::Proto { check } => proto(check),
        Cmd::Fuzz { target, duration } => parse_duration(&duration).and_then(|d| fuzz(&target, d)),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xtask: error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the workspace root")
        .to_path_buf()
}

/// CI reports every failure; local runs stop at the first.
fn in_ci() -> bool {
    std::env::var_os("CI").is_some_and(|v| !v.is_empty() && v != "false")
}

/// A command run from the workspace root, using sccache when it is installed.
fn cmd(program: &str) -> Command {
    let mut c = Command::new(program);
    c.current_dir(root());
    if std::env::var_os("RUSTC_WRAPPER").is_none() && sccache_installed() {
        c.env("RUSTC_WRAPPER", "sccache");
    }
    c
}

fn sccache_installed() -> bool {
    Command::new("sccache")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn cargo<I, S>(args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut c = cmd("cargo");
    c.args(args);
    c
}

fn run(mut c: Command) -> Result {
    let shown = format!(
        "{} {}",
        c.get_program().to_string_lossy(),
        c.get_args()
            .map(|a| a.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );
    eprintln!("$ {shown}");
    let status = c
        .status()
        .map_err(|e| format!("cannot run `{shown}`: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{shown}` failed: {status}").into())
    }
}

fn output(mut c: Command) -> Result<String> {
    let out = c.output()?;
    if !out.status.success() {
        return Err(format!(
            "{:?} failed: {}",
            c.get_program(),
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(out.stdout)?)
}

fn read_toml(path: &Path) -> Result<toml::Table> {
    Ok(std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?
        .parse()?)
}

// ---------------------------------------------------------------- dev env

fn compose() -> Command {
    let mut c = cmd("docker");
    c.args(["compose", "--file", "compose.yaml"]);
    c
}

fn dev_up() -> Result {
    // `up` reuses running containers; `--wait` returns once both health checks pass.
    let mut c = compose();
    c.args(["up", "--detach", "--wait", "--wait-timeout", "180"]);
    run(c)
}

fn dev_down() -> Result {
    let mut c = compose();
    c.args(["down", "--volumes"]);
    run(c)
}

// ---------------------------------------------------------------- tiers

fn run_tier(name: &str, steps: &[Step], only: &[Step], fuzz_shard: Option<Shard>) -> Result {
    if let Some(step) = only.iter().find(|s| !steps.contains(s)) {
        return Err(format!("{name}: step {step:?} is not part of this tier").into());
    }
    let selected: Vec<Step> = if only.is_empty() {
        steps.to_vec()
    } else {
        only.to_vec()
    };
    let keep_going = in_ci();
    let start = Instant::now();
    let mut failed = Vec::new();
    for step in selected {
        eprintln!("==> {name}: {step:?}");
        let t = Instant::now();
        let result = run_step(step, fuzz_shard);
        let secs = t.elapsed().as_secs_f64();
        match result {
            Ok(()) => eprintln!("<== {step:?} ok ({secs:.1}s)"),
            Err(e) => {
                eprintln!("<== {step:?} FAILED ({secs:.1}s): {e}");
                failed.push(step);
                if !keep_going {
                    break;
                }
            }
        }
    }
    let total = start.elapsed().as_secs_f64();
    if failed.is_empty() {
        eprintln!("{name}: all steps passed in {total:.1}s");
        Ok(())
    } else {
        Err(format!("{name}: failed steps {failed:?} after {total:.1}s").into())
    }
}

/// The nextest profile for a test step: CI's profiles report every failure
/// and size the run to the machine.
fn nextest_profile(slow: bool) -> &'static str {
    match (slow, in_ci()) {
        (false, false) => "default",
        (false, true) => "ci",
        (true, false) => "slow",
        (true, true) => "ci-slow",
    }
}

fn clippy() -> Result {
    run(cargo([
        "clippy",
        "--workspace",
        "--all-targets",
        "--locked",
        "--",
        "-D",
        "warnings",
    ]))
}

fn run_step(step: Step, fuzz_shard: Option<Shard>) -> Result {
    match step {
        Step::Fmt => run(cargo(["fmt", "--all", "--check"])),
        Step::Build => run(cargo(["build", "--workspace", "--all-targets", "--locked"])),
        Step::Clippy => clippy(),
        Step::Test => nextest(false, &["--workspace"]),
        Step::TestSlow => nextest(true, &["--workspace"]),
        Step::Doctest => run(cargo(["test", "--doc", "--workspace", "--locked"])),
        Step::Doc => {
            let mut c = cargo(["doc", "--workspace", "--no-deps", "--locked"]);
            c.env("RUSTDOCFLAGS", "-D warnings");
            run(c)
        }
        Step::Deny => run(cargo(["deny", "--locked", "check"])),
        Step::Semver => semver(),
        Step::Buf => buf_breaking(),
        Step::ProtoCheck => proto(true),
        Step::Fuzz => fuzz_all(fuzz_shard.unwrap_or(Shard { index: 1, count: 1 })),
        Step::Msrv => {
            // Type checking finds every use of a language or library feature
            // newer than the MSRV; code generation adds nothing to that.
            let msrv = msrv()?;
            install_toolchain(&msrv)?;
            run(cargo([
                format!("+{msrv}").as_str(),
                "check",
                "--workspace",
                "--all-targets",
                "--locked",
            ]))
        }
        Step::Portable => {
            clippy()?;
            let crates = portable_crates()?;
            let mut packages = Vec::new();
            for krate in &crates {
                packages.extend(["--package", krate.as_str()]);
            }
            let mut tests = vec!["--no-tests=warn"];
            tests.extend(&packages);
            nextest(false, &tests)?;
            let mut doctests = cargo(["test", "--doc", "--locked"]);
            doctests.args(&packages);
            run(doctests)
        }
    }
}

fn nextest(slow: bool, args: &[&str]) -> Result {
    let mut c = cargo([
        "nextest",
        "run",
        "--locked",
        "--profile",
        nextest_profile(slow),
    ]);
    c.args(args);
    run(c)
}

fn msrv() -> Result<String> {
    let manifest = read_toml(&root().join("Cargo.toml"))?;
    manifest
        .get("workspace")
        .and_then(|w| w.get("package"))
        .and_then(|p| p.get("rust-version"))
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| "workspace.package.rust-version is missing".into())
}

/// A workspace member's name and manifest.
struct Member {
    name: String,
    /// Its directory, relative to the workspace root, with `/` separators.
    dir: String,
    manifest: toml::Table,
}

/// Every crate in the workspace's `crates/` and `bins/` directories.
fn members() -> Result<Vec<Member>> {
    let mut out = Vec::new();
    for dir in ["crates", "bins"] {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(root().join(dir))?
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<_, _>>()?;
        entries.sort();
        for path in entries {
            let manifest_path = path.join("Cargo.toml");
            if !manifest_path.exists() {
                continue;
            }
            let manifest = read_toml(&manifest_path)?;
            let name = manifest
                .get("package")
                .and_then(|p| p.get("name"))
                .and_then(|n| n.as_str())
                .ok_or_else(|| format!("{}: no package name", manifest_path.display()))?
                .to_owned();
            out.push(Member {
                name,
                dir: format!(
                    "{dir}/{}",
                    path.file_name()
                        .expect("a directory entry")
                        .to_string_lossy()
                ),
                manifest,
            });
        }
    }
    Ok(out)
}

/// The crates published with a semver promise: those whose manifest leaves
/// `publish` unset (Cargo's default, true) or sets it to true, rather than
/// inheriting the workspace's `publish = false` or setting false.
fn public_crates() -> Result<Vec<String>> {
    Ok(members()?
        .into_iter()
        .filter(|m| {
            match m.manifest.get("package").and_then(|p| p.get("publish")) {
                None => true,
                Some(toml::Value::Boolean(publish)) => *publish,
                // `publish.workspace = true` inherits the workspace's false;
                // a registry list publishes.
                Some(toml::Value::Array(registries)) => !registries.is_empty(),
                Some(_) => false,
            }
        })
        .map(|m| m.name)
        .collect())
}

/// The crates whose tests also run on macOS and Windows in the full tier:
/// every public crate, and any crate that sets
/// `package.metadata.scion.portable-tests = true`.
fn portable_crates() -> Result<Vec<String>> {
    let public = public_crates()?;
    Ok(members()?
        .into_iter()
        .filter(|m| {
            public.contains(&m.name)
                || m.manifest
                    .get("package")
                    .and_then(|p| p.get("metadata"))
                    .and_then(|m| m.get("scion"))
                    .and_then(|s| s.get("portable-tests"))
                    .and_then(toml::Value::as_bool)
                    == Some(true)
        })
        .map(|m| m.name)
        .collect())
}

// ---------------------------------------------------------------- releases

/// The newest release tag `<krate>-v<semver>`, or `None` if never released.
fn last_release(krate: &str) -> Result<Option<String>> {
    let mut c = cmd("git");
    c.args(["tag", "--list", &format!("{krate}-v*")]);
    let tags = output(c).map_err(|e| format!("cannot list release tags: {e}"))?;
    let prefix = format!("{krate}-v");
    let mut best: Option<(Vec<u64>, String)> = None;
    for tag in tags.lines() {
        let Some(version) = tag.strip_prefix(&prefix) else {
            continue;
        };
        let parts: Option<Vec<u64>> = version.split('.').map(|p| p.parse().ok()).collect();
        let Some(parts) = parts.filter(|p| p.len() == 3) else {
            return Err(format!("release tag `{tag}` is not `{prefix}MAJOR.MINOR.PATCH`").into());
        };
        if best.as_ref().is_none_or(|(b, _)| parts > *b) {
            best = Some((parts, tag.to_owned()));
        }
    }
    Ok(best.map(|(_, tag)| tag))
}

fn semver() -> Result {
    for krate in public_crates()? {
        match last_release(&krate)? {
            None => eprintln!("semver-checks: {krate} skipped (never released)"),
            Some(tag) => run(cargo([
                "semver-checks",
                "check-release",
                "--package",
                &krate,
                "--baseline-rev",
                &tag,
            ]))?,
        }
    }
    Ok(())
}

fn buf_breaking() -> Result {
    match last_release(PROTO_CRATE)? {
        None => {
            eprintln!("buf breaking: scion.v1 skipped (never released)");
            Ok(())
        }
        Some(tag) => {
            let mut c = cmd("buf");
            c.args([
                "breaking",
                PROTO_DIR,
                "--against",
                &format!(".git#tag={tag},subdir={PROTO_DIR}"),
            ]);
            run(c)
        }
    }
}

// ---------------------------------------------------------------- proto

fn proto(check: bool) -> Result {
    let root = root();
    let proto_dir = root.join(PROTO_DIR);
    if !proto_dir.exists() {
        // The scion.v1 proto arrives with scion-proto in milestone 5; until
        // then there is no generated code to be stale.
        if check {
            eprintln!("proto: {PROTO_DIR} does not exist yet; nothing to check");
            return Ok(());
        }
        return Err(format!("proto: {PROTO_DIR} does not exist").into());
    }
    let mut files = Vec::new();
    collect_files(&proto_dir, "proto", &mut files)?;
    let files: Vec<PathBuf> = files
        .iter()
        .map(|f| f.strip_prefix(&proto_dir).map(Path::to_path_buf))
        .collect::<Result<_, _>>()?;

    let out = root.join("target/xtask/proto");
    if out.exists() {
        std::fs::remove_dir_all(&out)?;
    }
    std::fs::create_dir_all(&out)?;

    let fds = protox::compile(&files, [&proto_dir])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .client_mod_attribute(".", r#"#[cfg(feature = "client")]"#)
        .server_mod_attribute(".", r#"#[cfg(feature = "server")]"#)
        .out_dir(&out)
        .compile_fds(fds)?;

    let mut generated = Vec::new();
    collect_files(&out, "rs", &mut generated)?;
    if !generated.is_empty() {
        let mut c = cmd("rustfmt");
        c.args(["--edition", "2024"]).args(&generated);
        run(c)?;
    }

    let committed_dir = root.join(PROTO_GEN_DIR);
    let mut committed = Vec::new();
    if committed_dir.exists() {
        collect_files(&committed_dir, "rs", &mut committed)?;
    }
    let names = |paths: &[PathBuf], base: &Path| -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = paths
            .iter()
            .map(|p| p.strip_prefix(base).unwrap_or(p).to_path_buf())
            .collect();
        v.sort();
        v
    };
    let fresh_names = names(&generated, &out);
    let stale = fresh_names != names(&committed, &committed_dir)
        || fresh_names.iter().try_fold(false, |stale, name| {
            Ok::<_, std::io::Error>(
                stale || std::fs::read(out.join(name))? != std::fs::read(committed_dir.join(name))?,
            )
        })?;

    if check {
        if stale {
            return Err(format!("{PROTO_GEN_DIR} is stale; run `cargo xtask proto`").into());
        }
        eprintln!("{PROTO_GEN_DIR} is up to date");
        return Ok(());
    }
    if stale {
        if committed_dir.exists() {
            std::fs::remove_dir_all(&committed_dir)?;
        }
        std::fs::create_dir_all(&committed_dir)?;
        for name in &fresh_names {
            std::fs::copy(out.join(name), committed_dir.join(name))?;
        }
        eprintln!("regenerated {PROTO_GEN_DIR}");
    } else {
        eprintln!("{PROTO_GEN_DIR} is already up to date");
    }
    Ok(())
}

/// Collects every file with extension `ext` under `dir`, recursively.
fn collect_files(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) -> Result {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_files(&path, ext, out)?;
        } else if path.extension().is_some_and(|e| e == ext) {
            out.push(path);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- fuzz

fn fuzz_toolchain() -> Result<String> {
    let file = read_toml(&root().join("fuzz/rust-toolchain.toml"))?;
    file.get("toolchain")
        .and_then(|t| t.get("channel"))
        .and_then(|c| c.as_str())
        .map(str::to_owned)
        .ok_or_else(|| "fuzz/rust-toolchain.toml has no toolchain.channel".into())
}

fn fuzz_targets() -> Result<Vec<String>> {
    let manifest = read_toml(&root().join("fuzz/Cargo.toml"))?;
    let bins = manifest.get("bin").and_then(|b| b.as_array());
    Ok(bins
        .into_iter()
        .flatten()
        .filter_map(|b| b.get("name").and_then(|n| n.as_str()).map(str::to_owned))
        .collect())
}

/// One of `count` disjoint parts of a list, numbered from 1.
#[derive(Clone, Copy, Debug)]
struct Shard {
    index: usize,
    count: usize,
}

fn parse_shard(s: &str) -> Result<Shard, String> {
    let parsed = s
        .split_once('/')
        .and_then(|(k, n)| Some((k.parse().ok()?, n.parse().ok()?)));
    match parsed {
        Some((index, count)) if 1 <= index && index <= count => Ok(Shard { index, count }),
        _ => Err(format!("invalid shard `{s}`: write K/N with 1 <= K <= N")),
    }
}

/// Paths whose change can change what a fuzz target executes: the fuzz
/// crate, every workspace crate it depends on (directly or not), the
/// lockfile, and the workspace manifest.
fn fuzzed_paths() -> Result<Vec<String>> {
    let members = members()?;
    let mut paths = vec![
        "fuzz/".to_owned(),
        "Cargo.lock".to_owned(),
        "Cargo.toml".to_owned(),
    ];
    let mut pending = workspace_deps(&read_toml(&root().join("fuzz/Cargo.toml"))?, &members);
    let mut seen = Vec::new();
    while let Some(name) = pending.pop() {
        if seen.contains(&name) {
            continue;
        }
        let member = members
            .iter()
            .find(|m| m.name == name)
            .expect("workspace_deps returns members only");
        paths.push(format!("{}/", member.dir));
        pending.extend(workspace_deps(&member.manifest, &members));
        seen.push(name);
    }
    Ok(paths)
}

/// The workspace members a manifest's normal and build dependencies name,
/// on every target.
fn workspace_deps(manifest: &toml::Table, members: &[Member]) -> Vec<String> {
    let tables = ["dependencies", "build-dependencies"];
    let direct = tables.iter().filter_map(|t| manifest.get(*t));
    let per_target = manifest
        .get("target")
        .and_then(toml::Value::as_table)
        .into_iter()
        .flat_map(|targets| targets.values())
        .flat_map(|target| tables.iter().filter_map(|t| target.get(*t)));
    direct
        .chain(per_target)
        .filter_map(toml::Value::as_table)
        .flat_map(|deps| deps.iter())
        .map(|(key, spec)| {
            // A renamed dependency names its crate in `package`.
            spec.get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(key)
                .to_owned()
        })
        .filter(|name| members.iter().any(|m| &m.name == name))
        .collect()
}

/// The ref the diff-based checks compare against: the local `main` branch,
/// or, in a CI checkout that has none, the remote's.
fn main_ref() -> Result<&'static str> {
    for candidate in ["refs/heads/main", "refs/remotes/origin/main"] {
        let mut c = cmd("git");
        c.args(["rev-parse", "--verify", "--quiet", candidate]);
        if c.output()?.status.success() {
            return Ok(candidate);
        }
    }
    Err("neither a local nor a remote main branch exists to diff against".into())
}

/// The files changed against the merge base with main, including
/// uncommitted changes.
fn changed_files() -> Result<Vec<String>> {
    let mut c = cmd("git");
    c.args(["diff", "--name-only", "--merge-base", main_ref()?]);
    let out = output(c).map_err(|e| format!("cannot diff against main: {e}"))?;
    Ok(out.lines().map(str::to_owned).collect())
}

/// The first of `files` that is one of `paths` or lies under one of them
/// (a path ending in `/` is a directory).
fn first_under<'a>(files: &'a [String], paths: &[String]) -> Option<&'a String> {
    files.iter().find(|f| {
        paths.iter().any(|p| {
            if p.ends_with('/') {
                f.starts_with(p.as_str())
            } else {
                *f == p
            }
        })
    })
}

/// Runs the shard's fuzz targets for the full budget each, when the fuzzed
/// code differs from main; fuzzing code main already fuzzed finds nothing
/// that a regression could have introduced. At most one target runs per
/// core, so each target gets its whole budget of CPU time.
fn fuzz_all(shard: Shard) -> Result {
    let changed = changed_files()?;
    let fuzzed = fuzzed_paths()?;
    let Some(reason) = first_under(&changed, &fuzzed) else {
        eprintln!("fuzz: no fuzzed code differs from main; nothing to fuzz");
        return Ok(());
    };
    eprintln!("fuzz: fuzzed code differs from main ({reason})");
    let targets: Vec<String> = fuzz_targets()?
        .into_iter()
        .enumerate()
        .filter(|(i, _)| i % shard.count == shard.index - 1)
        .map(|(_, t)| t)
        .collect();
    if targets.is_empty() {
        return Err(format!("fuzz: shard {}/{} has no targets", shard.index, shard.count).into());
    }
    let budget = Duration::from_secs(FUZZ_BUDGET_SECONDS);
    let setup = fuzz_setup()?;
    run(fuzz_command(&setup, "build", None, None))?;
    let cores = std::thread::available_parallelism()?.get();
    let mut failed = Vec::new();
    for wave in targets.chunks(cores) {
        let mut children: Vec<(&String, std::process::Child)> = Vec::new();
        for target in wave {
            let mut c = fuzz_command(&setup, "run", Some(target), Some(budget));
            eprintln!("$ cargo fuzz run {target} (budget {}s)", budget.as_secs());
            let child = match c.spawn() {
                Ok(child) => child,
                Err(e) => {
                    // Stop the fuzzers already running rather than leave
                    // them for their whole budget. This is cleanup on the way
                    // to reporting the spawn error, so its own failures (a
                    // fuzzer that already exited) change nothing.
                    for (_, mut running) in children {
                        let _ = running.kill();
                        let _ = running.wait();
                    }
                    return Err(format!("cannot start fuzzer `{target}`: {e}").into());
                }
            };
            children.push((target, child));
        }
        for (target, mut child) in children {
            let status = child.wait()?;
            eprintln!("fuzz: {target} finished: {status}");
            if !status.success() {
                failed.push(target.as_str());
            }
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("fuzz targets failed: {failed:?}; reproducers are in fuzz/artifacts").into())
    }
}

fn fuzz(target: &str, duration: Duration) -> Result {
    if !fuzz_targets()?.iter().any(|t| t == target) {
        return Err(format!("unknown fuzz target `{target}`").into());
    }
    let setup = fuzz_setup()?;
    run(fuzz_command(&setup, "run", Some(target), Some(duration)))
}

struct FuzzSetup {
    toolchain: String,
    host: String,
    seeds: PathBuf,
}

/// Installs the pinned nightly, finds its host target, and writes the seed
/// corpus.
fn fuzz_setup() -> Result<FuzzSetup> {
    let channel = fuzz_toolchain()?;
    install_toolchain(&channel)?;
    let toolchain = format!("+{channel}");
    // Prebuilt cargo-fuzz binaries default to the target they were built for
    // (musl), which sanitizers reject, so name the host target.
    let mut c = cmd("rustc");
    c.args([toolchain.as_str(), "-vV"]);
    let host = output(c)?
        .lines()
        .find_map(|l| l.strip_prefix("host: ").map(str::to_owned))
        .ok_or("rustc -vV printed no host")?;
    let seeds = write_fuzz_seeds()?;
    Ok(FuzzSetup {
        toolchain,
        host,
        seeds,
    })
}

fn fuzz_command(
    setup: &FuzzSetup,
    action: &str,
    target: Option<&str>,
    duration: Option<Duration>,
) -> Command {
    let mut c = cargo([
        setup.toolchain.as_str(),
        "fuzz",
        action,
        "--fuzz-dir",
        "fuzz",
        "--target",
        &setup.host,
    ]);
    if let Some(target) = target {
        // The first corpus directory collects new inputs; the second holds
        // the seeds, which are only read.
        let corpus = root().join("fuzz/corpus").join(target);
        c.arg(target).arg(&corpus).arg(&setup.seeds);
    }
    if let Some(d) = duration {
        c.args(["--", &format!("-max_total_time={}", d.as_secs())]);
    }
    c
}

/// Writes every scion-core golden vector as a seed input, so the fuzzers
/// start from valid objects of every kind and from the known rejects.
fn write_fuzz_seeds() -> Result<PathBuf> {
    let vectors = std::fs::read_to_string(root().join(VECTORS_FILE))?;
    let dir = root().join("target/xtask/fuzz-seeds");
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    for target in fuzz_targets()? {
        std::fs::create_dir_all(root().join("fuzz/corpus").join(target))?;
    }
    let mut name = None;
    for line in vectors.lines() {
        if let Some(n) = line.strip_prefix("name: ") {
            name = Some(n.to_owned());
        } else if let Some(id) = line.strip_prefix("id: ") {
            // Ids seed the hex id parser's fuzzer.
            let n = name.as_deref().ok_or("vectors file: id without a name")?;
            std::fs::write(dir.join(format!("{n}.id")), id.trim())?;
        } else if let Some(hex) = line.strip_prefix("bytes:") {
            let n = name.take().ok_or("vectors file: bytes without a name")?;
            std::fs::write(dir.join(n), decode_hex(hex.trim())?)?;
        }
    }
    Ok(dir)
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&s[i..i + 2], 16)?))
        .collect()
}

fn parse_duration(s: &str) -> Result<Duration> {
    let (digits, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("invalid duration `{s}`"))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(format!("invalid duration `{s}`: use s, m, or h").into()),
    };
    if secs == 0 {
        return Err("duration must be positive".into());
    }
    Ok(Duration::from_secs(secs))
}

// ---------------------------------------------------------------- mutants

fn mutants(args: &[OsString]) -> Result {
    let mut c = cmd("git");
    c.args(["diff", "--merge-base", main_ref()?]);
    let diff = output(c).map_err(|e| format!("cannot diff against main: {e}"))?;
    if diff.trim().is_empty() {
        return Err("mutants: no changes against main, so nothing to mutate".into());
    }
    let dir = root().join("target/xtask");
    std::fs::create_dir_all(&dir)?;
    let diff_file = dir.join("mutants.diff");
    std::fs::write(&diff_file, diff)?;
    // Most of a crate's behavior is tested from other crates (derived and
    // store from ops, for example), so every mutant runs the workspace's
    // tests; testing only the mutated package reports caught mutants as
    // missed.
    let mut c = cargo([
        "mutants",
        "--test-tool",
        "nextest",
        "--test-workspace=true",
        // The baseline runs only the mutated packages' tests, so the timeout
        // it sets would be too short for the workspace's.
        "--minimum-test-timeout=120",
        "--in-diff",
    ]);
    c.arg(&diff_file);
    // Test-support code is exercised by the tests, not tested by them, and a
    // mutant that breaks it (for example the database cleanup) only harms
    // the run.
    for glob in MUTANTS_EXCLUDE {
        c.args(["--exclude", glob]);
    }
    c.args(args);
    // The mutant source copies and their builds live under target/, not in
    // the system temp directory, which may be small or in RAM.
    let tmp = root().join("target/mutants-tmp");
    std::fs::create_dir_all(&tmp)?;
    c.env("TMPDIR", &tmp);
    run(c)
}

/// Installs a toolchain other than the pinned development one; a no-op when present.
fn install_toolchain(channel: &str) -> Result {
    let mut c = cmd("rustup");
    c.args(["toolchain", "install", "--profile", "minimal", channel]);
    run(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_are_k_of_n() {
        let s = parse_shard("2/3").unwrap();
        assert_eq!((s.index, s.count), (2, 3));
        assert_eq!(parse_shard("1/1").unwrap().index, 1);
        for bad in ["0/2", "3/2", "1/0", "2", "a/b", "1/2/3", ""] {
            assert!(parse_shard(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn fuzzed_paths_follow_the_fuzz_crates_workspace_dependencies() {
        let paths = fuzzed_paths().unwrap();
        for p in [
            "fuzz/",
            "Cargo.lock",
            "Cargo.toml",
            "crates/scion-core/",
            "crates/derived/",
        ] {
            assert!(paths.iter().any(|q| q == p), "{p} missing from {paths:?}");
        }
        // derived depends on store, so store is fuzzed code too.
        assert!(paths.iter().any(|q| q == "crates/store/"), "{paths:?}");
        // Nothing depends on the bench generator or on ops.
        for p in ["bins/scion-bench/", "crates/ops/", "xtask/"] {
            assert!(!paths.iter().any(|q| q == p), "{p} in {paths:?}");
        }
    }

    #[test]
    fn workspace_deps_include_renamed_build_and_target_dependencies() {
        let members: Vec<Member> = ["a", "b", "c", "d"]
            .iter()
            .map(|n| Member {
                name: (*n).to_owned(),
                dir: format!("crates/{n}"),
                manifest: toml::Table::new(),
            })
            .collect();
        let manifest: toml::Table = r#"
            [dependencies]
            a = { workspace = true }
            renamed = { package = "b", workspace = true }
            outside = "1"
            [build-dependencies]
            c = { workspace = true }
            [dev-dependencies]
            excluded = { package = "a", workspace = true }
            [target.'cfg(unix)'.dependencies]
            d = { workspace = true }
        "#
        .parse()
        .unwrap();
        let mut deps = workspace_deps(&manifest, &members);
        deps.sort();
        assert_eq!(deps, ["a", "b", "c", "d"]);
    }

    #[test]
    fn first_under_matches_files_and_directories_exactly() {
        let paths = ["crates/derived/".to_owned(), "Cargo.lock".to_owned()];
        let files = |fs: &[&str]| fs.iter().map(|f| (*f).to_owned()).collect::<Vec<_>>();
        assert_eq!(
            first_under(&files(&["crates/derived-x/a.rs"]), &paths),
            None
        );
        assert_eq!(
            first_under(&files(&["Cargo.lock.bak", "crates/derive"]), &paths),
            None
        );
        let hit = files(&["README", "crates/derived/src/a.rs"]);
        assert_eq!(first_under(&hit, &paths), Some(&hit[1]));
        let hit = files(&["Cargo.lock"]);
        assert_eq!(first_under(&hit, &paths), Some(&hit[0]));
    }
}
