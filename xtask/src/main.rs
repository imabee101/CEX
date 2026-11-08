//! Project tooling for the Scion workspace, run with `cargo xtask`.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

/// Seconds each fuzz target runs in the full tier.
const FUZZ_BUDGET_SECONDS: u64 = 30 * 60;

/// Crates published with a semver promise.
const PUBLIC_CRATES: &[&str] = &[
    "scion-core",
    "scion-proto",
    "scion-client",
    "scion-conformance",
];

/// Crates whose tests also run on macOS and Windows in the full tier.
const PORTABLE_TEST_CRATES: &[&str] = &[
    "scion-core",
    "scion-proto",
    "scion-client",
    "scion-conformance",
    "vfs",
];

/// The crate whose release tags are the baseline for `buf breaking`.
const PROTO_CRATE: &str = "scion-proto";
const PROTO_DIR: &str = "crates/scion-proto/proto";
const PROTO_GEN_DIR: &str = "crates/scion-proto/src/gen";

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
    /// Run the full check tier.
    CiFull {
        /// Run only these steps.
        #[arg(long, value_enum)]
        only: Vec<Step>,
    },
    /// Run cargo-mutants with nextest on the diff against main.
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
    /// Every test, including the slow group.
    TestAll,
    Doctest,
    Doc,
    Deny,
    Semver,
    Buf,
    ProtoCheck,
    Fuzz,
    Msrv,
    /// Public-crate and vfs tests; the full tier runs these on macOS and Windows too.
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

const FULL_TIER: &[Step] = &[
    Step::Fmt,
    Step::Build,
    Step::Clippy,
    Step::TestAll,
    Step::Doctest,
    Step::Doc,
    Step::Deny,
    Step::Semver,
    Step::Buf,
    Step::ProtoCheck,
    Step::Fuzz,
    Step::Msrv,
    Step::Portable,
];

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Cmd::DevUp => dev_up(),
        Cmd::DevDown => dev_down(),
        Cmd::Ci { only } => run_tier("ci", FAST_TIER, &only, Tier::Fast),
        Cmd::CiFull { only } => run_tier("ci-full", FULL_TIER, &only, Tier::Full),
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

#[derive(Clone, Copy)]
enum Tier {
    Fast,
    Full,
}

fn run_tier(name: &str, steps: &[Step], only: &[Step], tier: Tier) -> Result {
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
        let result = run_step(step, tier);
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

fn nextest_profile(tier: Tier) -> &'static str {
    match (tier, in_ci()) {
        (Tier::Fast, false) => "default",
        (Tier::Fast, true) => "ci",
        (Tier::Full, false) => "full",
        (Tier::Full, true) => "ci-full",
    }
}

fn run_step(step: Step, tier: Tier) -> Result {
    match step {
        Step::Fmt => run(cargo(["fmt", "--all", "--check"])),
        Step::Build => run(cargo(["build", "--workspace", "--all-targets", "--locked"])),
        Step::Clippy => run(cargo([
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ])),
        Step::Test => nextest(Tier::Fast, &["--workspace"]),
        Step::TestAll => nextest(Tier::Full, &["--workspace"]),
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
        Step::Fuzz => fuzz_all(),
        Step::Msrv => {
            let msrv = msrv()?;
            install_toolchain(&msrv)?;
            run(cargo([
                format!("+{msrv}").as_str(),
                "build",
                "--workspace",
                "--all-targets",
                "--locked",
            ]))
        }
        Step::Portable => {
            let mut args = vec!["--no-tests=warn"];
            for krate in PORTABLE_TEST_CRATES {
                args.extend(["--package", krate]);
            }
            nextest(tier, &args)
        }
    }
}

fn nextest(tier: Tier, args: &[&str]) -> Result {
    let mut c = cargo([
        "nextest",
        "run",
        "--locked",
        "--profile",
        nextest_profile(tier),
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
    for krate in PUBLIC_CRATES {
        match last_release(krate)? {
            None => eprintln!("semver-checks: {krate} skipped (never released)"),
            Some(tag) => run(cargo([
                "semver-checks",
                "check-release",
                "--package",
                krate,
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

/// Runs every fuzz target for the full budget at once: the targets are
/// independent, so running them in parallel gives each its whole budget in
/// a fraction of the wall time.
fn fuzz_all() -> Result {
    let targets = fuzz_targets()?;
    if targets.is_empty() {
        return Err("fuzz: no fuzz targets defined".into());
    }
    let budget = Duration::from_secs(FUZZ_BUDGET_SECONDS);
    let setup = fuzz_setup()?;
    run(fuzz_command(&setup, "build", None, None))?;
    let mut children = Vec::new();
    for target in &targets {
        let mut c = fuzz_command(&setup, "run", Some(target), Some(budget));
        eprintln!("$ cargo fuzz run {target} (budget {}s)", budget.as_secs());
        let child = c
            .spawn()
            .map_err(|e| format!("cannot start fuzzer `{target}`: {e}"))?;
        children.push((target, child));
    }
    let mut failed = Vec::new();
    for (target, mut child) in children {
        let status = child.wait()?;
        eprintln!("fuzz: {target} finished: {status}");
        if !status.success() {
            failed.push(target.as_str());
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
    c.args(["diff", "--merge-base", "main"]);
    let diff = output(c).map_err(|e| format!("cannot diff against main: {e}"))?;
    let dir = root().join("target/xtask");
    std::fs::create_dir_all(&dir)?;
    let diff_file = dir.join("mutants.diff");
    std::fs::write(&diff_file, diff)?;
    let mut c = cargo(["mutants", "--test-tool", "nextest", "--in-diff"]);
    c.arg(&diff_file).args(args);
    run(c)
}

/// Installs a toolchain other than the pinned development one; a no-op when present.
fn install_toolchain(channel: &str) -> Result {
    let mut c = cmd("rustup");
    c.args(["toolchain", "install", "--profile", "minimal", channel]);
    run(c)
}
