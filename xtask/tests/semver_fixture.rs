//! M1: `cargo semver-checks` fails on a fixture release of scion-core that
//! removes a public item, and passes on an unchanged copy (the control that
//! shows the failure comes from the removal, not from the fixture setup).
//!
//! Needs `cargo-semver-checks` installed; slow, because it builds rustdoc
//! JSON for two copies of the crate.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The public item the fixture release removes, and its declaration.
const REMOVED_ITEM: &str = "MAX_TREE_ENTRIES";
const DECLARATION: &str = "pub const MAX_TREE_ENTRIES: usize = 4096;";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), &dest).unwrap();
        }
    }
}

/// A one-crate workspace holding a copy of scion-core with the workspace's
/// package settings, dependency versions, lints, and lockfile.
fn fixture_workspace(dir: &Path) {
    let root = root();
    if dir.exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    copy_dir(&root.join("crates/scion-core"), &dir.join("scion-core"));
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let start = manifest
        .find("members = [")
        .expect("workspace members list");
    let end = start + manifest[start..].find(']').unwrap() + 1;
    let mut fixture = format!(
        "{}members = [\"scion-core\"]{}",
        &manifest[..start],
        &manifest[end..]
    );
    // Drop workspace path dependencies; the fixture has no other members.
    fixture = fixture
        .lines()
        .filter(|l| !l.contains("path = \""))
        .collect::<Vec<_>>()
        .join("\n");
    fixture.push('\n');
    std::fs::write(dir.join("Cargo.toml"), fixture).unwrap();
    std::fs::copy(root.join("Cargo.lock"), dir.join("Cargo.lock")).unwrap();
}

fn semver_checks(current: &Path, baseline: &Path) -> (bool, String) {
    let out = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
        .args([
            "semver-checks",
            "check-release",
            "--package",
            "scion-core",
            "--manifest-path",
        ])
        .arg(current.join("Cargo.toml"))
        .arg("--baseline-root")
        .arg(baseline)
        .current_dir(current)
        .output()
        .expect("cargo-semver-checks must be installed");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

#[test]
fn slow_semver_checks_rejects_a_release_removing_a_public_scion_core_item() {
    let base = root().join("target/xtask/semver-fixture");
    let baseline = base.join("baseline");
    let current = base.join("current");
    fixture_workspace(&baseline);
    fixture_workspace(&current);

    let (ok, text) = semver_checks(&current, &baseline);
    assert!(ok, "an unchanged release must pass:\n{text}");

    let lib = current.join("scion-core/src/lib.rs");
    let source = std::fs::read_to_string(&lib).unwrap();
    assert_eq!(
        source.matches(DECLARATION).count(),
        1,
        "scion-core declares `{DECLARATION}` once"
    );
    std::fs::write(
        &lib,
        source.replace(DECLARATION, &DECLARATION.replacen("pub ", "pub(crate) ", 1)),
    )
    .unwrap();

    let (ok, text) = semver_checks(&current, &baseline);
    assert!(
        !ok,
        "a release removing `{REMOVED_ITEM}` must fail:\n{text}"
    );
    assert!(
        text.contains(REMOVED_ITEM),
        "the failure must name `{REMOVED_ITEM}`:\n{text}"
    );
}
