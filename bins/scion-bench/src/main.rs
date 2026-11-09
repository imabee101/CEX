//! Generates the bench repo and runs the benchmark harness.
//!
//! `scion-bench gen [files]` builds (or finds cached) the bench repo's tip
//! in the dev environment and prints where it is.

#[tokio::main]
async fn main() {
    let mut params = scion_bench::BENCH;
    if let Some(files) = std::env::args().nth(2) {
        params.files = files.parse().expect("files must be a number");
    }
    match std::env::args().nth(1).as_deref() {
        Some("gen") => {
            let repo = scion_bench::cached_repo(&params).await;
            println!(
                "bench repo: database {} key prefix {} tip {}",
                repo.template, repo.prefix, repo.tip
            );
        }
        _ => {
            eprintln!("usage: scion-bench gen [files]");
            std::process::exit(1);
        }
    }
}
