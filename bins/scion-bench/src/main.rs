//! `scion-bench`: generates the bench repo's tip into a metadata database and
//! object store.
//!
//! ```text
//! scion-bench gen --pg-url URL [--pg-ca FILE] --s3-endpoint URL --s3-bucket NAME
//!                 [--s3-prefix PREFIX] [--files N]
//! ```
//!
//! Postgres or CockroachDB is reached at `--pg-url`, over TLS trusting the
//! PEM certificates in `--pg-ca`; without `--pg-ca` only a loopback address
//! is accepted, in plaintext. S3 credentials come from the standard `AWS_*`
//! environment variables. The tip changeset becomes the ref `main`, which
//! must not exist yet, and is printed.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use object_store::aws::AmazonS3Builder;
use object_store::prefix::PrefixStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use store::{CasOutcome, ObjectBlobStore, PgMetaStore, PgTls, PoolLimits};

#[derive(Parser)]
#[command(about = "Generates the Scion bench repo")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generates the bench repo's tip and points `main` at it.
    Gen(Gen),
}

#[derive(clap::Args)]
struct Gen {
    /// The metadata database, as a Postgres connection URL.
    #[arg(long)]
    pg_url: String,
    /// PEM certificates of the authorities the database's TLS certificate
    /// chains to. Without it, only a loopback database is accepted.
    #[arg(long)]
    pg_ca: Option<PathBuf>,
    /// The S3-compatible endpoint.
    #[arg(long)]
    s3_endpoint: String,
    /// The bucket.
    #[arg(long)]
    s3_bucket: String,
    /// A key prefix within the bucket.
    #[arg(long, default_value = "")]
    s3_prefix: String,
    /// Files in the tip, instead of the bench repo's 3,000,000.
    #[arg(long)]
    files: Option<u64>,
    /// Connections to the metadata database.
    #[arg(long, default_value_t = 32)]
    pg_connections: usize,
}

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn tls(ca: Option<&PathBuf>) -> Result<PgTls> {
    let Some(path) = ca else {
        return Ok(PgTls::LoopbackPlaintext);
    };
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path)? {
        roots.add(cert?)?;
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(PgTls::Rustls(Arc::new(config)))
}

async fn generate(args: Gen) -> Result {
    let mut params = scion_bench::BENCH_TIP;
    if let Some(files) = args.files {
        params.files = files;
    }
    let meta = Arc::new(PgMetaStore::new(
        args.pg_url.parse()?,
        tls(args.pg_ca.as_ref())?,
        PoolLimits::new(args.pg_connections),
    )?);
    meta.migrate().await?;
    let s3 = AmazonS3Builder::from_env()
        .with_endpoint(&args.s3_endpoint)
        .with_bucket_name(&args.s3_bucket)
        .build()?;
    let blobs = Arc::new(ObjectBlobStore::new(Arc::new(PrefixStore::new(
        s3,
        args.s3_prefix.as_str(),
    ))));
    let tip = scion_bench::generate(&params, meta.clone(), blobs).await;
    if let CasOutcome::Mismatch { .. } = scion_bench::create_main(&*meta, tip).await? {
        return Err("the repo already has a `main` ref".into());
    }
    println!("{tip}");
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("scion-bench: another rustls crypto provider is already installed");
        return std::process::ExitCode::FAILURE;
    }
    let result = match Cli::parse().command {
        Command::Gen(args) => generate(args).await,
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("scion-bench: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
