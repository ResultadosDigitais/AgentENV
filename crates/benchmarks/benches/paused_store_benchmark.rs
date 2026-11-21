//! Paused-store spike harness (OPSAI-6192): node-local SSD versus the bucket.
//!
//! The plan asks for pause and resume p50/p90, bytes per pause, and the monthly
//! storage plus operation cost, for both node shapes, before any adoption
//! decision. This harness produces the raw numbers; the cost arithmetic lives in
//! `agentenv::orchestrator::persistence` as a unit-tested function, so a price
//! change is one constant and the monthly figure is reproducible by hand.
//!
//! It runs the *same* pause and resume cycle for both backends on the same image
//! and memory size, so the only difference between the two rows is where the
//! artifacts went. A resume that cannot restore the guest is reported as a
//! failure rather than as a number: timings from a harness that cannot resume
//! are worthless.
//!
//! This needs real KVM, ublk and a configured object-store repository, so it is
//! a hardware-gated benchmark and says so instead of silently skipping:
//!
//! ```text
//! # node-local SSD baseline
//! AENV_PAUSED_STORE_BENCH_BACKEND=local cargo bench -p agentenv-benchmarks \
//!     --bench paused_store -- 32g
//!
//! # bucket
//! AENV_PAUSED_STORE_BENCH_BACKEND=bucket cargo bench -p agentenv-benchmarks \
//!     --bench paused_store -- 32g
//! ```
//!
//! The node shape (`runner-generico`, `32g`) is a label, recorded next to the
//! numbers so a result is never silently generalized across shapes.

use agentenv::cfg::ConfigManager;
use agentenv::orchestrator::PausedStoreCost;
use agentenv::sandbox::{
    FirecrackerSandbox, FirecrackerSandboxConfig, OverlaybdConfig, SandboxExecutor,
    UblkDeviceManager,
};
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const BACKEND_ENV: &str = "AENV_PAUSED_STORE_BENCH_BACKEND";
const SHAPE_ENV: &str = "AENV_PAUSED_STORE_BENCH_SHAPE";
const SAMPLES_ENV: &str = "AENV_PAUSED_STORE_BENCH_SAMPLES";
const DEFAULT_SHAPE: &str = "32g";
const DEFAULT_SAMPLES: usize = 5;
const READY_MARKER: &str = "paused-store-bench-ok";

/// Where a pause puts its artifacts. This is the dimension the spike varies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PausedStoreBackend {
    /// Artifacts stay on the node's SSD, the behavior the card wants to replace.
    Local,
    /// Artifacts are uploaded to the configured object-store repository, wiped
    /// from the node, and fetched back on resume.
    Bucket,
}

impl PausedStoreBackend {
    fn from_env() -> Result<Self> {
        match std::env::var(BACKEND_ENV).as_deref() {
            Ok("local") => Ok(Self::Local),
            Ok("bucket") => Ok(Self::Bucket),
            Ok(other) => bail!("{BACKEND_ENV} must be 'local' or 'bucket', got {other:?}"),
            Err(_) => {
                bail!("set {BACKEND_ENV}=local or {BACKEND_ENV}=bucket; the spike compares the two")
            }
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Bucket => "bucket",
        }
    }
}

/// The measurements one sample contributes.
#[derive(Clone, Copy, Debug)]
struct PauseResumeSample {
    pause: Duration,
    resume: Duration,
    /// Bytes the pause wrote, summed over every artifact object.
    artifact_bytes: u64,
    /// Object count, which drives the per-operation cost.
    artifact_objects: u64,
}

fn samples_from_env() -> Result<usize> {
    match std::env::var(SAMPLES_ENV) {
        Ok(value) => {
            let samples = value.parse::<usize>().with_context(|| {
                format!("{SAMPLES_ENV} must be a positive integer, got {value:?}")
            })?;
            anyhow::ensure!(samples > 0, "{SAMPLES_ENV} must be greater than zero");
            Ok(samples)
        }
        Err(_) => Ok(DEFAULT_SAMPLES),
    }
}

fn shape_from_env() -> String {
    std::env::var(SHAPE_ENV).unwrap_or_else(|_| DEFAULT_SHAPE.to_owned())
}

const POPULATION_ENV: &str = "AENV_PAUSED_STORE_BENCH_POPULATION";
const PAUSES_PER_MONTH_ENV: &str = "AENV_PAUSED_STORE_BENCH_PAUSES_PER_MONTH";
const RESIDENCY_HOURS_ENV: &str = "AENV_PAUSED_STORE_BENCH_RESIDENCY_HOURS";

/// The paused population to price. Defaults to the volume the card states.
fn population_from_env() -> u64 {
    std::env::var(POPULATION_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(43)
}

fn pauses_per_month_from_env() -> u64 {
    std::env::var(PAUSES_PER_MONTH_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(43 * 2)
}

fn residency_hours_from_env() -> f64 {
    std::env::var(RESIDENCY_HOURS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(48.0)
}

/// Nearest-rank percentile, so p50/p90 are exact order statistics of the samples
/// rather than an interpolation no one can reproduce from the printed list.
fn percentile(samples: &[Duration], percent: usize) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (percent * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Every regular file under `root`, which is the artifact set of one pause.
fn artifact_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("read {}", directory.display()));
            }
        };
        for entry in entries {
            let entry = entry.with_context(|| format!("walk {}", directory.display()))?;
            let file_type = entry
                .file_type()
                .with_context(|| format!("stat {}", entry.path().display()))?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            }
        }
    }
    Ok(files)
}

/// Bytes and object count of one pause's artifacts.
///
/// This is the instrumentation the cost math needs, taken from the files
/// themselves so the month figure never depends on a second estimate.
fn artifact_size(root: &Path) -> Result<(u64, u64)> {
    let files = artifact_files(root)?;
    let mut bytes = 0u64;
    for file in &files {
        bytes = bytes.saturating_add(
            std::fs::metadata(file)
                .with_context(|| format!("stat {}", file.display()))?
                .len(),
        );
    }
    Ok((bytes, files.len() as u64))
}

async fn setup_sandbox(mem_size_mib: u32) -> Result<FirecrackerSandbox> {
    let app_config = ConfigManager::init_global()?.config();
    let image_resolver = agentenv::image::ImageResolver::new(app_config);
    let image_config_path = image_resolver
        .resolve(image_resolver.default_image())
        .await
        .map(|resolved| resolved.overlaybd_config_path)?;
    UblkDeviceManager::init_global_from_config(app_config)
        .await
        .context("init global UblkDeviceManager for the paused-store benchmark")?;

    let mut config =
        FirecrackerSandboxConfig::from_global_config_with_user_image(OverlaybdConfig {
            image_config_path,
            read_only: false,
            runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
        })
        .context("load sandbox config for the paused-store benchmark")?;
    config.mem_size_mib = mem_size_mib;
    config.vcpu_count = 1;

    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await.context("start benchmark sandbox")?;
    sandbox
        .run_command(
            "sh",
            &[
                "-c",
                &format!("echo {READY_MARKER} > /tmp/paused-store-marker"),
            ],
        )
        .await
        .context("write the resume marker")?;
    Ok(sandbox)
}

/// One pause/resume cycle through `backend`, verifying the guest came back.
async fn run_sample(backend: PausedStoreBackend, mem_size_mib: u32) -> Result<PauseResumeSample> {
    let mut sandbox = setup_sandbox(mem_size_mib).await?;
    // Held, not leaked: dropping the tempdir at the end of the sample is what
    // takes the node-local copy away, so the footprint measured afterwards is real.
    let artifact_dir = tempfile::tempdir()?;
    let artifact_root = artifact_dir.path();

    let pause_start = Instant::now();
    let (snapshot, _manifest) = sandbox.pause_to_dir(artifact_root).await?;
    let pause = pause_start.elapsed();
    sandbox.stop().await?;

    let (artifact_bytes, artifact_objects) = artifact_size(artifact_root)?;
    anyhow::ensure!(
        artifact_bytes > 0,
        "a pause wrote no artifacts; nothing to store"
    );

    let resume_start = Instant::now();
    let resume_snapshot = match backend {
        PausedStoreBackend::Local => snapshot.clone(),
        PausedStoreBackend::Bucket => {
            bail!(
                "the bucket backend needs the repository-backed paused store (plan step p-6); \
                 run the local baseline now and record the bucket row once that lands"
            );
        }
    };
    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&resume_snapshot).await?;
    let output = resumed
        .run_command("cat", &["/tmp/paused-store-marker"])
        .await?;
    anyhow::ensure!(
        output.stdout.contains(READY_MARKER),
        "the resumed guest lost its state; timings from this run must not be recorded"
    );
    let resume = resume_start.elapsed();
    resumed.stop().await?;

    Ok(PauseResumeSample {
        pause,
        resume,
        artifact_bytes,
        artifact_objects,
    })
}

/// Prints the row the card records, one line per statistic, so a run's output can
/// be pasted into the decision without re-deriving anything.
fn print_row(shape: &str, backend: PausedStoreBackend, samples: &[PauseResumeSample]) {
    let pauses: Vec<Duration> = samples.iter().map(|sample| sample.pause).collect();
    let resumes: Vec<Duration> = samples.iter().map(|sample| sample.resume).collect();
    let bytes = samples[0].artifact_bytes;
    let objects = samples[0].artifact_objects;

    println!(
        "shape={shape} backend={} samples={}",
        backend.as_str(),
        samples.len()
    );
    println!(
        "  pause  p50={:.3}s p90={:.3}s",
        percentile(&pauses, 50).as_secs_f64(),
        percentile(&pauses, 90).as_secs_f64(),
    );
    println!(
        "  resume p50={:.3}s p90={:.3}s",
        percentile(&resumes, 50).as_secs_f64(),
        percentile(&resumes, 90).as_secs_f64(),
    );
    println!("  bytes_per_pause={bytes} objects_per_pause={objects}");
}

#[tokio::main]
async fn main() -> Result<()> {
    let backend = PausedStoreBackend::from_env()?;
    let shape = shape_from_env();
    let samples_wanted = samples_from_env()?;
    let mem_size_mib = mem_size_for_shape(&shape)?;

    let app_config = ConfigManager::init_global()?.config();
    if backend == PausedStoreBackend::Bucket
        && app_config.snapshot.repository_backend
            == agentenv::cfg::SnapshotRepositoryBackendKind::PosixFs
    {
        bail!(
            "the bucket row needs an object-store repository_backend; a POSIX repository on this \
             node would measure the SSD the spike is meant to replace"
        );
    }

    println!(
        "paused-store spike: backend={} shape={shape} mem_size_mib={mem_size_mib}",
        backend.as_str()
    );

    let mut samples = Vec::with_capacity(samples_wanted);
    for index in 0..samples_wanted {
        let sample = run_sample(backend, mem_size_mib)
            .await
            .with_context(|| format!("sample {}/{samples_wanted}", index + 1))?;
        println!(
            "  sample {} pause={:.3}s resume={:.3}s bytes={} objects={}",
            index + 1,
            sample.pause.as_secs_f64(),
            sample.resume.as_secs_f64(),
            sample.artifact_bytes,
            sample.artifact_objects,
        );
        samples.push(sample);
    }

    print_row(&shape, backend, &samples);

    let cost = PausedStoreCost {
        bytes_per_pause: samples[0].artifact_bytes,
        objects_per_pause: samples[0].artifact_objects,
        residency_hours: residency_hours_from_env(),
        pauses_per_month: pauses_per_month_from_env(),
    };
    let population = population_from_env();
    let monthly = cost.monthly(population);
    println!(
        "  monthly population={population} pauses_per_month={} residency_hours={} \
         resident_gib={:.2}",
        cost.pauses_per_month,
        cost.residency_hours,
        cost.resident_gib(population),
    );
    println!(
        "  monthly_usd storage={:.4} class_a={:.4} class_b={:.4} total={:.4}",
        monthly.storage_usd,
        monthly.class_a_usd,
        monthly.class_b_usd,
        monthly.total_usd(),
    );
    Ok(())
}

/// Memory for the named node shape. The shapes are the ones the card measures; an
/// unknown one fails loudly rather than silently borrowing another shape's number.
fn mem_size_for_shape(shape: &str) -> Result<u32> {
    match shape {
        "32g" => Ok(32 * 1024),
        "runner-generico" => Ok(8 * 1024),
        other => bail!(
            "unknown shape {other:?}; expected '32g' or 'runner-generico' so the row states the \
             shape it was measured on"
        ),
    }
}
