use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::LayerConfig;
use overlaybd::index_file::{merge_files_ro, CommitArgs};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
use tracing::debug;
use uuid::Uuid;

use crate::cfg::{OverlaybdCompressionAlgorithm, SnapshotPublishCompressionConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OverlaybdCompactOutput {
    Raw,
    ZFile {
        algorithm: OverlaybdCompressionAlgorithm,
        workers: usize,
    },
}

impl OverlaybdCompactOutput {
    /// Maximum persistent compression threads. Prevents absurd config values
    /// from spawning thousands of OS threads during layer upload.
    const MAX_COMPRESSION_WORKERS: usize = 64;

    /// Resolve the `[snapshot.publish_compression]` switch into the layer
    /// output mode applied when recontainerizing local raw layers for upload.
    /// Capture paths never consult this: local layers always stay raw.
    pub(crate) fn from_publish_compression_config(
        config: &SnapshotPublishCompressionConfig,
    ) -> Self {
        if config.enabled {
            Self::ZFile {
                algorithm: config.algorithm,
                workers: config.workers.clamp(1, Self::MAX_COMPRESSION_WORKERS),
            }
        } else {
            Self::Raw
        }
    }
}

/// Fraction of the compacted view reserved for the commit's index and metadata
/// on top of its data blocks (one eighth).
const COMPACTION_HEADROOM_OVERHEAD_DIVISOR: u64 = 8;

/// Upper bound of the transient disk a compaction needs.
///
/// The merged commit cannot exceed the device's virtual size and cannot exceed
/// the sum of its inputs' allocated bytes, because blocks rewritten by several
/// layers are stored once; the smaller bound wins. The index and metadata add a
/// fraction of the data, covered by the overhead divisor. Shared by the pause's
/// pre-rewrite refusal and by the deferred worker, so both judge the same
/// number: the pause leaves the work behind only when the node can finish it,
/// and the worker re-checks the same bound before it starts.
pub(crate) fn compaction_transient_bytes(view_bytes: u64, compactable_bytes: u64) -> u64 {
    let merged_bytes = view_bytes.min(compactable_bytes);
    merged_bytes.saturating_add(merged_bytes / COMPACTION_HEADROOM_OVERHEAD_DIVISOR)
}

/// Allocated bytes of the layers a compaction would rewrite, deduplicated by
/// device and inode so a hardlinked layer counts once.
///
/// A remote lower has no local file and contributes nothing; an unreadable path
/// is skipped, which can only under-report the transient bytes and is never a
/// reason to start a rewrite that would not fit.
pub(crate) fn compactable_allocated_bytes(layers: &[LayerConfig]) -> u64 {
    let mut seen = HashSet::new();
    let mut total = 0u64;
    for layer in layers {
        if layer.file.is_empty() {
            continue;
        }
        if let Ok(metadata) = std::fs::metadata(&layer.file) {
            if seen.insert((metadata.dev(), metadata.ino())) {
                total = total.saturating_add(metadata.blocks().saturating_mul(512));
            }
        }
    }
    total
}

/// Free bytes of the filesystem holding `path`, or `None` when the probe fails.
///
/// The deferred worker refuses to start a rewrite it cannot prove fits, exactly
/// like the pause's inline refusal: an unreadable probe is a fail-closed answer.
pub(crate) fn filesystem_free_bytes(path: &Path) -> Option<u64> {
    let stats = nix::sys::statvfs::statvfs(path).ok()?;
    let block_size = stats.fragment_size();
    Some(stats.blocks_free().saturating_mul(block_size))
}

pub(crate) async fn create_commit_args(
    output: Arc<dyn VirtualFile>,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<CommitArgs> {
    let mut args = match mode {
        OverlaybdCompactOutput::Raw => CommitArgs::new(output),
        OverlaybdCompactOutput::ZFile { algorithm, workers } => {
            let algorithm = match algorithm {
                OverlaybdCompressionAlgorithm::Lz4 => CompressOptions::LZ4,
                OverlaybdCompressionAlgorithm::Zstd => CompressOptions::ZSTD,
            };
            let mut compress_args = CompressArgs::new(CompressOptions::new(
                algorithm,
                CompressOptions::DEFAULT_BLOCK_SIZE,
                0,
            ));
            compress_args.workers = workers.max(1);
            CommitArgs::from_writer(Arc::new(
                ZFileCompactWriter::new(output, &compress_args).await?,
            ))
        }
    };
    args.concurrency = concurrency;
    Ok(args)
}

/// Compact multiple overlaybd layers into one sealed commit at `output_path`.
///
/// Each input layer is opened through the tar adaptor + switch file chain so
/// raw and ZFile-compressed layers can be mixed freely. The merged commit is
/// written directly in the requested `mode` to a sibling temp file and
/// atomically renamed into place; on failure the temp file is removed and any
/// pre-existing output is left untouched.
pub(crate) async fn compact_layers(
    layers: &[LayerConfig],
    output_path: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<Option<PathBuf>> {
    if layers.is_empty() {
        return Ok(None);
    }

    let mut src_files: Vec<Arc<dyn VirtualFile>> = Vec::with_capacity(layers.len());
    for layer in layers {
        let path = Path::new(&layer.file);
        let local: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path)
                .with_context(|| format!("open layer for compaction: {}", path.display()))?,
        );
        let tar_adapted = overlaybd::backend::tar::new_tar_file_adaptor(local)
            .await
            .with_context(|| format!("adapt layer as tar file: {}", path.display()))?;
        let display = path.display().to_string();
        let switched =
            overlaybd::backend::switch::new_switch_file(tar_adapted, true, Some(&display))
                .await
                .with_context(|| format!("open layer via switch file: {}", path.display()))?;
        src_files.push(switched);
    }

    let lower_tmp = output_path.with_extension(format!("commit.{}.tmp", Uuid::now_v7()));
    let build_result: Result<()> = async {
        let output_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&lower_tmp).context("create compacted layer output file")?);
        let commit_args = create_commit_args(output_file, mode, 32).await?;
        merge_files_ro(&src_files, commit_args)
            .await
            .context("merge layers")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "publish compacted layer to {} failed",
                    output_path.display()
                )
            })?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result?;

    debug!(
        output = %output_path.display(),
        input_layers = layers.len(),
        "compacted overlaybd layers"
    );
    Ok(Some(output_path.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::switch::new_switch_file;
    use overlaybd::backend::tar::new_tar_file_adaptor;
    use overlaybd::index_file::{LSMTFile, LSMTReadOnlyFile};
    use overlaybd::zfile::is_zfile;

    async fn create_sealed_layer(
        dir: &Path,
        name: &str,
        vsize: u64,
        writes: &[(u64, u8)],
        zfile_algo: Option<u8>,
    ) -> PathBuf {
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data"))).expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index"))).expect("create layer index file"),
        );
        let layer = LSMTFile::create(data, Some(index), vsize, false)
            .await
            .expect("create layer");
        for (offset, byte) in writes {
            layer
                .write_at(*offset, &[*byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&commit_path).expect("create layer commit output"));
        match zfile_algo {
            Some(algo) => {
                let compress_args = CompressArgs::new(CompressOptions::new(
                    algo,
                    CompressOptions::DEFAULT_BLOCK_SIZE,
                    0,
                ));
                let writer = Arc::new(
                    ZFileCompactWriter::new(output, &compress_args)
                        .await
                        .expect("create zfile compact writer"),
                );
                layer
                    .commit_with_args(CommitArgs::from_writer(writer))
                    .await
                    .expect("commit zfile layer");
            }
            None => {
                layer
                    .commit_with_args(CommitArgs::new(output))
                    .await
                    .expect("commit raw layer");
            }
        }
        commit_path
    }

    async fn read_sealed_layer(path: &Path, len: usize) -> (i32, Vec<u8>) {
        let local: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_ro(path).expect("open sealed layer"));
        let zfile_flag = is_zfile(local.clone()).await.expect("probe zfile header");
        let display = path.display().to_string();
        let tar_adapted = new_tar_file_adaptor(local).await.expect("tar adaptor");
        let switched = new_switch_file(tar_adapted, true, Some(&display))
            .await
            .expect("switch file");
        let layer = LSMTReadOnlyFile::open(switched)
            .await
            .expect("open sealed layer as LSMT");
        let data = layer.read_at(0, len).await.expect("read sealed layer");
        (zfile_flag, data.to_vec())
    }

    /// Whether `dir` holds a compaction temp file (`<output>.commit.<uuid>.tmp`).
    fn has_temp_file(dir: &Path) -> bool {
        std::fs::read_dir(dir)
            .expect("list compaction dir")
            .any(|entry| {
                let name = entry.expect("read compaction dir entry").file_name();
                name.to_string_lossy().ends_with(".tmp")
            })
    }

    fn layer_config(path: &Path) -> LayerConfig {
        LayerConfig {
            file: path.display().to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn compact_layers_mixed_input_and_configured_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vsize = 3 * 4096u64;
        let bottom = create_sealed_layer(
            temp.path(),
            "bottom",
            vsize,
            &[(0, 0x11), (4096, 0x11), (8192, 0x11)],
            None,
        )
        .await;
        let middle = create_sealed_layer(
            temp.path(),
            "middle",
            vsize,
            &[(4096, 0x22)],
            Some(CompressOptions::LZ4),
        )
        .await;
        let top = create_sealed_layer(
            temp.path(),
            "top",
            vsize,
            &[(8192, 0x33)],
            Some(CompressOptions::ZSTD),
        )
        .await;
        let layers = [&bottom, &middle, &top]
            .into_iter()
            .map(|path| layer_config(path))
            .collect::<Vec<_>>();

        // Later layers take precedence over earlier ones on overlapping pages.
        let mut expected = vec![0u8; vsize as usize];
        expected[..4096].fill(0x11);
        expected[4096..8192].fill(0x22);
        expected[8192..].fill(0x33);

        for (name, mode, expected_zfile) in [
            ("raw", OverlaybdCompactOutput::Raw, 0),
            (
                "lz4",
                OverlaybdCompactOutput::ZFile {
                    algorithm: OverlaybdCompressionAlgorithm::Lz4,
                    // Exercise the segmented parallel compression path.
                    workers: 2,
                },
                1,
            ),
            (
                "zstd",
                OverlaybdCompactOutput::ZFile {
                    algorithm: OverlaybdCompressionAlgorithm::Zstd,
                    workers: 1,
                },
                1,
            ),
        ] {
            let output_path = temp.path().join(format!("compacted-{name}.commit"));
            let published = compact_layers(&layers, &output_path, mode)
                .await
                .expect("compact mixed raw/lz4/zstd layers");
            assert_eq!(published.as_deref(), Some(output_path.as_path()));
            assert!(!has_temp_file(temp.path()));

            let (zfile_flag, data) = read_sealed_layer(&output_path, vsize as usize).await;
            assert_eq!(zfile_flag, expected_zfile, "mode {name}");
            assert_eq!(data, expected, "mode {name}");
        }

        // A failed compaction must neither clobber a previously published
        // output nor leave the sibling temp file behind.
        let output_path = temp.path().join("compacted-raw.commit");
        let before = std::fs::read(&output_path).expect("read published output");
        let bogus = temp.path().join("bogus.commit");
        std::fs::write(&bogus, vec![0xAB; 4096]).expect("write bogus layer");
        let mut broken = layers.clone();
        broken.push(layer_config(&bogus));
        compact_layers(&broken, &output_path, OverlaybdCompactOutput::Raw)
            .await
            .expect_err("invalid input layer must fail");
        assert_eq!(
            std::fs::read(&output_path).expect("read published output"),
            before
        );
        assert!(!has_temp_file(temp.path()));
    }

    /// The 13/11/2026 disk scenario: 8 GiB rewritten before a pause on a 63 GiB
    /// guest disk needs 9 GiB of transient space, which the provisional 96 GiB
    /// pending budget admits. The number is the pause's admission input, not the
    /// chain's bytes, so mixing the two units would send the scenario inline.
    #[test]
    fn compaction_transient_bytes_bounds_the_13_11_disk_scenario() {
        let gib = 1u64 << 30;
        let required = compaction_transient_bytes(63 * gib, 8 * gib);
        assert_eq!(required, 8 * gib + gib);
        assert!(
            required <= 96 * gib,
            "the provisional pending budget must admit the measured pause"
        );
    }

    #[test]
    fn compact_output_from_publish_compression_config() {
        let disabled = SnapshotPublishCompressionConfig {
            enabled: false,
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers: 1,
        };
        assert_eq!(
            OverlaybdCompactOutput::from_publish_compression_config(&disabled),
            OverlaybdCompactOutput::Raw
        );

        let zstd = SnapshotPublishCompressionConfig {
            enabled: true,
            algorithm: OverlaybdCompressionAlgorithm::Zstd,
            // Zero workers clamps to sequential.
            workers: 0,
        };
        assert_eq!(
            OverlaybdCompactOutput::from_publish_compression_config(&zstd),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Zstd,
                workers: 1,
            }
        );

        let clamped = SnapshotPublishCompressionConfig {
            enabled: true,
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers: usize::MAX,
        };
        assert_eq!(
            OverlaybdCompactOutput::from_publish_compression_config(&clamped),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Lz4,
                workers: OverlaybdCompactOutput::MAX_COMPRESSION_WORKERS,
            }
        );
    }
}
