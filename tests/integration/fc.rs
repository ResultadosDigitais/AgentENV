use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentenv::cfg::ConfigManager;
use agentenv::sandbox::{
    BaseSandboxNetworkPolicy, FirecrackerSandbox, FirecrackerSnapshotConfig, ProcessOutput,
    SandboxBackend, SandboxExecutor, SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
};
use anyhow::{bail, Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::ImageConfig;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::is_zfile;

use crate::common;

const DISK_MARKER: &str = "agentenv-disk-test-ok";
const DISK_MARKER_PATH: &str = "/tmp/agentenv-test-marker";

fn lower_file_paths_from_image_config(image_config_path: &Path) -> Result<Vec<PathBuf>> {
    let image_config: ImageConfig = serde_json::from_slice(
        &fs::read(image_config_path)
            .with_context(|| format!("read image config {}", image_config_path.display()))?,
    )
    .with_context(|| format!("parse image config {}", image_config_path.display()))?;
    let image_config_dir = image_config_path
        .parent()
        .context("image config should have a parent directory")?;

    image_config
        .lowers
        .into_iter()
        .filter_map(|lower| {
            if !lower.file.is_empty() {
                Some(lower.file)
            } else if !lower.dir.is_empty() {
                Some(lower.dir)
            } else {
                None
            }
        })
        .map(|lower| {
            let lower_path = Path::new(&lower);
            let resolved = if lower_path.is_absolute() {
                lower_path.to_path_buf()
            } else {
                image_config_dir.join(lower_path)
            };
            Ok(fs::canonicalize(&resolved).unwrap_or(resolved))
        })
        .collect()
}

fn assert_no_lower_paths_under(paths: &[PathBuf], root: &Path, context: &str) {
    let canonical_root = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    assert!(
        paths.iter().all(|path| !path.starts_with(&canonical_root)),
        "{context} should not reference managed snapshot lower paths under {}: {paths:?}",
        canonical_root.display()
    );
}

/// Write a marker file to guest disk via envd and verify it was written.
async fn write_disk_marker(sandbox: &mut FirecrackerSandbox) -> Result<()> {
    let output = sandbox
        .run_command(
            "sh",
            &["-c", &format!("echo {DISK_MARKER} > {DISK_MARKER_PATH}")],
        )
        .await?;
    if output.exit_code != 0 {
        bail!("write marker failed: {}", output.stderr);
    }
    Ok(())
}

/// Read the marker file from guest disk and verify it contains the expected value.
async fn verify_disk_marker(sandbox: &mut FirecrackerSandbox) -> Result<()> {
    let output = sandbox.run_command("cat", &[DISK_MARKER_PATH]).await?;
    if output.exit_code != 0 {
        bail!(
            "read marker failed (exit {}): {}",
            output.exit_code,
            output.stderr
        );
    }
    assert!(
        output.stdout.trim().contains(DISK_MARKER),
        "marker mismatch: expected '{}', got '{}'",
        DISK_MARKER,
        output.stdout.trim()
    );
    Ok(())
}

/// Assert that the newest local memory lower of `snapshot` is a raw LSMT
/// layer (no ZFile wrapper). Capture-time compression was removed: local
/// layers always stay raw, so local resume pays no decompression cost.
/// Compression, when enabled via `[snapshot.publish_compression]`, happens
/// once at publish time on the repository upload path.
///
/// Image config lowers are ordered bottom-to-top (oldest base layer first), so
/// the newest local lower is the last entry with a `file` path.
async fn assert_memory_layer_is_raw(snapshot: &FirecrackerSnapshotConfig) -> Result<()> {
    let image_config_path = &snapshot.mem_overlaybd_config.image_config_path;
    let image_config: ImageConfig = serde_json::from_slice(
        &fs::read(image_config_path)
            .with_context(|| format!("read image config {}", image_config_path.display()))?,
    )
    .with_context(|| format!("parse image config {}", image_config_path.display()))?;
    let image_config_dir = image_config_path
        .parent()
        .context("image config should have a parent directory")?;
    let newest_lower = image_config
        .lowers
        .iter()
        .rev()
        .find(|lower| !lower.file.is_empty())
        .context("memory snapshot should reference at least one local lower")?;
    let lower_path = Path::new(&newest_lower.file);
    let lower_path = if lower_path.is_absolute() {
        lower_path.to_path_buf()
    } else {
        image_config_dir.join(lower_path)
    };

    let file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_ro(&lower_path)
            .with_context(|| format!("open memory lower {}", lower_path.display()))?,
    );
    let zfile_flag = is_zfile(file)
        .await
        .with_context(|| format!("probe zfile header of {}", lower_path.display()))?;
    assert_eq!(
        zfile_flag,
        0,
        "memory lower {} should be a raw LSMT layer",
        lower_path.display()
    );
    Ok(())
}

#[tokio::test]
async fn microvm_lifecycle_and_snapshot_preserve_disk_state() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let snapshot = sandbox.pause().await?;
    assert!(snapshot.vm_state_path.exists());
    assert!(snapshot.mem_overlaybd_config.image_config_path.exists());

    sandbox.resume().await?;
    verify_disk_marker(&mut sandbox).await?;

    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    verify_disk_marker(&mut resumed).await?;
    resumed.stop().await?;
    Ok(())
}

async fn serve_tools_rootfs(
    rootfs: &Path,
) -> Result<(String, tokio::task::JoinHandle<std::io::Result<()>>)> {
    use axum::{extract::Path as RoutePath, routing::get, Router};
    use bytes::Bytes;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    let mut archive = tar::Builder::new(Vec::new());
    archive.follow_symlinks(false);
    archive.append_dir_all(".", rootfs)?;
    let layer = archive.into_inner()?;
    let digest = |bytes: &[u8]| format!("sha256:{:x}", Sha256::digest(bytes));
    let layer_digest = digest(&layer);
    let config = serde_json::to_vec(&json!({
        "architecture": if std::env::consts::ARCH == "x86_64" { "amd64" } else { "arm64" },
        "os": "linux",
        "config": {"Labels": {"io.agentenv.tools-drive.format": "oci-rootfs-v1"}},
        "rootfs": {"type": "layers", "diff_ids": [layer_digest]}
    }))?;
    let config_digest = digest(&config);
    let manifest_type = "application/vnd.oci.image.manifest.v1+json";
    let manifest = Bytes::from(serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": manifest_type,
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len()},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar", "digest": layer_digest, "size": layer.len()}]
    }))?);
    let manifest_digest = digest(&manifest);
    let blobs = Arc::new(std::collections::HashMap::from([
        (layer_digest, Bytes::from(layer)),
        (config_digest, Bytes::from(config)),
    ]));
    let app = Router::new()
        .route("/v2/", get(|| async { "{}" }))
        .route(
            "/v2/tools/manifests/{reference}",
            get(move || {
                let body = manifest.clone();
                let digest = manifest_digest.clone();
                async move {
                    (
                        [
                            ("content-type", manifest_type.to_string()),
                            ("docker-content-digest", digest),
                        ],
                        body,
                    )
                }
            }),
        )
        .route(
            "/v2/tools/blobs/{digest}",
            get(move |RoutePath(digest): RoutePath<String>| {
                let blobs = Arc::clone(&blobs);
                async move {
                    blobs
                        .get(&digest)
                        .cloned()
                        .ok_or(axum::http::StatusCode::NOT_FOUND)
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let registry = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((registry, server))
}

#[tokio::test]
async fn oci_tools_share_a_readonly_device_across_concurrent_launches() -> Result<()> {
    use std::os::unix::fs::{symlink, FileTypeExt, PermissionsExt};

    common::setup().await;
    let global = ConfigManager::global_config();
    let tools_dir = global.deps_path.join("tools");
    fs::create_dir_all(&tools_dir)?;
    let release = tempfile::Builder::new()
        .prefix("0.0.0-oci-")
        .tempdir_in(&tools_dir)?;
    let version = release
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let rootfs = release.path().join("rootfs");
    fs::create_dir(&rootfs)?;

    // Reuse the provisioned guest binaries, regardless of the default tools format.
    let mut seed = FirecrackerSandbox::new(common::default_sandbox_config()?)?;
    seed.start().await?;
    let extracted = tokio::process::Command::new("debugfs")
        .args(["-R", &format!("rdump / {}", rootfs.display())])
        .arg(seed.work_rootfs_path().with_file_name("rootfs.ext4"))
        .output()
        .await;
    seed.stop().await?;
    let extracted = extracted?;
    assert!(
        extracted.status.success(),
        "{}",
        String::from_utf8_lossy(&extracted.stderr)
    );
    fs::write(
        rootfs.join("agentenv/tools-drive-version"),
        format!("{version}\n"),
    )?;

    let mut setup = global.clone();
    setup.home_path = release.path().join("home");
    setup.image.cache.root_dir = release.path().join("image-cache");
    setup.deps_path = release.path().join("deps");
    setup.ublk.overlaybd.global_config_path = release.path().join("config/overlaybd.json");
    setup.memory_snapshot.overlaybd_global_config_path = release.path().join("config/memory.json");
    setup.firecracker.binary_path = Some(global.resolved_firecracker_binary_path());
    setup.kernel.image_path = Some(global.resolved_kernel_image_path());
    setup.tools.drive_path = None;
    setup.tools.version = Some(version.clone());
    let regctl = setup.resolved_regctl_binary();
    fs::create_dir_all(regctl.parent().unwrap())?;
    symlink(
        global.deps_path.join("overlaybd"),
        setup.deps_path.join("overlaybd"),
    )?;
    let (registry, server) = serve_tools_rootfs(&rootfs).await?;
    setup.tools.url = Some(format!("{registry}/tools:{{version}}"));
    let prepared = async {
        fs::write(
            &regctl,
            format!(
                "#!/bin/sh\nexec {} --host reg={registry},tls=disabled \"$@\"\n",
                shell_util::shell_quote(&global.resolved_regctl_binary().to_string_lossy())
            ),
        )?;
        fs::set_permissions(&regctl, fs::Permissions::from_mode(0o755))?;
        agentenv::setup::ensure_provisioning(&setup).await
    }
    .await;
    server.abort();
    prepared?;
    for entry in fs::read_dir(setup.deps_path.join("tools").join(&version))? {
        let entry = entry?;
        fs::rename(entry.path(), release.path().join(entry.file_name()))?;
    }

    let mut config = common::default_sandbox_config()?;
    config.common.tools_drive_version = version.clone();
    let mut first = FirecrackerSandbox::new(config.clone())?;
    let mut second = FirecrackerSandbox::new(config)?;
    tokio::try_join!(first.start(), second.start())?;
    let backing = |sandbox: &FirecrackerSandbox| {
        fs::canonicalize(sandbox.work_rootfs_path().with_file_name("rootfs.ext4"))
    };
    let device = backing(&first)?;
    assert!(fs::metadata(&device)?.file_type().is_block_device());
    assert_eq!(device, backing(&second)?);
    first.stop().await?;
    let output = second
        .run_command(
            "/agentenv/bin/busybox",
            &["cat", "/agentenv/tools-drive-version", "/sys/block/vda/ro"],
        )
        .await?;
    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), format!("{version}\n1"));
    let paused = second.pause().await?;
    assert_eq!(paused.common.tools_drive_version, version);
    second.resume().await?;
    assert_eq!(device, backing(&second)?);
    second.stop().await?;
    Ok(())
}

/// Shared body of the memory snapshot format test: pause a marked sandbox into
/// a temp dir, assert the direct OverlayBD snapshot artifact layout, validate
/// that the newest memory lower is a raw LSMT layer, and verify that a resume
/// round-trip preserves guest disk state.
async fn run_memory_snapshot_format_and_resume_case() -> Result<()> {
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let snapshot_dir = tempfile::tempdir()?;
    let (snapshot, _) = sandbox.pause_to_dir(snapshot_dir.path()).await?;
    sandbox.stop().await?;

    assert!(snapshot.vm_state_path.exists());
    assert!(snapshot.mem_overlaybd_config.image_config_path.exists());
    assert!(snapshot_dir
        .path()
        .join("mem_overlaybd/overlaybd.commit")
        .exists());
    assert!(snapshot_dir.path().join("mem_image.json").exists());
    assert!(
        !snapshot_dir.path().join("mem.bin").exists(),
        "direct OverlayBD snapshot should not create mem.bin"
    );
    assert_memory_layer_is_raw(&snapshot).await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    verify_disk_marker(&mut resumed).await?;
    resumed.stop().await?;
    Ok(())
}

/// Direct OverlayBD memory layers are built from Firecracker memory ranges
/// without an intermediate raw memory file, and are always written raw.
#[tokio::test]
async fn memory_snapshot_format_matches_config_and_resumes() -> Result<()> {
    common::setup().await;
    run_memory_snapshot_format_and_resume_case().await
}

#[tokio::test]
async fn backend_pause_state_round_trips_through_encoded_artifacts() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let tools_version = sandbox_config.common.tools_drive_version.clone();
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let temp = tempfile::tempdir()?;
    let artifact_root = temp.path().join("paused-artifacts");
    let paused_state = SandboxBackend::pause(&mut sandbox, Some(&artifact_root)).await?;
    sandbox.stop().await?;

    let encoded = paused_state.encode()?;
    assert_eq!(encoded["common"]["tools_drive_version"], tools_version);
    assert!(encoded.get("tools").is_none());
    assert!(!artifact_root.join("tools").exists());
    drop(paused_state);

    let decoded =
        agentenv::sandbox::FirecrackerPausedState::decode(artifact_root.clone(), encoded)?;
    let mut resumed =
        FirecrackerSandbox::resume_from_snapshot_config(decoded.snapshot_config()).await?;
    verify_disk_marker(&mut resumed).await?;
    resumed.stop().await?;
    Ok(())
}

/// Verify multi-level snapshot chains: first → second → resume from second after
/// dropping the first snapshot handle.
#[tokio::test]
async fn snapshot_chain_survives_after_parent_snapshot_handle_is_dropped() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let first_snapshot = sandbox.pause().await?;
    assert_memory_layer_is_raw(&first_snapshot).await?;
    sandbox.stop().await?;
    let first_snapshot_dir = fs::canonicalize(first_snapshot.vm_state_path.parent().unwrap())?;
    let first_persistent_generation = fs::canonicalize(
        first_snapshot
            .mem_overlaybd_config
            .image_config_path
            .parent()
            .context("first memory image should live under a persistent generation dir")?,
    )?;
    let first_rootfs_commit = fs::canonicalize(
        first_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("first snapshot rootfs image config should exist")?
            .image_config_path
            .parent()
            .context("first rootfs image should have a parent dir")?
            .join("snapshot.commit"),
    )?;
    assert!(
        first_snapshot_dir.exists(),
        "first snapshot tempdir should exist at {}",
        first_snapshot_dir.display()
    );

    // Resume from first, write additional data, pause again.
    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&first_snapshot).await?;
    verify_disk_marker(&mut resumed).await?;
    let second_snapshot = resumed.pause().await?;
    assert_memory_layer_is_raw(&second_snapshot).await?;
    resumed.stop().await?;
    let second_snapshot_dir = fs::canonicalize(second_snapshot.vm_state_path.parent().unwrap())?;
    let second_mem_lowers = lower_file_paths_from_image_config(
        &second_snapshot.mem_overlaybd_config.image_config_path,
    )?;
    assert!(
        second_mem_lowers
            .iter()
            .all(|path| !path.starts_with(&first_persistent_generation)),
        "second memory snapshot should adopt first generation layers instead of referencing {}: {second_mem_lowers:?}",
        first_persistent_generation.display(),
    );
    assert!(
        second_mem_lowers
            .iter()
            .any(|path| path.starts_with(second_snapshot_dir.join("inherited-layers"))),
        "second memory snapshot should adopt inherited layers under its own generation",
    );
    assert!(
        second_mem_lowers.iter().all(|path| path.exists()),
        "second memory snapshot should reference existing lower files",
    );

    let second_rootfs_lowers = lower_file_paths_from_image_config(
        &second_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("second snapshot rootfs image config should exist")?
            .image_config_path,
    )?;
    let second_rootfs_dir = fs::canonicalize(
        second_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("second snapshot rootfs image config should exist")?
            .image_config_path
            .parent()
            .context("second rootfs image should have a parent dir")?,
    )?;
    assert!(
        second_rootfs_lowers
            .iter()
            .all(|path| path != &first_rootfs_commit),
        "second rootfs snapshot should adopt first generation layers instead of referencing {}: {second_rootfs_lowers:?}",
        first_rootfs_commit.display(),
    );
    assert!(
        second_rootfs_lowers
            .iter()
            .any(|path| path.starts_with(second_rootfs_dir.join("inherited-layers"))),
        "second rootfs snapshot should adopt inherited layers under its own rootfs dir",
    );
    assert!(
        second_rootfs_lowers.iter().all(|path| path.exists()),
        "second rootfs snapshot should reference existing lower files",
    );

    // Drop first snapshot handle — second snapshot's layers should still be valid.
    drop(first_snapshot);

    let mut resumed_again =
        FirecrackerSandbox::resume_from_snapshot_config(&second_snapshot).await?;
    verify_disk_marker(&mut resumed_again).await?;

    // Also test pause_to_dir with snapshot-owned inherited layer adoption.
    let expected_snapshot_dir = tempfile::tempdir()?;
    let (dir_snapshot, _) = resumed_again
        .pause_to_dir(expected_snapshot_dir.path())
        .await?;
    resumed_again.stop().await?;
    let managed_snapshot_base_root = ConfigManager::global_config()
        .firecracker
        .work_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("agentenv"))
        .join("managed-snapshots");
    let dir_snapshot_mem_lowers =
        lower_file_paths_from_image_config(&dir_snapshot.mem_overlaybd_config.image_config_path)?;
    assert_no_lower_paths_under(
        &dir_snapshot_mem_lowers,
        &managed_snapshot_base_root,
        "pause_to_dir memory snapshot",
    );

    let dir_snapshot_rootfs_lowers = lower_file_paths_from_image_config(
        &dir_snapshot
            .common
            .rootfs_image_config
            .as_ref()
            .context("dir snapshot rootfs image config should exist")?
            .image_config_path,
    )?;
    assert_no_lower_paths_under(
        &dir_snapshot_rootfs_lowers,
        &managed_snapshot_base_root,
        "pause_to_dir rootfs snapshot",
    );
    let actual_snapshot_dir = dir_snapshot.vm_state_path.parent().unwrap();
    assert!(
        actual_snapshot_dir == expected_snapshot_dir.path(),
        "snapshot_to_dir should use the provided directory"
    );
    assert!(
        actual_snapshot_dir.exists(),
        "the provided snapshot directory should exist after snapshot_to_dir"
    );
    assert!(
        dir_snapshot_mem_lowers.iter().all(|path| path.exists()),
        "pause_to_dir memory snapshot should keep all lower files after dropping the paused-state handle"
    );
    assert!(
        dir_snapshot_rootfs_lowers.iter().all(|path| path.exists()),
        "pause_to_dir rootfs snapshot should keep all lower files after dropping the paused-state handle"
    );
    drop(second_snapshot);

    let mut resumed_from_dir =
        FirecrackerSandbox::resume_from_snapshot_config(&dir_snapshot).await?;
    verify_disk_marker(&mut resumed_from_dir).await?;
    resumed_from_dir.stop().await?;
    Ok(())
}

/// Verify multiple resumes from the same snapshot each get independent writable
/// state. Each instance reads the shared marker and writes its own file.
#[tokio::test]
async fn multiple_resumes_have_independent_disk_state() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    write_disk_marker(&mut sandbox).await?;
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    // Resume multiple VMs sequentially from the same snapshot.
    // Each one should see the original marker and produce its own independent snapshot.
    let vm_count = 2;
    let mut snapshots = Vec::new();
    for i in 0..vm_count {
        let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
        verify_disk_marker(&mut resumed).await?;
        // Write a per-instance marker to prove writable state is independent.
        let per_vm_marker = format!("{DISK_MARKER_PATH}.vm{i}");
        let output = resumed
            .run_command("sh", &["-c", &format!("echo ok > {per_vm_marker}")])
            .await?;
        assert_eq!(
            output.exit_code, 0,
            "per-vm write failed: {}",
            output.stderr
        );
        let snap = resumed.pause().await?;
        resumed.stop().await?;
        snapshots.push(snap);
    }

    // Verify each snapshot still has the original marker.
    for snap in &snapshots {
        let mut sandbox = FirecrackerSandbox::resume_from_snapshot_config(snap).await?;
        verify_disk_marker(&mut sandbox).await?;
        sandbox.stop().await?;
    }

    Ok(())
}

/// Ten consecutive pauses of one sandbox stay under a small chain ceiling.
///
/// Needs a real Firecracker host (`/dev/kvm`) and ublk, so it lives in the
/// gated integration target and is skipped with a clear message elsewhere. The
/// printed footprint uses the same method as the staging measurement: allocated
/// bytes deduplicated by device and inode.
///
/// The workload is what makes the ceiling hold. Every pause overwrites the same
/// tmpfs region with new bytes, so each memory generation covers the same guest
/// offsets as the previous one and compaction folds them into a single copy: the
/// compressed chain stays at one generation's worth of pages however many pauses
/// ran. A workload that writes new data every pause cannot be held by any
/// ceiling — its generations are disjoint, so compaction's output equals the sum
/// of its inputs and the chain grows without bound — which is why that shape is
/// the rejection case in
/// [`unique_chain_growth_past_the_ceiling_fails_named_and_keeps_the_sandbox`]
/// rather than the workload here.
///
/// The ceiling bounds the reclaimable part of the chain — the runtime-owned
/// lowers plus the layer each pause appends — and never the stable prefix, which
/// is materialized from the image once and shared by every sandbox of it. The
/// assertions therefore compare the reclaimable part, read through
/// [`FirecrackerSandbox::chain_bytes_for_pause`] so the test and the pause path
/// charge the same bytes; the total footprint is printed beside it for the
/// staging measurement.
///
/// The ceiling itself is measured, never guessed: a warm-up pause with the
/// ballast already written produces exactly one generation of the rewritten
/// region, and the ceiling is that size plus a quarter. That is above what
/// compaction has to leave behind and below the two uncompacted generations the
/// next pause would otherwise carry, so the parity between "the ceiling fires"
/// and "compaction is what brings the chain back under it" is what the assertions
/// observe.
///
/// The ceiling is injected into each sandbox handle instead of being read from
/// the process-wide config: the pause path takes it from the sandbox, and only
/// [`FirecrackerSandbox::build`] fills it from
/// `orchestrator.chain_bytes_ceiling` in production. The pauses go through
/// [`FirecrackerSandbox::pause`], whose managed snapshot root is a runtime-owned
/// root, so the runtime suffix is actually owned and compactable; writing each
/// generation into an arbitrary temp directory would leave the suffix
/// unowned and nothing would ever compact.
///
/// The artifact-directory growth assertion lives with the file-backed persister
/// unit test, which is the only place the stale-generation sweep runs.
#[tokio::test]
async fn ten_consecutive_pauses_stay_under_the_chain_bytes_ceiling() -> Result<()> {
    if !Path::new("/dev/kvm").exists() {
        eprintln!(
            "skipping ten_consecutive_pauses_stay_under_the_chain_bytes_ceiling: /dev/kvm is not \
             available on this host"
        );
        return Ok(());
    }
    common::setup().await;

    let sandbox_config = common::default_sandbox_config()?;
    let mem_size_mib = u64::from(sandbox_config.mem_size_mib);
    // An eighth of guest memory per cycle: comfortably inside the guest's tmpfs
    // budget (default half of RAM) and still large enough that a couple of
    // generations dwarf anything the image contributes. Rewritten in place, so
    // every cycle dirties the SAME guest pages.
    let ballast_mib = (mem_size_mib / 8).max(1);
    let pause_count = 10;

    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    write_disk_marker(&mut sandbox).await?;
    if !chain_ballast_dir_is_tmpfs(&mut sandbox).await? {
        eprintln!(
            "skipping ten_consecutive_pauses_stay_under_the_chain_bytes_ceiling: {CHAIN_TMPFS_DIR} \
             is not tmpfs in this guest, so the ballast would dirty the rootfs chain instead of \
             the memory chain"
        );
        sandbox.stop().await?;
        return Ok(());
    }
    // Allocate the ballast region before the first pause, so every later cycle
    // rewrites pages that already exist. Letting `dd` create the file on the
    // first write of each cycle would place the data on freshly allocated guest
    // pages, and the generations would then cover disjoint offsets — exactly the
    // un-reclaimable shape this test exists to distinguish from.
    seed_chain_ballast(&mut sandbox, ballast_mib).await?;

    // Warm-up: one pause carrying the ballast once. The first capture of a booted
    // guest also carries everything the boot touched, so this generation is the
    // SUPERSET of every later one — the ballast pages are a subset of it. That
    // matters twice: compaction of a chain starting here can never produce more
    // than this, and the chain has to be given room for exactly that much.
    let mut current = sandbox.pause().await?;
    let (warmup_bytes, base_mem_prefix) = memory_chain_parts(&sandbox, &current)?;
    let (base_rootfs_reclaimable, base_rootfs_prefix) = rootfs_chain_parts(&sandbox, &current)?;
    assert!(
        warmup_bytes > 0,
        "the warm-up pause must allocate a memory generation to bound"
    );
    // Steady state: one more pause that only rewrites the same ballast pages.
    // The growth it adds is one pause's worth of churn, which is what decides how
    // soon the chain crosses the ceiling.
    let steady_first = current.clone();
    let mut steady_sandbox =
        FirecrackerSandbox::from_snapshot_config(&current)?.with_chain_bytes_ceiling(0);
    steady_sandbox.start().await?;
    rewrite_chain_ballast(&mut steady_sandbox, ballast_mib).await?;
    let steady_second = steady_sandbox.pause().await?;
    drop(steady_sandbox);
    let before = allocated_bytes(&lower_file_paths_from_image_config(
        &steady_first.mem_overlaybd_config.image_config_path,
    )?);
    let after = allocated_bytes(&lower_file_paths_from_image_config(
        &steady_second.mem_overlaybd_config.image_config_path,
    )?);
    let generation_bytes = after.saturating_sub(before);
    assert!(
        generation_bytes > 0,
        "a pause that rewrites {ballast_mib} MiB must add a generation to the chain: \
         {before} bytes before, {after} after"
    );
    // One value bounds both chains, so it has to clear everything either of them
    // carries that the workload cannot shrink: both shared prefixes and the
    // rootfs chain's restacked part.
    let fixed_bytes = base_mem_prefix
        .max(base_rootfs_prefix)
        .max(base_rootfs_reclaimable);
    // The memory chain's budget: room for the warm-up generation (the most a
    // compaction of this chain can leave behind), plus one steady generation and
    // half of another. The second pause then still fits, the third does not, and
    // from there every pause above the ceiling has a fitting compacted result —
    // so the trigger fires repeatedly inside ten pauses and never rejects one.
    let memory_budget = warmup_bytes
        .saturating_add(generation_bytes)
        .saturating_add(generation_bytes / 2);
    let ceiling = fixed_bytes.max(memory_budget);
    // `0` means "ceiling disabled", so a degenerate fixture must not silently
    // turn enforcement off and pass every assertion for the wrong reason.
    assert!(
        ceiling > 0,
        "the ceiling must be a real bound, not the disabled value: {ceiling}"
    );
    // The two properties the fixture depends on, asserted up front so a red run
    // says which one broke instead of failing later on a missing compaction. A
    // compacted memory chain is at most the warm-up generation, so the ceiling has
    // to hold that much; and the chain grows by one generation per pause, so the
    // ceiling has to be below two generations' growth or ten pauses never reach it.
    assert!(
        ceiling >= warmup_bytes,
        "the ceiling must hold a compacted memory chain: {ceiling} < warm-up {warmup_bytes}"
    );
    assert!(
        ceiling < warmup_bytes.saturating_add(2 * generation_bytes),
        "the chain must reach the ceiling within ten pauses, or nothing compacts: ceiling \
         {ceiling} vs warm-up {warmup_bytes} plus two {generation_bytes} byte generations; the \
         fixed floor that could push it out of reach is {fixed_bytes} (memory prefix \
         {base_mem_prefix}, rootfs prefix {base_rootfs_prefix}, rootfs reclaimable \
         {base_rootfs_reclaimable})"
    );
    eprintln!(
        "measured: warm-up generation {warmup_bytes} bytes, a steady-state pause adds \
         {generation_bytes} bytes ({before} -> {after}); shared prefixes memory \
         {base_mem_prefix} / rootfs {base_rootfs_prefix} bytes, rootfs reclaimable \
         {base_rootfs_reclaimable} bytes, ceiling {ceiling} bytes ({warmup_bytes} warm-up + \
         1.5 x {generation_bytes} steady, floor {fixed_bytes}), ballast {ballast_mib} MiB"
    );
    current = steady_second;

    let mut compaction_seen = false;
    let mut first_footprint = None;
    let mut previous_footprint = None;
    let mut final_reclaimable = 0u64;
    let mut final_prefix = 0u64;
    for pause_index in 0..pause_count {
        let mut resumed =
            FirecrackerSandbox::from_snapshot_config(&current)?.with_chain_bytes_ceiling(ceiling);
        resumed.start().await?;
        rewrite_chain_ballast(&mut resumed, ballast_mib)
            .await
            .with_context(|| format!("rewrite chain ballast for pause {pause_index}"))?;
        // A rejected pause only reports the byte totals, which cannot tell a
        // ceiling that was set too low from a chain whose runtime-owned lowers
        // were misclassified as immutable stable prefix. Classify the chain the
        // pause started from so a red CI run names the cause instead of only the
        // byte counts.
        let snapshot = match resumed.pause().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let message = format!("{error:#}");
                if message.contains("CHAIN_BYTES_CEILING_EXCEEDED") {
                    eprintln!(
                        "pause {} was rejected by the chain ceiling: {message}\n\
                         chain it tried to grow:\n{}",
                        pause_index + 1,
                        describe_chain(&resumed, &current)
                    );
                }
                return Err(error);
            }
        };

        let lowers =
            lower_file_paths_from_image_config(&snapshot.mem_overlaybd_config.image_config_path)?;
        let footprint = allocated_bytes(&lowers);
        first_footprint.get_or_insert(footprint);
        // Print the chain shape the ceiling rejection reports, pause by pause:
        // the lower count, the allocated bytes of each lower and the total, with
        // the per-lower classification below. A chain that compacts does not
        // shrink shows up here as the pause count rising while the total stays
        // at one compacted generation's worth of bytes.
        let per_lower_bytes = per_lower_allocated_bytes(&lowers);
        let change_bytes = previous_footprint
            .map(|previous| footprint as i128 - previous as i128)
            .unwrap_or(0);
        // The ceiling bounds the reclaimable part, so the assertion has to read
        // the same two parts the pause path charges, from the same accounting.
        let (reclaimable, shared_prefix) = memory_chain_parts(&resumed, &snapshot)?;
        let (rootfs_reclaimable, rootfs_prefix) = rootfs_chain_parts(&resumed, &snapshot)?;
        eprintln!(
            "pause {}: chain footprint {} bytes ({} lowers) = {} reclaimable + {} shared prefix, \
             change {:+} bytes, ceiling {} bytes; rootfs chain {} reclaimable + {} shared \
             prefix\nlowers (bytes, deduplicated): {}\n{}",
            pause_index + 1,
            footprint,
            lowers.len(),
            reclaimable,
            shared_prefix,
            change_bytes,
            ceiling,
            rootfs_reclaimable,
            rootfs_prefix,
            format_lower_bytes(&lowers, &per_lower_bytes),
            describe_chain(&resumed, &snapshot)
        );
        previous_footprint = Some(footprint);
        // A compaction rewrote the runtime-owned suffix into one generation.
        if lowers
            .iter()
            .any(|path| path.ends_with("mem_compacted.commit"))
        {
            compaction_seen = true;
        }
        assert!(
            reclaimable <= ceiling,
            "pause {} must leave the reclaimable part of the chain under the ceiling: \
             {reclaimable} > {ceiling} (shared prefix {shared_prefix} bytes is not charged)",
            pause_index + 1
        );
        // The pause path charges the rootfs chain too, and a rootfs breach is
        // terminal, so the workload must not push that chain past the ceiling
        // either: the ballast lives outside the rootfs precisely so it does not.
        assert!(
            rootfs_reclaimable <= ceiling,
            "pause {} must leave the rootfs chain's reclaimable part under the ceiling: \
             {rootfs_reclaimable} > {ceiling} (shared prefix {rootfs_prefix} bytes is not charged)",
            pause_index + 1
        );
        final_reclaimable = reclaimable;
        final_prefix = shared_prefix;
        current = snapshot;
    }

    let final_lowers =
        lower_file_paths_from_image_config(&current.mem_overlaybd_config.image_config_path)?;
    let final_footprint = allocated_bytes(&final_lowers);
    eprintln!(
        "ten pauses: first footprint {} bytes, final footprint {} bytes ({} reclaimable + {} \
         shared prefix), ceiling {} bytes",
        first_footprint.expect("at least one pause ran"),
        final_footprint,
        final_reclaimable,
        final_prefix,
        ceiling
    );

    assert!(
        final_reclaimable <= ceiling,
        "ten pauses must leave the reclaimable part of the chain under the ceiling: \
         {final_reclaimable} > {ceiling}"
    );
    assert!(
        compaction_seen,
        "at least one pause must have compacted the runtime-owned suffix"
    );
    Ok(())
}

/// A pause whose chain grows with data compaction cannot reclaim fails named
/// and keeps the sandbox.
///
/// The complement of [`ten_consecutive_pauses_stay_under_the_chain_bytes_ceiling`]:
/// there the guest rewrites one region, so compaction folds every generation into
/// a single copy and the ceiling holds. Here each pause writes a fresh region and
/// keeps the previous ones, so the generations are disjoint: compaction can only
/// concatenate them, its output equals the sum of its inputs, and a fixed ceiling
/// is unreachable no matter how often it runs. The named rejection is the correct
/// outcome for that workload, and the sandbox must survive it — the pause is a
/// recoverable capture error, so the VM is resumed in place, the handle keeps its
/// runtime state, and the same sandbox pauses as soon as the ceiling allows it.
///
/// Needs a real Firecracker host (`/dev/kvm`), so it is skipped with a clear
/// message elsewhere, like the reclaim test above.
#[tokio::test]
async fn unique_chain_growth_past_the_ceiling_fails_named_and_keeps_the_sandbox() -> Result<()> {
    if !Path::new("/dev/kvm").exists() {
        eprintln!(
            "skipping unique_chain_growth_past_the_ceiling_fails_named_and_keeps_the_sandbox: \
             /dev/kvm is not available on this host"
        );
        return Ok(());
    }
    common::setup().await;

    let sandbox_config = common::default_sandbox_config()?;
    let mem_size_mib = u64::from(sandbox_config.mem_size_mib);
    // An eighth of guest memory per region. Two are written in total, so the
    // fixture uses a quarter of RAM: the guest tmpfs default is half of RAM, so
    // the writes cannot hit ENOSPC and turn the assertion into a storage failure.
    let unique_mib = (mem_size_mib / 8).max(1);

    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    write_disk_marker(&mut sandbox).await?;
    if !chain_ballast_dir_is_tmpfs(&mut sandbox).await? {
        eprintln!(
            "skipping unique_chain_growth_past_the_ceiling_fails_named_and_keeps_the_sandbox: \
             {CHAIN_TMPFS_DIR} is not tmpfs in this guest, so the unique regions would dirty the \
             rootfs chain, whose ceiling breach is terminal"
        );
        sandbox.stop().await?;
        return Ok(());
    }
    let output = sandbox
        .run_command("mkdir", &["-p", CHAIN_UNIQUE_DIR])
        .await
        .context("create the unique-data directory")?;
    anyhow::ensure!(
        output.exit_code == 0,
        "create {} failed: {}",
        CHAIN_UNIQUE_DIR,
        output.stderr
    );

    // Two measured pauses, because the number the rejection depends on is the
    // MARGINAL growth of a unique region, not the size of the base chain: the base
    // chain of a freshly booted guest already carries everything the boot touched,
    // so a ceiling derived from it would sit far above any single extra region and
    // the pause would never be refused.
    //
    // Pause A carries one unique region and defines the base chain. Pause B adds a
    // second one; the difference between their reclaimable parts is what one more
    // disjoint region costs this chain, and compaction can never reclaim it.
    write_unique_chain_data(&mut sandbox, unique_mib, 0).await?;
    let base_snapshot = FirecrackerSandbox::pause(&mut sandbox).await?;
    let (base_mem_reclaimable, base_mem_prefix) = memory_chain_parts(&sandbox, &base_snapshot)?;
    let (base_rootfs_reclaimable, base_rootfs_prefix) =
        rootfs_chain_parts(&sandbox, &base_snapshot)?;
    assert!(
        base_mem_reclaimable > 0,
        "the unique region must allocate a memory generation to bound"
    );
    let mut measure_sandbox =
        FirecrackerSandbox::from_snapshot_config(&base_snapshot)?.with_chain_bytes_ceiling(0);
    measure_sandbox.start().await?;
    write_unique_chain_data(&mut measure_sandbox, unique_mib, 1).await?;
    let grown_snapshot = measure_sandbox.pause().await?;
    let (grown_mem_reclaimable, _) = memory_chain_parts(&measure_sandbox, &grown_snapshot)?;
    drop(measure_sandbox);
    let marginal_bytes = grown_mem_reclaimable.saturating_sub(base_mem_reclaimable);
    assert!(
        marginal_bytes > 0,
        "a second disjoint region must grow the reclaimable part: base \
         {base_mem_reclaimable} bytes, grown {grown_mem_reclaimable}"
    );
    // The ceiling holds the base chain and half of one more region: the base chain
    // fits on its own, and the next disjoint region cannot. That is the statement
    // the rejection makes — compaction ran and could not reclaim a byte of it.
    let fixed_bytes = base_mem_prefix
        .max(base_rootfs_prefix)
        .max(base_rootfs_reclaimable);
    let ceiling = fixed_bytes
        .max(base_mem_reclaimable)
        .saturating_add(marginal_bytes / 2);
    // `0` is the documented disabled value; a degenerate fixture must not select
    // it and then pass because enforcement was off.
    assert!(
        ceiling > 0 && ceiling >= base_mem_reclaimable,
        "the ceiling must hold the base chain and still be a real bound: \
         {base_mem_reclaimable} vs {ceiling}"
    );
    assert!(
        ceiling < base_mem_reclaimable.saturating_add(marginal_bytes),
        "the ceiling must be below the base chain plus one more region, or nothing would be \
         refused: {ceiling} vs {base_mem_reclaimable} + {marginal_bytes}"
    );
    eprintln!(
        "unique-growth base: memory {base_mem_reclaimable} reclaimable over a \
         {base_mem_prefix} byte prefix, rootfs {base_rootfs_reclaimable} reclaimable over a \
         {base_rootfs_prefix} byte prefix, marginal growth per region {marginal_bytes} bytes, \
         unique region {unique_mib} MiB, ceiling {ceiling} bytes (floor {fixed_bytes})"
    );

    // Resume the base chain — the one that fits — under the tight ceiling and add
    // one more unique region. The next chain carries both regions, and compaction
    // cannot drop either, so the pause must be refused by name.
    let mut resumed =
        FirecrackerSandbox::from_snapshot_config(&base_snapshot)?.with_chain_bytes_ceiling(ceiling);
    resumed.start().await?;
    write_unique_chain_data(&mut resumed, unique_mib, 1).await?;
    let error = SandboxBackend::pause(&mut resumed, None)
        .await
        .expect_err("a chain of disjoint generations cannot fit a fixed ceiling");
    let message = format!("{error:#}");
    assert!(
        message.contains("CHAIN_BYTES_CEILING_EXCEEDED"),
        "the rejection must carry the named code: {message}"
    );
    assert!(
        !error.is_terminal(),
        "a ceiling breach must be recoverable, never terminal: {message}"
    );
    eprintln!("expected ceiling rejection: {message}");

    // The runtime state survived the rejection: the VM is running again and both
    // the marker and the unique regions written so far are still there.
    verify_disk_marker(&mut resumed).await?;
    let output = resumed
        .run_command(
            "sh",
            &[
                "-c",
                &format!("test -s {CHAIN_UNIQUE_DIR}/gen-0 && test -s {CHAIN_UNIQUE_DIR}/gen-1"),
            ],
        )
        .await
        .context("check the unique regions survived the rejection")?;
    assert_eq!(
        output.exit_code, 0,
        "both unique regions must survive the rejected pause: {}",
        output.stderr
    );

    // And the same sandbox pauses as soon as the ceiling allows it, which is what
    // makes the rejection recoverable rather than a lost sandbox.
    let mut resumed = resumed.with_chain_bytes_ceiling(0);
    let snapshot = FirecrackerSandbox::pause(&mut resumed)
        .await
        .context("a rejected pause must be retryable once the ceiling is lifted")?;
    let (reclaimable, prefix) = memory_chain_parts(&resumed, &snapshot)?;
    eprintln!(
        "retry with the ceiling disabled: memory chain {reclaimable} reclaimable + {prefix} \
         shared prefix"
    );
    Ok(())
}

/// Guest tmpfs directory the ceiling tests write their data into.
///
/// The data has to live in guest RAM: a file on the overlay rootfs would be
/// captured by the rootfs chain instead of the memory chain, and the two tests
/// would then be asserting against a different device than the one their
/// doc comments describe. `/dev/shm` is the conventional tmpfs mount and is
/// verified to be one before either test proceeds.
const CHAIN_TMPFS_DIR: &str = "/dev/shm";
/// The rewritten-in-place region of the reclaim test.
const CHAIN_BALLAST_PATH: &str = "/dev/shm/aenv-chain-ballast";
/// The append-only directory of the rejection test.
const CHAIN_UNIQUE_DIR: &str = "/dev/shm/aenv-chain-unique";

/// Whether the guest `/dev/shm` really is tmpfs, so the data lands in guest RAM.
///
/// Returns `false` — and the caller skips with a message — instead of writing to
/// whatever filesystem happens to back the path: on a rootfs-backed `/dev/shm`
/// the dirty pages would belong to the rootfs chain, whose ceiling rejection is
/// terminal, so the assertion would test the wrong device and the wrong error
/// classification.
async fn chain_ballast_dir_is_tmpfs(sandbox: &mut FirecrackerSandbox) -> Result<bool> {
    // Pure shell against `/proc/mounts`: the guest carries no guaranteed
    // `stat`/`findmnt`/`awk`, and the guest image is free to change.
    let output = sandbox
        .run_command(
            "sh",
            &[
                "-c",
                &format!(
                    "while read -r _ mount type _; do \
                       if [ \"$mount\" = {CHAIN_TMPFS_DIR} ] && [ \"$type\" = tmpfs ]; then \
                         echo tmpfs; break; \
                       fi; \
                     done < /proc/mounts"
                ),
            ],
        )
        .await
        .context("inspect the guest filesystem type of /dev/shm")?;
    Ok(output.exit_code == 0 && output.stdout.trim() == "tmpfs")
}

/// Size the ballast file once, before the first pause.
///
/// Later cycles overwrite these very pages, which is what keeps every memory
/// generation covering the same guest offsets.
async fn seed_chain_ballast(sandbox: &mut FirecrackerSandbox, size_mib: u64) -> Result<()> {
    let output = sandbox
        .run_command(
            "dd",
            &[
                "if=/dev/zero",
                &format!("of={CHAIN_BALLAST_PATH}"),
                "bs=1M",
                &format!("count={size_mib}"),
            ],
        )
        .await
        .with_context(|| format!("seed {size_mib} MiB chain ballast"))?;
    anyhow::ensure!(
        output.exit_code == 0,
        "chain ballast seed failed ({}): {}",
        output.exit_code,
        output.stderr
    );
    Ok(())
}

/// Overwrite the existing ballast pages with fresh bytes, in place.
///
/// `conv=notrunc` keeps the file's size and extent, so each pause dirties the
/// same offsets as the previous one — the redundancy compaction reclaims. The
/// bytes differ per call so every generation really commits new data; a pause
/// that rewrote the same bytes would leave nothing for the overlay to replace.
async fn rewrite_chain_ballast(sandbox: &mut FirecrackerSandbox, size_mib: u64) -> Result<()> {
    let output = sandbox
        .run_command(
            "dd",
            &[
                "if=/dev/urandom",
                &format!("of={CHAIN_BALLAST_PATH}"),
                "bs=1M",
                &format!("count={size_mib}"),
                "conv=notrunc",
            ],
        )
        .await
        .with_context(|| format!("rewrite {size_mib} MiB chain ballast"))?;
    anyhow::ensure!(
        output.exit_code == 0,
        "chain ballast rewrite failed ({}): {}",
        output.exit_code,
        output.stderr
    );
    Ok(())
}

/// Grow the guest's unique-data footprint by `size_mib` MiB, never rewriting.
///
/// Each call leaves the previous file in place and writes a new one, so the
/// union of the generations covers offsets no earlier generation covered:
/// compaction can only concatenate them, and the chain grows by the full
/// `size_mib` every pause.
async fn write_unique_chain_data(
    sandbox: &mut FirecrackerSandbox,
    size_mib: u64,
    pause_index: usize,
) -> Result<()> {
    let path = format!("{CHAIN_UNIQUE_DIR}/gen-{pause_index}");
    let output = sandbox
        .run_command(
            "dd",
            &[
                "if=/dev/urandom",
                &format!("of={path}"),
                "bs=1M",
                &format!("count={size_mib}"),
            ],
        )
        .await
        .with_context(|| {
            format!("write {size_mib} MiB of unique chain data for pause {pause_index}")
        })?;
    anyhow::ensure!(
        output.exit_code == 0,
        "unique chain data write failed ({}): {}",
        output.exit_code,
        output.stderr
    );
    Ok(())
}

/// The `(reclaimable, shared_prefix)` byte parts of a snapshot's memory chain.
fn memory_chain_parts(
    sandbox: &FirecrackerSandbox,
    snapshot: &FirecrackerSnapshotConfig,
) -> Result<(u64, u64)> {
    sandbox.chain_bytes_for_pause(&snapshot.mem_overlaybd_config.image_config_path)
}

/// The `(reclaimable, shared_prefix)` byte parts of a snapshot's rootfs chain.
fn rootfs_chain_parts(
    sandbox: &FirecrackerSandbox,
    snapshot: &FirecrackerSnapshotConfig,
) -> Result<(u64, u64)> {
    let rootfs = snapshot
        .common
        .rootfs_image_config
        .as_ref()
        .context("paused snapshot should carry a rootfs image config")?;
    sandbox.chain_bytes_for_pause(&rootfs.image_config_path)
}

/// Report how `sandbox` classifies each lower of `snapshot`'s memory chain.
///
/// Used on the ceiling paths so a red run shows the classification (runtime-owned
/// vs stable prefix) and the roots in force, not only the byte totals.
fn describe_chain(sandbox: &FirecrackerSandbox, snapshot: &FirecrackerSnapshotConfig) -> String {
    match sandbox.describe_chain_classification(&snapshot.mem_overlaybd_config.image_config_path) {
        Ok(report) => report,
        Err(error) => format!(
            "<failed to classify {}: {error:#}>",
            snapshot.mem_overlaybd_config.image_config_path.display()
        ),
    }
}

/// Allocated bytes of a set of layer paths, deduplicated by device and inode.
fn allocated_bytes(paths: &[PathBuf]) -> u64 {
    let mut seen = std::collections::HashSet::new();
    paths
        .iter()
        .filter_map(|path| fs::metadata(path).ok())
        .filter(|metadata| seen.insert((metadata.dev(), metadata.ino())))
        .map(|metadata| metadata.blocks().saturating_mul(512))
        .sum()
}

/// Allocated bytes of each layer path, deduplicated by device and inode.
///
/// Reports `0` for a path a hardlink already accounted for and for one that
/// cannot be inspected, so the printed per-lower figures sum to
/// [`allocated_bytes`] over the same paths and a hardlinked generation is
/// visible as the lower that contributed nothing.
fn per_lower_allocated_bytes(paths: &[PathBuf]) -> Vec<u64> {
    let mut seen = std::collections::HashSet::new();
    paths
        .iter()
        .map(|path| {
            fs::metadata(path)
                .ok()
                .filter(|metadata| seen.insert((metadata.dev(), metadata.ino())))
                .map(|metadata| metadata.blocks().saturating_mul(512))
                .unwrap_or(0)
        })
        .collect()
}

/// Render `paths` and their allocated bytes as one comma-separated line.
fn format_lower_bytes(paths: &[PathBuf], bytes: &[u64]) -> String {
    paths
        .iter()
        .zip(bytes)
        .enumerate()
        .map(|(index, (path, allocated))| format!("[{index}] {allocated} bytes {}", path.display()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Runs a connectivity probe, retrying expected successes so transient public
/// DNS or upstream failures do not fail policy assertions. Expected failures
/// run once: a retry cannot turn a correctly blocked connection into a pass.
async fn run_connectivity_probe(
    sandbox: &mut FirecrackerSandbox,
    cmd: &str,
    args: &[&str],
    should_succeed: bool,
) -> Result<ProcessOutput> {
    const ATTEMPTS: usize = 3;
    let mut attempt = 1;
    loop {
        let output = sandbox.run_command(cmd, args).await?;
        if output.exit_code == 0 || !should_succeed || attempt == ATTEMPTS {
            return Ok(output);
        }
        tracing::warn!(
            attempt,
            exit_code = output.exit_code,
            stderr = %output.stderr,
            "connectivity probe failed; retrying"
        );
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn assert_tcp_connect(
    sandbox: &mut FirecrackerSandbox,
    destination: &str,
    should_succeed: bool,
) -> Result<()> {
    let cmd = format!("timeout 5 bash -lc ': </dev/tcp/{destination}'");
    let output = run_connectivity_probe(sandbox, "bash", &["-lc", &cmd], should_succeed).await?;
    assert_eq!(
        output.exit_code == 0,
        should_succeed,
        "tcp connectivity expectation failed for {destination}; exit={}, stdout={}, stderr={}",
        output.exit_code,
        output.stdout,
        output.stderr
    );
    Ok(())
}

async fn assert_curl(
    sandbox: &mut FirecrackerSandbox,
    url: &str,
    should_succeed: bool,
) -> Result<()> {
    let output = run_connectivity_probe(
        sandbox,
        "curl",
        &[
            "--noproxy",
            "*",
            "-4",
            "-sS",
            "--connect-timeout",
            "5",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "--",
            url,
        ],
        should_succeed,
    )
    .await?;
    assert_eq!(
        output.exit_code == 0,
        should_succeed,
        "curl expectation failed for {url}; exit={}, stdout={}, stderr={}",
        output.exit_code,
        output.stdout,
        output.stderr
    );
    Ok(())
}

async fn test_network(sandbox: &mut FirecrackerSandbox, after_resume: bool) -> Result<()> {
    let checks = [("tcp_ip", "8.8.8.8/53"), ("tcp_dns", "www.baidu.com/443")];

    for (name, destination) in checks {
        if let Err(err) = assert_tcp_connect(sandbox, destination, true).await {
            tracing::error!(
                name,
                after_resume,
                error = %err,
                "connectivity check failed"
            );
            return Err(err).with_context(|| format!("connectivity check failed: {name}"));
        }
    }

    Ok(())
}

#[tokio::test]
async fn microvm_can_access_internet() -> Result<()> {
    common::setup().await;
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    test_network(&mut sandbox, false).await?;
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed_sandbox = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    test_network(&mut resumed_sandbox, true).await?;
    resumed_sandbox.stop().await?;

    Ok(())
}

#[tokio::test]
async fn microvm_network_policy_controls_egress() -> Result<()> {
    common::setup().await;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy = Some(SandboxNetworkPolicy::new(
        true,
        BaseSandboxNetworkPolicy::Deny,
        SandboxNetworkEgressPolicy::new(Some(vec!["8.8.8.8".to_string()]), None)?,
    ));

    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", true).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(Some(vec!["1.1.1.1".to_string()]), None)?,
        )))
        .await?;

    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", false).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", true).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Allow,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["8.8.8.8".to_string()]),
                Some(vec!["8.8.8.0/24".to_string(), "1.1.1.1".to_string()]),
            )?,
        )))
        .await?;

    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", true).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", false).await?;
    assert_tcp_connect(&mut sandbox, "1.0.0.1/53", true).await?;

    sandbox.update_network_policy(None).await?;
    assert_tcp_connect(&mut sandbox, "8.8.8.8/53", true).await?;
    assert_tcp_connect(&mut sandbox, "1.1.1.1/53", true).await?;
    assert_tcp_connect(&mut sandbox, "10.255.255.254/80", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Allow,
            SandboxNetworkEgressPolicy::new(Some(vec!["10.0.0.0/8".to_string()]), None)?,
        )))
        .await?;
    assert_tcp_connect(&mut sandbox, "10.255.255.254/80", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["www.baidu.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )?,
        )))
        .await?;

    assert_curl(&mut sandbox, "http://www.baidu.com/", true).await?;
    assert_curl(&mut sandbox, "https://www.baidu.com/", true).await?;
    assert_curl(&mut sandbox, "https://www.qq.com/", false).await?;

    sandbox
        .update_network_policy(Some(SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Default,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["www.qq.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )?,
        )))
        .await?;

    assert_curl(&mut sandbox, "https://www.baidu.com/", false).await?;
    assert_curl(&mut sandbox, "https://www.qq.com/", true).await?;

    sandbox.update_network_policy(None).await?;
    assert_curl(&mut sandbox, "https://www.baidu.com/", true).await?;
    assert_curl(&mut sandbox, "https://www.qq.com/", true).await?;

    sandbox.stop().await?;
    Ok(())
}
