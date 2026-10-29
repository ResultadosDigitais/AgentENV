//! The managed-layer collector's behavior table against MinIO.
//!
//! `agentenv::behavior_table!` carries the scenarios; this file only supplies
//! the OSS backend, so both backends are verified against one list of cases
//! instead of each against its own.

use agentenv::cfg::{ConfigManager, OssBackendConfig, SnapshotPublishCompressionConfig};
use agentenv::repository::Repository;
use agentenv::snapshot::backends::OssBackend;
use agentenv_test_support::minio::{MinioFixture, MINIO_PASS, MINIO_USER};
use tempfile::TempDir;
use tokio::sync::OnceCell;

/// One MinIO instance shared by the cases: starting a container per case would
/// dominate the run, and every case works under its own object prefix.
static FIXTURE: OnceCell<MinioFixture> = OnceCell::const_new();

async fn fixture() -> &'static MinioFixture {
    FIXTURE.get_or_init(|| async { MinioFixture::start().await.expect("start MinIO") }).await
}

fn ensure_test_config() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config_root = workspace_root.join("target").join("managed-layer-collect-config");
        std::fs::create_dir_all(&config_root).expect("config dir");
        std::env::set_var("AENV_CONFIG_PATH", workspace_root.join("config").join("default.toml"));
        std::env::set_var("AENV_HOME_PATH", &config_root);
        std::env::set_var("AENV_DEPS_PATH", config_root.join("deps"));
        std::env::set_var("AENV_SNAPSHOT_LOCAL_CACHE_PATH", config_root.join("cache"));
        ConfigManager::init_global().expect("init config");
    });
}

/// Builds an OSS repository over a fresh object prefix of the shared bucket.
///
/// The bucket is shared, so a unique prefix per case keeps the passes from
/// seeing each other's layers.
async fn oss_repository(root: &std::path::Path) -> std::sync::Arc<dyn Repository> {
    ensure_test_config();
    let fixture = fixture().await;
    let prefix = format!("managed-layer-collect/{}", uuid_prefix(root));
    let config = OssBackendConfig {
        endpoint: fixture.endpoint.clone(),
        bucket: fixture.bucket.clone(),
        prefix: Some(prefix),
        credential_process: None,
        access_key_id: Some(MINIO_USER.to_string()),
        access_key_secret: Some(MINIO_PASS.to_string()),
        security_token: None,
        region: Some(fixture.region.clone()),
        addressing_style: None,
        cache_max_size_gb: Some(1),
    };
    let backend = OssBackend::new_with_publish_compression(
        &config,
        root.join("cache"),
        &SnapshotPublishCompressionConfig::default(),
    )
    .expect("build OSS backend");
    let (repository, _resolver) = backend.into_parts();
    repository
}

fn uuid_prefix(root: &std::path::Path) -> String {
    let temp = TempDir::new_in(root).expect("tempdir");
    // The directory name is unique per case, which makes the prefix unique.
    temp.path()
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("case")
        .to_owned()
}

// The OSS backend keeps its own clock, so these cases ask for a zero grace
// instead of aging a file: `age_layer` cannot reach a bucket.
agentenv::behavior_table!(
    oss,
    oss_repository,
    |temp: &tempfile::TempDir| temp.path().join("never-aged"),
    false
);
