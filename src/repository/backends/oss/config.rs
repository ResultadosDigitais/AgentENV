use anyhow::{Context, Result};
use object_store_operator::{
    credential_source_from_fields, normalized_credential, AddressingStyle, CredentialFields,
    CredentialSource, CredentialSourceOptions, GcsAuth,
};

use super::client::OssClient;
use crate::cfg::{
    GcsBackendConfig, OssAddressingStyle, OssBackendConfig, SnapshotImageStoragePolicy,
};

/// `[backend.gcs]` with blanks trimmed and the conflicting settings rejected.
#[derive(Debug, Clone)]
pub(crate) struct NormalizedGcsConfig {
    bucket: String,
    prefix: String,
    endpoint: String,
    auth: GcsAuth,
}

impl NormalizedGcsConfig {
    pub(crate) fn new(config: &GcsBackendConfig) -> Result<Self> {
        let bucket = normalized_credential(Some(config.bucket.as_str()))
            .context("backend.gcs.bucket cannot be empty")?
            .to_string();
        let prefix = config
            .prefix
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        let endpoint = normalized_credential(config.endpoint.as_deref())
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();
        let credential_path = normalized_credential(config.credential_path.as_deref());
        let adc = config.allow_application_default_credentials;
        let auth = match (credential_path, config.allow_anonymous, adc) {
            (Some(_), true, _) => anyhow::bail!(
                "backend.gcs.credential_path cannot be combined with backend.gcs.allow_anonymous"
            ),
            (Some(_), _, true) => anyhow::bail!(
                "backend.gcs.credential_path cannot be combined with \
                 backend.gcs.allow_application_default_credentials"
            ),
            (None, true, true) => anyhow::bail!(
                "backend.gcs.allow_anonymous cannot be combined with \
                 backend.gcs.allow_application_default_credentials"
            ),
            (Some(path), false, false) => GcsAuth::CredentialFile(path.to_string()),
            (None, true, false) => GcsAuth::Anonymous,
            (None, false, true) => GcsAuth::ApplicationDefault,
            (None, false, false) => anyhow::bail!(
                "backend.gcs needs credentials: set backend.gcs.credential_path (recommended), \
                 or explicitly backend.gcs.allow_application_default_credentials = true, \
                 or backend.gcs.allow_anonymous = true for emulators"
            ),
        };
        Ok(Self {
            bucket,
            prefix,
            endpoint,
            auth,
        })
    }

    /// The emulator endpoint; empty for Google's own.
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn auth(&self) -> &GcsAuth {
        &self.auth
    }

    pub(crate) fn client(&self) -> OssClient {
        OssClient::new_gcs(
            self.bucket.clone(),
            self.endpoint.clone(),
            self.prefix.clone(),
            self.auth.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NormalizedOssConfig {
    bucket: String,
    endpoint: String,
    region: String,
    prefix: String,
    credential_source: CredentialSource,
    snapshot_image_storage: SnapshotImageStoragePolicy,
    addressing_style: Option<AddressingStyle>,
}

impl NormalizedOssConfig {
    pub(crate) fn new(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
    ) -> Result<Self> {
        let bucket = normalized_credential(Some(config.bucket.as_str()))
            .context("backend.oss.bucket cannot be empty")?
            .to_string();
        let endpoint = normalized_credential(Some(config.endpoint.as_str()))
            .context("backend.oss.endpoint cannot be empty")?
            .to_string();
        // Region is only consumed later by overlaybd global-config generation.
        // The runtime backend keeps the validation here so incomplete OSS config
        // still fails fast during backend construction.
        let region = normalized_credential(config.region.as_deref())
            .context("backend.oss.region is required")?
            .to_string();
        let prefix = config
            .prefix
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        let credential_source = credential_source_from_fields(
            CredentialFields {
                access_key_id: config.access_key_id.as_deref(),
                secret_access_key: config.access_key_secret.as_deref(),
                security_token: config.security_token.as_deref(),
                credential_process: config.credential_process.as_deref(),
            },
            CredentialSourceOptions {
                scope: "backend.oss",
                allow_anonymous: false,
                required_access_key_id_label: "backend.oss.access_key_id",
                required_secret_access_key_label: "backend.oss.access_key_secret",
            },
        )?;
        let addressing_style = config.addressing_style.map(|style| match style {
            OssAddressingStyle::Path => AddressingStyle::Path,
            OssAddressingStyle::Virtual => AddressingStyle::Virtual,
        });
        Ok(Self {
            bucket,
            endpoint,
            region,
            prefix,
            credential_source,
            snapshot_image_storage,
            addressing_style,
        })
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn region(&self) -> &str {
        &self.region
    }

    /// Explicit bucket addressing style override, if configured. `None` means
    /// the client should fall back to endpoint-based auto-detection.
    pub(crate) fn addressing_style(&self) -> Option<AddressingStyle> {
        self.addressing_style
    }

    pub(crate) fn credential_source(&self) -> CredentialSource {
        self.credential_source.clone()
    }

    pub(crate) fn snapshot_image_storage(&self) -> SnapshotImageStoragePolicy {
        self.snapshot_image_storage
    }
}

#[cfg(test)]
mod tests {
    use object_store_operator::GcsAuth;

    use super::{NormalizedGcsConfig, NormalizedOssConfig};
    use crate::cfg::{GcsBackendConfig, OssBackendConfig, SnapshotImageStoragePolicy};

    fn gcs_config() -> GcsBackendConfig {
        GcsBackendConfig {
            bucket: " snapshots ".to_string(),
            prefix: Some(" /agentenv/prod/ ".to_string()),
            credential_path: Some("/etc/gcp/credential.json".to_string()),
            endpoint: None,
            allow_anonymous: false,
            allow_application_default_credentials: false,
            cache_max_size_gb: None,
        }
    }

    #[test]
    fn gcs_config_trims_and_selects_the_credential_file() {
        let mut config = gcs_config();
        config.credential_path = Some(" /etc/gcp/key.json ".to_string());
        config.endpoint = Some(" http://127.0.0.1:4443/ ".to_string());

        let normalized = NormalizedGcsConfig::new(&config).expect("normalize");
        assert_eq!(normalized.bucket, "snapshots");
        assert_eq!(normalized.prefix, "agentenv/prod");
        assert_eq!(normalized.endpoint(), "http://127.0.0.1:4443");
        assert_eq!(
            normalized.auth(),
            &GcsAuth::CredentialFile("/etc/gcp/key.json".to_string())
        );
    }

    #[test]
    fn gcs_config_refuses_application_default_credentials_by_default() {
        let mut config = gcs_config();
        config.credential_path = None;

        let err = NormalizedGcsConfig::new(&config).expect_err("no credentials must fail");
        let message = err.to_string();
        assert!(message.contains("credential_path"), "{message}");
        assert!(
            message.contains("allow_application_default_credentials"),
            "{message}"
        );
    }

    #[test]
    fn gcs_config_allows_application_default_credentials_only_when_asked() {
        let mut config = gcs_config();
        config.credential_path = None;
        config.allow_application_default_credentials = true;

        let normalized = NormalizedGcsConfig::new(&config).expect("explicit opt-in");
        assert_eq!(normalized.auth(), &GcsAuth::ApplicationDefault);
    }

    #[test]
    fn gcs_config_rejects_conflicting_credential_choices() {
        let mut config = gcs_config();
        config.allow_application_default_credentials = true;
        let err = NormalizedGcsConfig::new(&config).expect_err("file and ADC");
        assert!(err.to_string().contains("cannot be combined"));

        let mut config = gcs_config();
        config.credential_path = None;
        config.allow_anonymous = true;
        config.allow_application_default_credentials = true;
        let err = NormalizedGcsConfig::new(&config).expect_err("anonymous and ADC");
        assert!(err.to_string().contains("cannot be combined"));
    }

    #[test]
    fn gcs_config_allows_anonymous_requests_only_without_a_credential_file() {
        let mut config = gcs_config();
        config.credential_path = None;
        config.allow_anonymous = true;
        assert_eq!(
            NormalizedGcsConfig::new(&config).expect("anonymous").auth(),
            &GcsAuth::Anonymous
        );

        config.credential_path = Some("/etc/gcp/key.json".to_string());
        let err = NormalizedGcsConfig::new(&config).expect_err("mixed auth must fail");
        assert!(err.to_string().contains("cannot be combined"));
    }

    #[test]
    fn gcs_config_requires_a_bucket() {
        let mut config = gcs_config();
        config.bucket = "  ".to_string();
        let err = NormalizedGcsConfig::new(&config).expect_err("blank bucket must fail");
        assert!(err.to_string().contains("backend.gcs.bucket"));
    }

    #[test]
    fn gcs_client_names_objects_with_gs_urls_under_the_prefix() {
        let client = NormalizedGcsConfig::new(&gcs_config())
            .expect("normalize")
            .client();
        assert_eq!(
            client.object_url("managed-layers"),
            "gs://snapshots/agentenv/prod/managed-layers"
        );
    }

    fn sample_config() -> OssBackendConfig {
        OssBackendConfig {
            endpoint: " https://oss-cn-hangzhou.aliyuncs.com ".to_string(),
            bucket: " demo-bucket ".to_string(),
            prefix: Some(" /snapshots/managed/ ".to_string()),
            credential_process: None,
            access_key_id: Some(" ak ".to_string()),
            access_key_secret: Some(" sk ".to_string()),
            security_token: Some(" token ".to_string()),
            region: Some(" cn-hangzhou ".to_string()),
            addressing_style: None,
            cache_max_size_gb: Some(8),
        }
    }

    #[test]
    fn normalized_config_trims_and_derives_shared_values() {
        let normalized =
            NormalizedOssConfig::new(&sample_config(), SnapshotImageStoragePolicy::ObjectStorage)
                .expect("normalize config");

        assert_eq!(normalized.bucket(), "demo-bucket");
        assert_eq!(
            normalized.endpoint(),
            "https://oss-cn-hangzhou.aliyuncs.com"
        );
        assert_eq!(normalized.prefix(), "snapshots/managed");
        assert_eq!(
            normalized.snapshot_image_storage(),
            crate::cfg::SnapshotImageStoragePolicy::ObjectStorage
        );
    }

    #[test]
    fn normalized_config_accepts_snapshot_image_publish_policy() {
        let normalized =
            NormalizedOssConfig::new(&sample_config(), SnapshotImageStoragePolicy::SourceRegistry)
                .expect("normalize config");

        assert_eq!(
            normalized.snapshot_image_storage(),
            crate::cfg::SnapshotImageStoragePolicy::SourceRegistry
        );
    }

    #[test]
    fn normalized_config_rejects_mixed_credential_sources() {
        let mut config = sample_config();
        config.credential_process = Some("echo creds".to_string());

        let err = NormalizedOssConfig::new(&config, SnapshotImageStoragePolicy::ObjectStorage)
            .expect_err("mixed credentials must fail");
        assert!(err
            .to_string()
            .contains("credential_process cannot be combined"));
    }

    #[test]
    fn normalized_config_requires_region() {
        let mut config = sample_config();
        config.region = Some(" ".to_string());

        let err = NormalizedOssConfig::new(&config, SnapshotImageStoragePolicy::ObjectStorage)
            .expect_err("missing region must fail");
        assert!(err.to_string().contains("backend.oss.region is required"));
    }
}
