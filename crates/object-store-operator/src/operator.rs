use std::future::Future;
use std::time::Duration;

use anyhow::Context;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::services::{Gcs, Oss, S3};
use thiserror::Error;

use crate::auth::{CachedCredentialSource, ResolvedCredential};
use crate::{OpenDalError, OpenDalErrorKind, OpenDalResult, Operator};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_RETRIES: usize = 3;

pub type ObjectStoreOperatorResult<T> = std::result::Result<T, ObjectStoreOperatorError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AddressingStyle {
    Virtual,
    Path,
}

impl AddressingStyle {
    pub fn uses_virtual_host_style(&self) -> bool {
        matches!(self, Self::Virtual)
    }
}

/// The API an operator speaks. Most supported stores speak S3; Alibaba OSS's
/// native API additionally appends to objects, which its S3 API cannot, and
/// Google Cloud Storage is reached through its native JSON/XML API.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ObjectStoreService {
    #[default]
    S3,
    AliyunOss,
    Gcs,
}

/// How a [`ObjectStoreService::Gcs`] operator authenticates.
///
/// Google Cloud Storage does not use the access-key credentials of the other
/// services. By default the operator resolves Application Default Credentials
/// (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud well-known file, then the
/// metadata server), which is what makes GKE Workload Identity work without
/// any static key. The token is refreshed by the operator itself.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum GcsAuth {
    /// Application Default Credentials.
    #[default]
    ApplicationDefault,
    /// A service-account or authorized-user JSON file.
    CredentialFile(String),
    /// Unauthenticated requests: public buckets and emulators.
    Anonymous,
}

#[derive(Clone, Debug)]
pub struct ObjectStoreOperatorConfig {
    pub service: ObjectStoreService,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub addressing_style: AddressingStyle,
    /// Authentication for [`ObjectStoreService::Gcs`]; ignored by other services.
    pub gcs_auth: GcsAuth,
    pub timeout: Option<Duration>,
    pub max_retries: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct OperatorWithCredential {
    operator: Operator,
    credential: Option<ResolvedCredential>,
}

impl OperatorWithCredential {
    pub fn new(operator: Operator, credential: Option<ResolvedCredential>) -> Self {
        Self {
            operator,
            credential,
        }
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    pub fn credential(&self) -> Option<&ResolvedCredential> {
        self.credential.as_ref()
    }
}

#[derive(Debug, Error)]
pub enum ObjectStoreOperatorError {
    #[error(transparent)]
    OpenDal(#[from] OpenDalError),
    #[error("credential refresh failed")]
    CredentialRefresh(#[source] anyhow::Error),
    #[error("operator build failed")]
    OperatorBuild(#[source] anyhow::Error),
}

pub fn build_object_store_operator(
    config: &ObjectStoreOperatorConfig,
    credential: Option<&ResolvedCredential>,
) -> ObjectStoreOperatorResult<Operator> {
    let operator = match config.service {
        ObjectStoreService::S3 => Operator::new(s3_builder(config, credential)).map(|b| b.finish()),
        ObjectStoreService::AliyunOss => {
            Operator::new(aliyun_oss_builder(config, credential)).map(|b| b.finish())
        }
        ObjectStoreService::Gcs => Operator::new(gcs_builder(config)).map(|b| b.finish()),
    }
    .context("build OpenDAL object-store operator")
    .map_err(ObjectStoreOperatorError::OperatorBuild)?;

    let timeout = config.timeout.unwrap_or(DEFAULT_TIMEOUT);
    let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
    Ok(operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(timeout)
                .with_io_timeout(timeout),
        )
        .layer(RetryLayer::new().with_max_times(max_retries)))
}

/// The object name under which a GCS operator stores `name`.
///
/// Names pass through this before they reach OpenDAL because OpenDAL 0.55's
/// GCS `write` of a single-request object (anything below the multipart part
/// size) percent-encodes the name twice: the writer encodes it, then the
/// request builder encodes the result. The server decodes the query parameter
/// once, so a name with a character that encoding changes (`:` in
/// `sha256:<hex>`, a space, a non-ASCII letter) is created under its
/// once-encoded spelling (`sha256%3A<hex>`), while every read, stat, list and
/// delete addresses the real one. Such an object is written and then reported
/// absent.
///
/// A name whose characters need no encoding is the same however many times it
/// is encoded, so the unsafe ones are replaced with `~XX`, `XX` being the
/// uppercase hex of one UTF-8 byte; `~` is escaped too, which keeps the
/// mapping reversible by [`gcs_object_key_decode`]. Names made of ASCII
/// letters, digits, `-`, `_`, `.` and `/` map to themselves.
pub fn gcs_object_key(name: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut key = String::with_capacity(name.len());
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'/' => {
                key.push(char::from(byte));
            }
            _ => {
                key.push('~');
                key.push(char::from(HEX[usize::from(byte >> 4)]));
                key.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    key
}

/// The name that [`gcs_object_key`] mapped to `key`. A `~` that does not start
/// a valid escape is kept as it is.
pub fn gcs_object_key_decode(key: &str) -> String {
    let bytes = key.as_bytes();
    let mut name = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let escaped = (bytes[at] == b'~')
            .then(|| bytes.get(at + 1..at + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                name.push(byte);
                at += 3;
            }
            None => {
                name.push(bytes[at]);
                at += 1;
            }
        }
    }
    String::from_utf8_lossy(&name).into_owned()
}

/// Builds a GCS operator. `endpoint` is only set for emulators; an empty one
/// selects `https://storage.googleapis.com`.
fn gcs_builder(config: &ObjectStoreOperatorConfig) -> Gcs {
    let mut builder = Gcs::default().bucket(&config.bucket);
    if !config.endpoint.trim().is_empty() {
        builder = builder.endpoint(config.endpoint.trim());
    }
    match &config.gcs_auth {
        GcsAuth::ApplicationDefault => builder,
        GcsAuth::CredentialFile(path) => builder.credential_path(path),
        // Skip token discovery too, or an emulator run would wait on the
        // metadata server.
        GcsAuth::Anonymous => builder
            .allow_anonymous()
            .disable_config_load()
            .disable_vm_metadata(),
    }
}

fn aliyun_oss_builder(
    config: &ObjectStoreOperatorConfig,
    credential: Option<&ResolvedCredential>,
) -> Oss {
    let mut builder = Oss::default()
        .bucket(&config.bucket)
        .endpoint(&config.endpoint);
    if let Some(credential) = credential {
        builder = builder.access_key_id(&credential.access_key_id);
        builder = builder.access_key_secret(&credential.secret_access_key);
        if let Some(token) = credential.security_token.as_deref() {
            builder = builder.security_token(token);
        }
    } else {
        builder = builder.allow_anonymous();
    }
    builder
}

fn s3_builder(config: &ObjectStoreOperatorConfig, credential: Option<&ResolvedCredential>) -> S3 {
    let mut builder = S3::default()
        .bucket(&config.bucket)
        .endpoint(&config.endpoint)
        .region(&config.region);

    if config.addressing_style.uses_virtual_host_style() {
        builder = builder.enable_virtual_host_style();
    }

    if let Some(credential) = credential {
        builder = builder.access_key_id(&credential.access_key_id);
        builder = builder.secret_access_key(&credential.secret_access_key);
        if let Some(token) = credential.security_token.as_deref() {
            builder = builder.session_token(token);
        }
    } else {
        builder = builder.allow_anonymous();
    }
    builder
}

pub async fn run_with_refresh<T, F, Fut>(
    current: &OperatorWithCredential,
    credentials: Option<&CachedCredentialSource>,
    config: &ObjectStoreOperatorConfig,
    op: F,
) -> ObjectStoreOperatorResult<(T, Option<OperatorWithCredential>)>
where
    F: Fn(Operator) -> Fut,
    Fut: Future<Output = OpenDalResult<T>>,
{
    match op(current.operator().clone()).await {
        Ok(value) => Ok((value, None)),
        Err(err) if err.kind() == OpenDalErrorKind::PermissionDenied => {
            // Only permission-denied responses trigger credential refresh.
            // This matches the backends we currently target, including Aliyun
            // OSS, where expired credentials surface as 403-style failures.
            // Other authentication errors are treated as terminal until we
            // explicitly broaden the retry contract.
            let Some(credentials) = credentials else {
                return Err(ObjectStoreOperatorError::OpenDal(err));
            };
            let Some(previous) = current.credential() else {
                return Err(ObjectStoreOperatorError::OpenDal(err));
            };
            let Some(refreshed) = credentials
                .force_refresh(previous)
                .await
                .map_err(ObjectStoreOperatorError::CredentialRefresh)?
            else {
                return Err(ObjectStoreOperatorError::OpenDal(err));
            };
            let operator = build_object_store_operator(config, Some(&refreshed))?;
            let value = op(operator.clone()).await?;
            Ok((
                value,
                Some(OperatorWithCredential::new(operator, Some(refreshed))),
            ))
        }
        Err(err) => Err(ObjectStoreOperatorError::OpenDal(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_object_store_operator, gcs_object_key, gcs_object_key_decode, AddressingStyle,
        GcsAuth, ObjectStoreOperatorConfig, ObjectStoreService,
    };

    fn gcs_config(endpoint: &str, gcs_auth: GcsAuth) -> ObjectStoreOperatorConfig {
        ObjectStoreOperatorConfig {
            service: ObjectStoreService::Gcs,
            bucket: "snapshots".to_string(),
            endpoint: endpoint.to_string(),
            region: String::new(),
            addressing_style: AddressingStyle::Path,
            gcs_auth,
            timeout: None,
            max_retries: None,
        }
    }

    #[test]
    fn gcs_object_keys_leave_ordinary_names_alone() {
        for name in [
            "catalog/aliases/team-a.json",
            "managed-layers/layer_1.v2",
            "a/b/c",
        ] {
            assert_eq!(gcs_object_key(name), name);
        }
    }

    #[test]
    fn gcs_object_keys_escape_what_opendal_would_encode_twice() {
        assert_eq!(
            gcs_object_key("managed-layers/sha256:ab"),
            "managed-layers/sha256~3Aab"
        );
        assert_eq!(gcs_object_key("a b"), "a~20b");
        assert_eq!(gcs_object_key("\u{e9}"), "~C3~A9");
        assert_eq!(gcs_object_key("~"), "~7E");
    }

    #[test]
    fn gcs_object_keys_round_trip() {
        for name in [
            "images/sha256:0123.json",
            "a b/c~d",
            "caf\u{e9}/\u{65e5}\u{672c}",
            "100%/~7E",
            "trailing~",
            "~4",
            "",
        ] {
            let key = gcs_object_key(name);
            assert!(
                key.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_./~".contains(&b)),
                "{key}"
            );
            assert_eq!(gcs_object_key_decode(&key), name, "{name}");
        }
        // A tilde that starts no escape is kept.
        assert_eq!(gcs_object_key_decode("a~zzb"), "a~zzb");
    }

    #[test]
    fn gcs_operator_targets_the_configured_bucket() {
        let operator = build_object_store_operator(
            &gcs_config("http://127.0.0.1:4443", GcsAuth::Anonymous),
            None,
        )
        .expect("build gcs operator");
        let info = operator.info();
        assert_eq!(info.scheme(), "gcs");
        assert_eq!(info.name(), "snapshots");
    }

    #[test]
    fn gcs_operator_advertises_create_only_writes_but_not_etag_writes() {
        let operator = build_object_store_operator(&gcs_config("", GcsAuth::Anonymous), None)
            .expect("build gcs operator");
        let capability = operator.info().full_capability();
        // The repository's document locking is built on these two facts.
        assert!(capability.write_with_if_not_exists);
        assert!(!capability.write_with_if_match);
        assert!(capability.write_can_multi);
    }

    #[test]
    fn gcs_operator_rejects_an_empty_bucket() {
        let mut config = gcs_config("", GcsAuth::Anonymous);
        config.bucket.clear();
        build_object_store_operator(&config, None).expect_err("empty bucket must not build");
    }

    #[test]
    fn addressing_style_maps_to_expected_s3_modes() {
        assert!(AddressingStyle::Virtual.uses_virtual_host_style());
        assert!(!AddressingStyle::Path.uses_virtual_host_style());
    }
}
