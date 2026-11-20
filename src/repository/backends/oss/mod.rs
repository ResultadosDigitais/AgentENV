mod client;
mod config;
mod documents;
mod epoch_lease;
// The managed-layer behavior table runs against this fake too, so it is visible
// to the crate's test build rather than to this module alone.
#[cfg(test)]
pub(crate) mod fake_gcs;
#[cfg(test)]
mod gcs_tests;
mod lease;
mod repository;

pub(crate) use self::client::OssClient;
pub(crate) use self::config::{NormalizedGcsConfig, NormalizedOssConfig};
pub(crate) use self::repository::OssRepository;
