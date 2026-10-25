mod client;
mod config;
mod documents;
mod epoch_lease;
#[cfg(test)]
mod fake_gcs;
#[cfg(test)]
mod gcs_tests;
mod lease;
mod repository;

pub(crate) use self::client::OssClient;
pub(crate) use self::config::{NormalizedGcsConfig, NormalizedOssConfig};
pub(crate) use self::repository::OssRepository;
