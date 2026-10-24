//! The repository client against a Google Cloud Storage endpoint, over the
//! wire protocol OpenDAL's `gcs` service speaks. See [`super::fake_gcs`] for
//! what the endpoint models.

use std::sync::Arc;

use object_store_operator::{
    build_object_store_operator, gcs_object_key, AddressingStyle, GcsAuth,
    ObjectStoreOperatorConfig, ObjectStoreService,
};
use overlaybd::backend::oss::upload_file_streaming;

use super::client::{DocumentLocking, OssClient, OssUploadArtifact};
use super::fake_gcs::FakeGcs;
use super::repository::OssRepository;
use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::repository::interfaces::Repository;
use crate::repository::{BlobBody, BlobKey, DocumentChange, DocumentKey, DocumentUpdate};

const BUCKET: &str = "snapshots";

fn client(fake: &FakeGcs, prefix: &str) -> OssClient {
    OssClient::new_gcs(
        BUCKET.to_owned(),
        fake.endpoint.clone(),
        prefix.to_owned(),
        GcsAuth::Anonymous,
    )
}

fn repository(fake: &FakeGcs, prefix: &str) -> OssRepository {
    OssRepository::new(
        Arc::new(client(fake, prefix)),
        SnapshotImageStoragePolicy::ObjectStorage,
        &SnapshotPublishCompressionConfig::default(),
    )
}

fn increment() -> DocumentUpdate {
    Box::new(|current| {
        let value = match current {
            Some(bytes) => std::str::from_utf8(bytes)
                .expect("utf-8 counter")
                .parse::<u64>()
                .expect("numeric counter"),
            None => 0,
        };
        Ok(DocumentChange::Put((value + 1).to_string().into_bytes()))
    })
}

#[tokio::test]
async fn objects_round_trip_under_the_configured_prefix() {
    let fake = FakeGcs::start().await;
    let client = client(&fake, "agentenv/prod");

    client
        .put_bytes(
            "catalog/a.json",
            b"alpha".to_vec(),
            OssUploadArtifact::CatalogRecord,
        )
        .await
        .expect("put a");
    client
        .put_bytes(
            "catalog/b.json",
            b"beta".to_vec(),
            OssUploadArtifact::CatalogRecord,
        )
        .await
        .expect("put b");
    client
        .put_bytes(
            "catalog/c.json",
            b"gamma".to_vec(),
            OssUploadArtifact::CatalogRecord,
        )
        .await
        .expect("put c");

    assert_eq!(
        fake.object("agentenv/prod/catalog/a.json").unwrap(),
        b"alpha"
    );
    assert_eq!(client.get_bytes("catalog/b.json").await.unwrap(), "beta");
    assert!(client.exists("catalog/c.json").await.unwrap());
    assert!(!client.exists("catalog/missing.json").await.unwrap());

    // Keys come back relative to the prefix, as the documents code expects.
    assert_eq!(
        client.list_keys_recursive("catalog/").await.unwrap(),
        ["catalog/a.json", "catalog/b.json", "catalog/c.json"]
    );
    assert_eq!(
        client
            .list_keys_page("catalog/", Some("catalog/a.json"), 2)
            .await
            .unwrap(),
        ["catalog/b.json", "catalog/c.json"]
    );

    client.delete("catalog/a.json").await.unwrap();
    client
        .delete("catalog/a.json")
        .await
        .expect("delete is idempotent");
    let missing = client.get_bytes("catalog/a.json").await.unwrap_err();
    assert!(OssClient::is_not_found_error(&missing), "{missing:#}");
}

/// Digests and other names carry characters that OpenDAL 0.55 would encode
/// twice when it writes them in one request. The store must still list,
/// read and delete what it wrote, under the names the repository uses.
#[tokio::test]
async fn names_with_reserved_characters_survive_a_write_and_read() {
    let fake = FakeGcs::start().await;
    let client = client(&fake, "env:prod");
    let key = format!("images/sha256:{}.json", "c".repeat(64));

    client
        .put_bytes(&key, b"image".to_vec(), OssUploadArtifact::CatalogRecord)
        .await
        .unwrap();

    assert_eq!(client.get_bytes(&key).await.unwrap(), "image");
    assert!(client.exists(&key).await.unwrap());
    assert_eq!(
        client.list_keys_recursive("images/").await.unwrap(),
        std::slice::from_ref(&key)
    );
    assert!(client
        .put_bytes_conditionally(&format!("{key}.lock"), b"x".to_vec(), None)
        .await
        .unwrap());
    // overlaybd receives the plain name and applies the same mapping.
    assert_eq!(
        client.object_url(&key),
        format!("gs://snapshots/env:prod/{key}")
    );

    client.delete(&key).await.unwrap();
    assert!(!client.exists(&key).await.unwrap());
}

#[tokio::test]
async fn a_create_only_write_refuses_an_existing_object() {
    let fake = FakeGcs::start().await;
    let client = client(&fake, "");

    assert!(client
        .put_bytes_conditionally("locks/x", b"first".to_vec(), None)
        .await
        .unwrap());
    assert!(!client
        .put_bytes_conditionally("locks/x", b"second".to_vec(), None)
        .await
        .unwrap());

    assert_eq!(fake.object("locks/x").unwrap(), b"first");
    // GCS says "only if there is no live version" with generation 0.
    assert_eq!(fake.requests_matching("ifGenerationMatch=0"), 2);
}

#[tokio::test]
async fn gcs_clients_lock_documents_with_create_only_leases() {
    let fake = FakeGcs::start().await;
    assert_eq!(
        client(&fake, "").document_locking(),
        DocumentLocking::CreateOnlyLease
    );
}

#[tokio::test]
async fn concurrent_document_updates_are_not_lost() {
    let fake = FakeGcs::start().await;
    let repository = Arc::new(repository(&fake, "p"));
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let repository = Arc::clone(&repository);
            tokio::spawn(async move {
                for _ in 0..3 {
                    repository
                        .update_document(DocumentKey::BuildCacheState, increment())
                        .await
                        .expect("update");
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("writer finished");
    }

    let stored = fake.object("p/template-build/cache-head.json").unwrap();
    assert_eq!(stored, b"12", "every increment must land");
    // Superseded lock epochs are swept; the last claim and its release remain.
    let locks = fake
        .object_names()
        .into_iter()
        .filter(|name| name.starts_with("p/locks/"))
        .count();
    assert!(locks <= 2, "{:?}", fake.object_names());
}

#[tokio::test]
async fn documents_can_be_put_read_and_deleted() {
    let fake = FakeGcs::start().await;
    let repository = repository(&fake, "");
    let key = || DocumentKey::VolumeAlias("vol-1".to_owned());

    repository
        .put_document(key(), b"log".to_vec())
        .await
        .unwrap();
    assert_eq!(
        repository.get_document(key()).await.unwrap().unwrap(),
        b"log"
    );

    let seen = repository
        .update_document(
            key(),
            Box::new(|current| {
                assert_eq!(current, Some(b"log".as_slice()));
                Ok(DocumentChange::Delete)
            }),
        )
        .await
        .unwrap();
    assert_eq!(seen.unwrap(), b"log");
    assert!(repository.get_document(key()).await.unwrap().is_none());

    repository
        .put_document(key(), b"again".to_vec())
        .await
        .unwrap();
    repository.delete_document(key()).await.unwrap();
    assert!(repository.get_document(key()).await.unwrap().is_none());
}

#[tokio::test]
async fn blobs_are_located_with_gs_urls_for_overlaybd() {
    let fake = FakeGcs::start().await;
    let repository = repository(&fake, "agentenv");

    assert_eq!(
        repository.managed_layers_repo_blob_url().as_deref(),
        Some("gs://snapshots/agentenv/managed-layers")
    );
    let key = BlobKey::Layer {
        role: crate::repository::LayerRole::Rootfs,
        digest: format!("sha256:{}", "a".repeat(64)),
    };
    let location = repository.locate_blob(&key).unwrap();
    assert!(
        matches!(&location, crate::repository::BlobLocation::Remote(url)
            if url.starts_with("gs://snapshots/agentenv/")),
        "{location:?}"
    );
}

#[tokio::test]
async fn a_small_file_uploads_in_one_request_and_is_not_re_uploaded() {
    let fake = FakeGcs::start().await;
    let repository = repository(&fake, "");
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("layer");
    std::fs::write(&source, vec![7_u8; 3 * 1024 * 1024]).unwrap();
    let key = BlobKey::Layer {
        role: crate::repository::LayerRole::Rootfs,
        digest: format!("sha256:{}", "b".repeat(64)),
    };

    repository
        .put_blob(&key, BlobBody::File(&source))
        .await
        .unwrap();
    let stored = fake
        .object(&gcs_object_key(&key.path()))
        .unwrap_or_else(|| panic!("layer stored: {:?}", fake.object_names()));
    assert_eq!(stored.len(), 3 * 1024 * 1024);
    assert_eq!(fake.requests_matching("uploads"), 0, "no multipart needed");

    // Layers are keyed by digest: a second put sees the object and stops.
    let before = fake.requests_matching("POST /upload/");
    repository
        .put_blob(&key, BlobBody::File(&source))
        .await
        .unwrap();
    assert_eq!(fake.requests_matching("POST /upload/"), before);
}

/// The failure that motivated a native backend: GCS's S3 interoperability API
/// answered `411 Length Required` to OpenDAL's S3 multipart. The native
/// service uses the XML multipart API with an explicit length on every part.
#[tokio::test]
async fn a_large_file_uploads_as_a_multipart_with_explicit_lengths() {
    let fake = FakeGcs::start().await;
    let operator = build_object_store_operator(
        &ObjectStoreOperatorConfig {
            service: ObjectStoreService::Gcs,
            bucket: BUCKET.to_owned(),
            endpoint: fake.endpoint.clone(),
            region: String::new(),
            addressing_style: AddressingStyle::Path,
            gcs_auth: GcsAuth::Anonymous,
            timeout: None,
            max_retries: None,
        },
        None,
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("memory-layer");
    // 12 MiB in 5 MiB parts (the service minimum): 5 + 5 + 2.
    let data: Vec<u8> = (0..12 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(&source, &data).unwrap();

    upload_file_streaming(
        &operator,
        "layers/memory",
        &source,
        5 * 1024 * 1024,
        2,
        None,
    )
    .await
    .unwrap();

    assert_eq!(fake.object("layers/memory").unwrap(), data);
    assert_eq!(
        fake.requests_matching("POST /snapshots/layers/memory?uploads"),
        1
    );
    assert_eq!(
        fake.requests_matching("PUT /snapshots/layers/memory?partNumber"),
        3
    );
    assert_eq!(
        fake.requests_matching("POST /snapshots/layers/memory?uploadId"),
        1
    );
}

#[tokio::test]
async fn ranged_reads_return_only_the_requested_bytes() {
    let fake = FakeGcs::start().await;
    let operator = build_object_store_operator(
        &ObjectStoreOperatorConfig {
            service: ObjectStoreService::Gcs,
            bucket: BUCKET.to_owned(),
            endpoint: fake.endpoint.clone(),
            region: String::new(),
            addressing_style: AddressingStyle::Path,
            gcs_auth: GcsAuth::Anonymous,
            timeout: None,
            max_retries: None,
        },
        None,
    )
    .unwrap();
    operator
        .write("layers/lower", (0..=255_u8).collect::<Vec<_>>())
        .await
        .unwrap();

    let slice = operator
        .read_with("layers/lower")
        .range(16..32)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(slice, (16..32_u8).collect::<Vec<_>>());
    assert_eq!(
        operator
            .stat("layers/lower")
            .await
            .unwrap()
            .content_length(),
        256
    );
}
