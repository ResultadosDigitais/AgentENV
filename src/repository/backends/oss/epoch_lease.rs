//! Leases that let one node at a time write a shared document on Google Cloud
//! Storage, which cannot replace or delete an object only if it is unchanged.
//!
//! What it can do is create an object only if none exists
//! (`ifGenerationMatch=0`), and of two such creations of one key exactly one
//! succeeds. A document's lock is therefore a series of numbered, immutable
//! objects under `locks/<document>/`: epoch `n` claims the lock for one
//! attempt, and the highest epoch is the current state. A free lock is
//! claimed by creating the next epoch; releasing creates the next epoch too,
//! marked released. Nothing is ever overwritten, so there is no race to lose
//! between reading a holder and replacing it.
//!
//! A holder that dies never releases, so a contender that has seen the same
//! highest epoch for [`LEASE`] takes the lock over by creating the next one.
//! Nothing can stop a holder that is merely slow, so holders bound themselves
//! exactly as in the append-based [`super::lease`]: every write starts before
//! [`Lease::deadline`], [`MARGIN`] before any contender may take over, and ends
//! within it. Each node measures time on its own monotonic clock.
//!
//! Superseded epochs are deleted by the node that claims a newer one. The
//! highest epoch is never deleted, so a contender that listed long ago and
//! creates a long-deleted epoch finds a higher one on its confirming listing
//! and gives up.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::{debug, warn};
use uuid::Uuid;

use super::client::OssClient;
use super::documents::Backoff;
use super::lease::{ACQUIRE_TIMEOUT, LEASE, MARGIN};
use crate::repository::{RepositoryError, RepositoryResult};

const LOCKS_PREFIX: &str = "locks/";
/// Epochs are zero-padded so that key order is epoch order.
const EPOCH_WIDTH: usize = 20;

/// The lock object operations a lease needs.
#[async_trait]
pub(super) trait EpochStore: Send + Sync {
    /// Keys under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// The object at `key`; `None` when absent.
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// Creates the object unless it exists. Returns whether this call did.
    async fn create(&self, key: &str, data: Vec<u8>) -> Result<bool>;
    /// Deletes the object; absent is fine.
    async fn delete(&self, key: &str) -> Result<()>;
}

#[async_trait]
impl EpochStore for OssClient {
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_keys_recursive(prefix).await
    }

    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.get_bytes(key).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(error) if Self::is_not_found_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn create(&self, key: &str, data: Vec<u8>) -> Result<bool> {
        self.put_bytes_conditionally(key, data, None).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        OssClient::delete(self, key).await
    }
}

/// The content of one epoch object.
#[derive(Debug, Serialize, Deserialize)]
struct Claim {
    /// The attempt that created the epoch.
    token: String,
    /// Whether the epoch gives the lock up rather than claims it.
    #[serde(default)]
    released: bool,
}

/// A held lock. Dropping it without [`release`] leaves it to run out.
#[derive(Debug)]
pub(super) struct Lease {
    prefix: String,
    epoch: u64,
    token: String,
    deadline: Instant,
}

impl Lease {
    /// `started` is when the create that took the lock was sent.
    fn new(prefix: String, epoch: u64, token: String, started: Instant) -> Self {
        Self {
            prefix,
            epoch,
            token,
            deadline: started + LEASE - MARGIN,
        }
    }

    /// Writes made under this lease must start before this.
    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }
}

fn lock_prefix(document: &str) -> String {
    format!("{LOCKS_PREFIX}{document}/")
}

fn epoch_key(prefix: &str, epoch: u64) -> String {
    format!("{prefix}{epoch:0width$}", width = EPOCH_WIDTH)
}

fn epoch_of(prefix: &str, key: &str) -> Option<u64> {
    key.strip_prefix(prefix)?.parse().ok()
}

/// The highest epoch among `keys`, if any.
fn highest_epoch(prefix: &str, keys: &[String]) -> Option<u64> {
    keys.iter().filter_map(|key| epoch_of(prefix, key)).max()
}

fn claim_bytes(token: &str, released: bool) -> Vec<u8> {
    serde_json::to_vec(&Claim {
        token: token.to_owned(),
        released,
    })
    .expect("lock claims serialize")
}

/// Takes the lock of the document at `document`, giving up after
/// [`ACQUIRE_TIMEOUT`]. A claim that lands after that runs out.
pub(super) async fn acquire<S: EpochStore + ?Sized>(
    store: &S,
    document: &str,
) -> RepositoryResult<Lease> {
    let prefix = lock_prefix(document);
    match tokio::time::timeout(ACQUIRE_TIMEOUT, take(store, &prefix)).await {
        Ok(taken) => taken,
        Err(_) => Err(RepositoryError::InvalidRequest {
            reason: format!("'{prefix}' stayed locked for {ACQUIRE_TIMEOUT:?}"),
        }),
    }
}

/// What the highest epoch shows.
enum Observed {
    /// No epoch, or one that released the lock: the next epoch may claim it.
    Free {
        next: u64,
    },
    Held {
        epoch: u64,
        token: String,
    },
}

async fn observe<S: EpochStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> RepositoryResult<Option<Observed>> {
    let backend = |what: &str, error: anyhow::Error| {
        RepositoryError::backend(format!("{what} '{prefix}'"), error)
    };
    let keys = store.list(prefix).await.map_err(|e| backend("list", e))?;
    let Some(epoch) = highest_epoch(prefix, &keys) else {
        return Ok(Some(Observed::Free { next: 1 }));
    };
    let key = epoch_key(prefix, epoch);
    let Some(object) = store.read(&key).await.map_err(|e| backend("read", e))? else {
        // Only superseded epochs are deleted, so the epoch listed as highest
        // was superseded since: list again.
        return Ok(None);
    };
    Ok(Some(match serde_json::from_slice::<Claim>(&object) {
        Ok(claim) if claim.released => Observed::Free { next: epoch + 1 },
        Ok(claim) => Observed::Held {
            epoch,
            token: claim.token,
        },
        // Unreadable means somebody else's, and no one can tell it is free.
        Err(_) => Observed::Held {
            epoch,
            token: String::new(),
        },
    }))
}

async fn take<S: EpochStore + ?Sized>(store: &S, prefix: &str) -> RepositoryResult<Lease> {
    let mut backoff = Backoff::new();
    // Every create is a separate attempt, so a holder knows which of its
    // creates took the lock, and when it was sent. A create whose response was
    // lost may have landed, which the highest epoch then shows.
    let mut attempts: Vec<(String, Instant)> = Vec::new();
    // The epoch this node has seen held without a break, and since when.
    let mut waiting_on: Option<(u64, Instant)> = None;
    loop {
        let Some(observed) = observe(store, prefix).await? else {
            continue;
        };
        let next = match observed {
            Observed::Free { next } => next,
            Observed::Held { epoch, token } => {
                if let Some((_, sent)) = attempts.iter().find(|(own, _)| *own == token) {
                    if let Some(lease) = confirmed(store, prefix, epoch, token, *sent).await? {
                        return Ok(lease);
                    }
                    continue;
                }
                let since = match &waiting_on {
                    Some((seen, since)) if *seen == epoch => *since,
                    _ => waiting_on.insert((epoch, Instant::now())).1,
                };
                if since.elapsed() < LEASE {
                    backoff.wait().await;
                    continue;
                }
                warn!(prefix, epoch, "taking over a lock its holder kept too long");
                epoch + 1
            }
        };
        let token = Uuid::now_v7().to_string();
        let sent = Instant::now();
        attempts.push((token.clone(), sent));
        let created = store
            .create(&epoch_key(prefix, next), claim_bytes(&token, false))
            .await
            .map_err(|error| RepositoryError::backend(format!("claim '{prefix}'"), error))?;
        if created {
            if let Some(lease) = confirmed(store, prefix, next, token, sent).await? {
                return Ok(lease);
            }
        }
        backoff.wait().await;
    }
}

/// Confirms that `epoch`, created by `token`, is still the highest, then
/// sweeps the epochs it supersedes. A claim created below the highest epoch
/// (from a listing that went stale) is not the lock, which is `None` here.
async fn confirmed<S: EpochStore + ?Sized>(
    store: &S,
    prefix: &str,
    epoch: u64,
    token: String,
    sent: Instant,
) -> RepositoryResult<Option<Lease>> {
    let keys = store
        .list(prefix)
        .await
        .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
    if highest_epoch(prefix, &keys) != Some(epoch) {
        return Ok(None);
    }
    for key in keys {
        if epoch_of(prefix, &key).is_some_and(|older| older < epoch) {
            if let Err(error) = store.delete(&key).await {
                debug!(key, %error, "could not delete a superseded lock epoch");
            }
        }
    }
    Ok(Some(Lease::new(prefix.to_owned(), epoch, token, sent)))
}

/// Gives the lock up, if it is still held. A lock that ran out and was taken
/// over is the new holder's, which this leaves alone: the epoch it would
/// create already exists.
pub(super) async fn release<S: EpochStore + ?Sized>(store: &S, lease: Lease) {
    let key = epoch_key(&lease.prefix, lease.epoch + 1);
    if let Err(error) = store.create(&key, claim_bytes(&lease.token, true)).await {
        // The lease runs out on its own.
        warn!(key, %error, "could not release a document lock");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A store whose creates are atomic, as GCS's are.
    #[derive(Default)]
    struct MemoryStore {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl EpochStore for MemoryStore {
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            tokio::task::yield_now().await;
            Ok(self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
            tokio::task::yield_now().await;
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn create(&self, key: &str, data: Vec<u8>) -> Result<bool> {
            tokio::task::yield_now().await;
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(key) {
                return Ok(false);
            }
            objects.insert(key.to_owned(), data);
            Ok(true)
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
    }

    impl MemoryStore {
        fn keys(&self) -> Vec<String> {
            self.objects.lock().unwrap().keys().cloned().collect()
        }
    }

    #[test]
    fn epoch_keys_sort_in_epoch_order() {
        let prefix = lock_prefix("catalog/a.json");
        assert_eq!(prefix, "locks/catalog/a.json/");
        assert!(epoch_key(&prefix, 9) < epoch_key(&prefix, 10));
        assert_eq!(epoch_of(&prefix, &epoch_key(&prefix, 42)), Some(42));
        assert_eq!(epoch_of(&prefix, "locks/other/00000000000000000001"), None);
    }

    #[tokio::test]
    async fn a_free_lock_is_taken_and_released_for_the_next_holder() {
        let store = MemoryStore::default();
        let first = acquire(&store, "doc.json").await.expect("first acquire");
        assert_eq!(first.epoch, 1);
        release(&store, first).await;

        let second = acquire(&store, "doc.json").await.expect("second acquire");
        // Released at epoch 2, claimed at epoch 3; the superseded epochs go.
        assert_eq!(second.epoch, 3);
        assert_eq!(store.keys().len(), 1, "{:?}", store.keys());
        assert!(store.keys()[0].ends_with("00000000000000000003"));
    }

    #[tokio::test]
    async fn contenders_exclude_each_other() {
        let store = Arc::new(MemoryStore::default());
        let inside = Arc::new(Mutex::new(0_u32));
        let max_inside = Arc::new(Mutex::new(0_u32));
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let (store, inside, max_inside) =
                    (store.clone(), inside.clone(), max_inside.clone());
                tokio::spawn(async move {
                    for _ in 0..5 {
                        let lease = acquire(&*store, "doc.json").await.expect("acquire");
                        {
                            let mut now = inside.lock().unwrap();
                            *now += 1;
                            let mut max = max_inside.lock().unwrap();
                            *max = (*max).max(*now);
                        }
                        tokio::task::yield_now().await;
                        *inside.lock().unwrap() -= 1;
                        release(&*store, lease).await;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.expect("contender finished");
        }
        assert_eq!(*max_inside.lock().unwrap(), 1);
        // The last claim and its release, at most.
        assert!(store.keys().len() <= 2, "{:?}", store.keys());
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_never_releases_is_taken_over_after_the_lease() {
        let store = MemoryStore::default();
        let dead = acquire(&store, "doc.json").await.expect("dead holder");
        let started = Instant::now();
        assert_eq!(dead.epoch, 1);

        let taken = acquire(&store, "doc.json").await.expect("takeover");
        assert_eq!(taken.epoch, 2);
        assert!(started.elapsed() >= LEASE);
    }

    #[tokio::test]
    async fn a_stale_release_does_not_free_a_lock_taken_over_since() {
        let store = MemoryStore::default();
        let slow = acquire(&store, "doc.json").await.expect("slow holder");
        // A contender took over after the slow holder's lease ran out.
        let prefix = lock_prefix("doc.json");
        store
            .create(&epoch_key(&prefix, 2), claim_bytes("other", false))
            .await
            .unwrap();

        release(&store, slow).await;

        let observed = observe(&store, &prefix).await.unwrap().unwrap();
        assert!(matches!(observed, Observed::Held { epoch: 2, ref token } if token == "other"));
    }

    #[tokio::test]
    async fn a_claim_made_from_a_stale_listing_does_not_count() {
        let store = MemoryStore::default();
        let prefix = lock_prefix("doc.json");
        // Epochs 1 and 2 were swept away by the holder of epoch 5.
        store
            .create(&epoch_key(&prefix, 5), claim_bytes("holder", false))
            .await
            .unwrap();
        // A contender that listed when epoch 0 was highest creates epoch 1.
        assert!(store
            .create(&epoch_key(&prefix, 1), claim_bytes("late", false))
            .await
            .unwrap());

        let result = confirmed(&store, &prefix, 1, "late".to_owned(), Instant::now()).await;
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_own_claim_whose_response_was_lost_is_recognized() {
        let store = MemoryStore::default();
        let prefix = lock_prefix("doc.json");
        let attempts = [("mine".to_owned(), Instant::now())];
        store
            .create(&epoch_key(&prefix, 1), claim_bytes("mine", false))
            .await
            .unwrap();

        let Some(Observed::Held { epoch, token }) = observe(&store, &prefix).await.unwrap() else {
            panic!("epoch 1 holds the lock");
        };
        assert_eq!(epoch, 1);
        assert!(attempts.iter().any(|(own, _)| *own == token));
    }
}
