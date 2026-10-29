use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};

use super::artifacts::sync_dir;

use crate::repository::{
    DocumentChange, DocumentCollection, DocumentKey, DocumentUpdate, MaintenanceLock,
    RepositoryError, RepositoryResult,
};

const FILE_LOCK_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));

/// Shared documents in a POSIX repository: one file each, updated under a
/// lock file so nodes sharing the filesystem exclude each other.
pub struct PosixFsCatalogStore {
    root: PathBuf,
}

#[derive(Debug)]
struct PosixFileLockGuard {
    _file: Flock<fs::File>,
}

/// The `flock` backing a POSIX maintenance lock. It is released when this guard
/// is dropped, or by the kernel when the process dies.
pub(crate) struct PosixMaintenanceLockKeeper {
    _file: Flock<fs::File>,
}

impl PosixFsCatalogStore {
    /// Creates a catalog store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub(crate) fn get_document(&self, key: &DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        let path = self.root.join(key.posix_path());
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RepositoryError::backend(
                format!("read '{}'", path.display()),
                error,
            )),
        }
    }

    pub(crate) fn put_document(&self, key: &DocumentKey, bytes: &[u8]) -> RepositoryResult<()> {
        let _guard =
            self.acquire_catalog_lock(self.root.join(key.posix_lock_path()), "document")?;
        self.write_bytes(&self.root.join(key.posix_path()), bytes)
    }

    pub(crate) fn update_document(
        &self,
        key: &DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        let _guard =
            self.acquire_catalog_lock(self.root.join(key.posix_lock_path()), "document")?;
        let current = self.get_document(key)?;
        match update(current.as_deref())? {
            DocumentChange::Keep => {}
            DocumentChange::Put(bytes) => {
                self.write_bytes(&self.root.join(key.posix_path()), &bytes)?
            }
            DocumentChange::Delete => {
                self.remove_file_if_exists(&self.root.join(key.posix_path()))?
            }
        }
        Ok(current)
    }

    pub(crate) fn delete_document(&self, key: &DocumentKey) -> RepositoryResult<()> {
        let _guard =
            self.acquire_catalog_lock(self.root.join(key.posix_lock_path()), "document")?;
        self.remove_file_if_exists(&self.root.join(key.posix_path()))
    }

    pub(crate) fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let directory = self.root.join(collection.prefix());
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("read '{}'", directory.display()),
                    error,
                ))
            }
        };
        let mut ids = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(format!("read '{}'", directory.display()), error)
            })?;
            let is_file = entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(format!("inspect '{}'", entry.path().display()), error)
                })?
                .is_file();
            // Lock files share the directory and are not documents.
            let file_name = entry.file_name();
            let Some(id) = file_name
                .to_str()
                .and_then(|name| collection.id_from_file_name(name))
                .filter(|_| is_file)
            else {
                continue;
            };
            if after.is_none_or(|after| id > after) {
                ids.push(id.to_owned());
            }
        }
        ids.sort_unstable();
        ids.truncate(limit);
        Ok(ids)
    }

    pub(crate) fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        let mut documents = Vec::new();
        for id in self.list_documents(collection, None, usize::MAX)? {
            let key = collection
                .key(&id)
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("invalid document id '{id}'"),
                })?;
            // Removed since the listing.
            if let Some(bytes) = self.get_document(&key)? {
                documents.push((id, bytes));
            }
        }
        Ok(documents)
    }

    /// Takes one maintenance lock, or reports `None` when a live process holds
    /// it.
    ///
    /// The lock is an advisory `flock` on a file of its own, so a process that
    /// dies releases it with the kernel and no stale-lock heuristic is needed.
    /// The keeper returned alongside the lock holds the `flock`: it must live
    /// as long as the pass.
    pub(crate) fn acquire_maintenance_lock(
        &self,
        name: &str,
    ) -> RepositoryResult<Option<(MaintenanceLock, PosixMaintenanceLockKeeper)>> {
        let path = self.root.join(crate::repository::maintenance_lock_key(name));
        let Some(parent) = path.parent() else {
            return Err(RepositoryError::Backend {
                message: format!("resolve parent for maintenance lock '{}'", path.display()),
                source: None,
            });
        };
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create maintenance lock dir '{}'", parent.display()),
                error,
            )
        })?;
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                RepositoryError::backend(
                    format!("open maintenance lock '{}'", path.display()),
                    error,
                )
            })?;
        let mut file = match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(file) => file,
            // A live pass holds it; report the contention without waiting, so
            // a scheduled sweep can skip this round.
            Err((_file, error)) if error == Errno::EAGAIN || error == Errno::EWOULDBLOCK => {
                return Ok(None)
            }
            Err((_file, error)) => {
                return Err(RepositoryError::backend(
                    format!("lock maintenance lock '{}'", path.display()),
                    error,
                ))
            }
        };
        let token = uuid::Uuid::now_v7().to_string();
        file.set_len(0).map_err(|error| {
            RepositoryError::backend(
                format!("truncate maintenance lock '{}'", path.display()),
                error,
            )
        })?;
        std::io::Write::write_all(&mut *file, token.as_bytes()).map_err(|error| {
            RepositoryError::backend(
                format!("write maintenance lock '{}'", path.display()),
                error,
            )
        })?;
        Ok(Some((
            MaintenanceLock::new(name.to_owned(), token),
            PosixMaintenanceLockKeeper { _file: file },
        )))
    }

    fn write_bytes(&self, path: &Path, bytes: &[u8]) -> RepositoryResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(format!("create '{}'", parent.display()), error)
            })?;
        }
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
        })?;
        temp.write_all(bytes).map_err(|error| {
            RepositoryError::backend(
                format!("write temp json '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.as_file().sync_all().map_err(|error| {
            RepositoryError::backend(format!("sync temp json '{}'", temp.path().display()), error)
        })?;
        let tmp_path = temp.path().to_path_buf();
        temp.persist(path).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "persist json '{}' -> '{}'",
                    tmp_path.display(),
                    path.display()
                ),
                error.error,
            )
        })?;
        sync_dir(parent)
    }

    fn remove_file_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_file(path) {
            Ok(()) => path.parent().map_or(Ok(()), sync_dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn acquire_file_lock(
        &self,
        lock_path: PathBuf,
        contents: String,
        label: &'static str,
        on_locked: impl Fn() -> RepositoryResult<PosixFileLockGuard>,
    ) -> RepositoryResult<PosixFileLockGuard> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(
                    format!("create {label} lock dir '{}'", parent.display()),
                    error,
                )
            })?;
        }

        let deadline = FILE_LOCK_TIMEOUT.map(|timeout| Instant::now() + timeout);
        loop {
            let file = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("open {label} lock '{}'", lock_path.display()),
                        error,
                    )
                })?;
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(mut file) => {
                    file.set_len(0).map_err(|error| {
                        RepositoryError::backend(
                            format!("truncate {label} lock '{}'", lock_path.display()),
                            error,
                        )
                    })?;
                    file.write_all(contents.as_bytes()).map_err(|error| {
                        RepositoryError::backend(
                            format!("write {label} lock '{}'", lock_path.display()),
                            error,
                        )
                    })?;
                    return Ok(PosixFileLockGuard { _file: file });
                }
                Err((_file, error)) if error == Errno::EAGAIN || error == Errno::EWOULDBLOCK => {
                    if let Some(deadline) = deadline {
                        if Instant::now() < deadline {
                            thread::sleep(Duration::from_millis(25));
                            continue;
                        }
                    }
                    return on_locked();
                }
                Err((_file, error)) => {
                    return Err(RepositoryError::backend(
                        format!("lock {label} lock '{}'", lock_path.display()),
                        error,
                    ));
                }
            }
        }
    }

    fn acquire_catalog_lock(
        &self,
        lock_path: PathBuf,
        label: &'static str,
    ) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            lock_path.clone(),
            std::process::id().to_string(),
            label,
            || {
                Err(RepositoryError::Backend {
                    message: format!(
                        "timed out waiting for {label} lock '{}'",
                        lock_path.display()
                    ),
                    source: None,
                })
            },
        )
    }
}
