//! The queries concern: derived state + accessors - the active context and
//! history snapshot, the branch scans, the window-backed reads, and the getters.

use super::{FileEntry, Path, SessionHeader, SessionManager, SessionTree};

impl SessionManager {
    /// The active compacted context without hydrating old message bodies: an
    /// attached window contributes only its settings overlay.
    #[must_use]
    pub fn active_context(&self) -> super::SessionContext {
        let mut context = super::build_session_context(&self.file_entries, self.get_leaf_id());
        if let Some(window) = &self.window {
            if !window.full_history() {
                let settings = window.settings();
                context.thinking_level.clone_from(&settings.thinking_level);
                context.service_tier = settings.service_tier;
                context.model.clone_from(&settings.model);
            }
        }
        context
    }
    /// Capture a historical read request while locked; await it after
    /// releasing the session mutex. A full-history window under the sole
    /// lease serves the retained rows directly.
    ///
    /// # Errors
    ///
    /// The future errors when the file read or blocking join fails; the
    /// retained arms never touch the disk.
    pub fn history_snapshot(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<FileEntry>>> + Send + 'static {
        let path = self
            .window
            .as_ref()
            .map(|window| window.source_path().to_owned());
        let retained = self.file_entries.clone();
        // Full-history fast path: the retained copy is exactly the file's rows
        // plus the unpersisted tail; without the lease the gate stays closed.
        let fast_path = self
            .window
            .as_ref()
            .is_some_and(super::window::WindowedSessionStore::retained_whole_file)
            && self.append_ownership == super::window::AppendOwnership::SessionLeaseHeld;
        async move {
            if fast_path {
                return Ok(retained);
            }
            let Some(path) = path else {
                return Ok(retained);
            };
            let mut entries = tokio::task::spawn_blocking(move || {
                std::fs::read_to_string(path).map(|text| super::parse_session_entries(&text))
            })
            .await??;
            let ids: std::collections::HashSet<String> = entries
                .iter()
                .filter_map(|entry| entry.id().map(str::to_owned))
                .collect();
            entries.extend(
                retained
                    .into_iter()
                    .filter(|entry| entry.id().is_some_and(|id| !ids.contains(id))),
            );
            Ok(entries)
        }
    }


    #[cfg(test)]
    #[must_use]
    pub fn is_full_history(&self) -> bool {
        self.window.is_none()
    }

    /// Hydrate before historical reads or mutation. Loading uses a blocking
    /// worker; the selected leaf is retained and disk appends are preserved.
    ///
    /// # Errors
    ///
    /// Error when the window's full-history hydration fails; a windowless
    /// manager is already hydrated.
    pub async fn ensure_full_history(&mut self) -> anyhow::Result<()> {
        let Some(window) = self.window.as_mut() else {
            return Ok(());
        };
        window.ensure_full_history().await?;
        let mut entries = window.entries().to_vec();
        let loaded_ids: std::collections::HashSet<String> = entries
            .iter()
            .filter_map(|entry| entry.id().map(str::to_owned))
            .collect();
        entries.extend(
            self.file_entries
                .iter()
                .filter(|entry| entry.id().is_some_and(|id| !loaded_ids.contains(id)))
                .cloned(),
        );
        let leaf = self.leaf_id.clone();
        self.refresh_has_assistant_entry(&entries);
        self.file_entries = entries;
        self.build_index();
        self.leaf_id = leaf;
        self.window = None;
        Ok(())
    }
    /// Session artifact directory (`dirname(sessionDir)/session-artifacts/<id>`);
    /// only persisted sessions have one.
    #[must_use]
    pub fn get_session_artifact_dir(&self) -> Option<std::path::PathBuf> {
        self.persist
            .then(|| {
                self.session_dir
                    .parent()
                    .map(|root| root.join("session-artifacts"))
            })
            .flatten()
            .map(|root| root.join(&self.session_id))
    }

    #[must_use]
    pub fn get_cwd(&self) -> &Path {
        &self.cwd
    }

    #[must_use]
    pub fn get_session_dir(&self) -> &Path {
        &self.session_dir
    }

    #[must_use]
    pub fn get_session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn get_session_file(&self) -> Option<&Path> {
        self.session_file.as_deref()
    }

    /// Entries excluding the session header.
    ///
    /// # Panics
    ///
    /// Asserts that the manager holds no windowed store: hydrate the full session history first.
    #[must_use]
    pub fn get_entries(&self) -> Vec<FileEntry> {
        assert!(
            self.window.is_none(),
            "hydrate full session history before this operation"
        );
        self.file_entries
            .iter()
            .filter(|entry| !matches!(entry, FileEntry::Header { .. }))
            .cloned()
            .collect()
    }

    /// All entries including the header (whole-file views).
    ///
    /// # Panics
    ///
    /// Asserts that the manager holds no windowed store: hydrate the full session history first.
    #[must_use]
    pub fn get_all_entries(&self) -> &[FileEntry] {
        assert!(
            self.window.is_none(),
            "hydrate full session history before this operation"
        );
        &self.file_entries
    }

    #[must_use]
    pub fn get_leaf_id(&self) -> Option<&str> {
        self.leaf_id.as_deref()
    }

    #[must_use]
    pub fn get_header(&self) -> Option<&SessionHeader> {
        self.file_entries.iter().find_map(|entry| match entry {
            FileEntry::Header { header } => Some(header),
            _ => None,
        })
    }

    #[must_use]
    pub fn get_session_name(&self) -> Option<String> {
        if let Some(window) = &self.window {
            return self
                .file_entries
                .iter()
                .rev()
                .find_map(|entry| match entry {
                    FileEntry::SessionInfo { payload, .. } => Some(payload.name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| {
                    window
                        .metadata_entries()
                        .iter()
                        .rev()
                        .find_map(|raw| match serde_json::from_str::<FileEntry>(raw).ok()? {
                            FileEntry::SessionInfo { payload, .. } => Some(payload.name),
                            _ => None,
                        })
                        .flatten()
                });
        }
        self.file_entries
            .iter()
            .rev()
            .find_map(|entry| match entry {
                FileEntry::SessionInfo { payload, .. } => Some(payload.name.clone()),
                _ => None,
            })
            .flatten()
    }
    /// The session tree (branch children + label state) over current entries.
    ///
    /// # Panics
    ///
    /// Asserts that the manager holds no windowed store: hydrate the full session history first.
    #[must_use]
    pub fn get_tree(&self) -> SessionTree {
        assert!(
            self.window.is_none(),
            "hydrate full session history before this operation"
        );
        SessionTree::build(&self.file_entries)
    }
    /// Look up an entry by id (file position index).
    ///
    /// # Panics
    ///
    /// Asserts that the manager holds no windowed store: hydrate the full session history first.
    #[must_use]
    pub fn get_entry_by_id(&self, id: &str) -> Option<&FileEntry> {
        assert!(
            self.window.is_none(),
            "hydrate full session history before this operation"
        );
        self.by_id.get(id).map(|&index| &self.file_entries[index])
    }

    /// The active label for a target entry id.
    ///
    /// # Panics
    ///
    /// Asserts that the manager holds no windowed store: hydrate the full session history first.
    #[must_use]
    pub fn get_label(&self, target_id: &str) -> Option<String> {
        assert!(
            self.window.is_none(),
            "hydrate full session history before this operation"
        );
        self.labels_by_id.get(target_id).cloned()
    }

    /// The timestamp of the label entry that set the target's active label.
    ///
    /// # Panics
    ///
    /// Asserts that the manager holds no windowed store: hydrate the full session history first.
    #[must_use]
    pub fn get_label_timestamp(&self, target_id: &str) -> Option<String> {
        assert!(
            self.window.is_none(),
            "hydrate full session history before this operation"
        );
        self.label_timestamps_by_id.get(target_id).cloned()
    }
}
