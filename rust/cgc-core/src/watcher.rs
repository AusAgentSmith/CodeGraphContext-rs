//! Native file watcher on top of the `notify` crate.
//!
//! Provides a queue-backed, polling-style API suitable for calling from
//! Python: spawn the watcher, register paths, call `poll_events(timeout)`
//! to drain what's arrived. Python owns debouncing + re-indexing
//! orchestration; this crate only delivers raw events.
//!
//! `notify` uses the best platform-specific backend available
//! (inotify / FSEvents / ReadDirectoryChangesW). When no backend fits
//! it falls back to polling internally — that's a user-visible
//! performance difference but behaviour is the same.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use notify::{
    event::{EventKind, ModifyKind, RemoveKind},
    Config, RecommendedWatcher, RecursiveMode, Watcher,
};

/// The watcher's view of a file system event — a simplified projection
/// of `notify::Event` that's easy to cross FFI.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    /// One of: "created", "modified", "removed", "renamed", "other".
    /// `rename` collapses both the from/to halves; the Python side sees
    /// one event per affected path.
    pub kind: &'static str,
    pub path: PathBuf,
}

/// Owning handle that keeps the notify watcher alive. Dropping this
/// stops event delivery. Cheap to construct, one per Python caller.
pub struct RustWatcher {
    // Keep the Watcher alive; dropping it unregisters the paths and
    // closes the channel.
    _watcher: RecommendedWatcher,
    rx: Receiver<WatchEvent>,
}

impl RustWatcher {
    /// Start watching `path` recursively and return a handle that drains
    /// events. Errors surface as `notify::Error`.
    pub fn start(path: &Path) -> Result<Self, notify::Error> {
        let (tx, rx): (Sender<WatchEvent>, Receiver<WatchEvent>) =
            crossbeam_channel::unbounded();
        let tx = Arc::new(tx);

        let event_handler = {
            let tx = tx.clone();
            move |res: notify::Result<notify::Event>| {
                let Ok(event) = res else { return };
                let kind = classify_kind(&event.kind);
                for p in event.paths {
                    let _ = tx.send(WatchEvent {
                        kind,
                        path: p.clone(),
                    });
                }
            }
        };

        let mut watcher = RecommendedWatcher::new(event_handler, Config::default())?;
        watcher.watch(path, RecursiveMode::Recursive)?;
        Ok(Self {
            _watcher: watcher,
            rx,
        })
    }

    /// Drain any events the watcher has buffered, blocking up to
    /// `timeout` for the first event. Returns an empty vec on timeout.
    pub fn poll(&self, timeout: Duration) -> Vec<WatchEvent> {
        let mut out = Vec::new();
        match self.rx.recv_timeout(timeout) {
            Ok(ev) => out.push(ev),
            Err(_) => return out,
        }
        // Drain anything else already queued without further blocking.
        while let Ok(ev) = self.rx.try_recv() {
            out.push(ev);
        }
        out
    }
}

fn classify_kind(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::Create(_) => "created",
        EventKind::Modify(ModifyKind::Name(_)) => "renamed",
        EventKind::Modify(_) => "modified",
        EventKind::Remove(RemoveKind::File) | EventKind::Remove(RemoveKind::Folder) => "removed",
        EventKind::Remove(_) => "removed",
        _ => "other",
    }
}
