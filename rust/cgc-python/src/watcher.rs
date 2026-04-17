//! PyO3 binding for the Rust file watcher.
//!
//! Exposes `FileWatcher(path)` as a Python class. Python drives polling
//! — each `poll(timeout_ms)` returns a list of `(kind, path)` tuples.
//! Debouncing and re-indexing stay in Python's watcher.py; this just
//! replaces the notify backend.

use std::path::PathBuf;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use cgc_core::watcher::RustWatcher;

#[pyclass(module = "_cgc_rust", name = "FileWatcher")]
pub struct PyFileWatcher {
    inner: RustWatcher,
}

#[pymethods]
impl PyFileWatcher {
    /// Start watching `path` recursively. Raises RuntimeError on any
    /// backend failure (e.g. path doesn't exist, platform limits).
    #[new]
    fn new(path: PathBuf) -> PyResult<Self> {
        let inner = RustWatcher::start(&path)
            .map_err(|e| PyRuntimeError::new_err(format!("watch failed: {e}")))?;
        Ok(Self { inner })
    }

    /// Block up to `timeout_ms` for the first event, then drain the
    /// queue non-blockingly. Returns `[(kind: str, path: str), ...]`,
    /// where `kind` is one of "created", "modified", "removed",
    /// "renamed", "other". Empty list means no events arrived before
    /// the timeout.
    #[pyo3(signature = (timeout_ms = 1000))]
    fn poll(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<Vec<(String, String)>> {
        // Drop the GIL while blocking so Python stays responsive.
        let events = py.allow_threads(|| self.inner.poll(Duration::from_millis(timeout_ms)));
        Ok(events
            .into_iter()
            .map(|e| (e.kind.to_string(), e.path.to_string_lossy().into_owned()))
            .collect())
    }
}
