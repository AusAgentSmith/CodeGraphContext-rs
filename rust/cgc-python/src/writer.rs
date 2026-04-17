//! PyO3 bindings for the Rust graph writer.
//!
//! A single shared multi-thread tokio runtime is lazily initialised on first
//! use and retained for the lifetime of the process. Each call drops the GIL
//! via `py.allow_threads` and blocks on the runtime handle while the write
//! executes. Neo4j transactions are issued from the tokio worker threads —
//! multiple Python callers land on distinct workers, so writes run in
//! parallel even though the Python caller is synchronous.

use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::{Builder, Runtime};

use cgc_core::writer::{FileRow, GraphWriter};

fn runtime() -> &'static Runtime {
    static RT: OnceCell<Runtime> = OnceCell::new();
    RT.get_or_init(|| {
        Builder::new_multi_thread()
            .thread_name("cgc-writer")
            .enable_all()
            .build()
            .expect("failed to start tokio runtime for cgc writer")
    })
}

/// Map any writer error to a PyRuntimeError with a readable message.
fn to_py_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(format!("{e}"))
}

/// Handle to a Rust-side Neo4j connection pool, exposed to Python.
#[pyclass(module = "_cgc_rust", name = "GraphWriter")]
pub struct PyGraphWriter {
    inner: GraphWriter,
}

#[pymethods]
impl PyGraphWriter {
    /// Connect to Neo4j and verify the connection.
    ///
    /// `database` is optional — pass None to use the driver's default.
    #[new]
    #[pyo3(signature = (uri, user, password, database=None))]
    fn new(
        py: Python<'_>,
        uri: &str,
        user: &str,
        password: &str,
        database: Option<String>,
    ) -> PyResult<Self> {
        let rt = runtime();
        let writer = py.allow_threads(|| {
            rt.block_on(async {
                let w = GraphWriter::connect(uri, user, password, database).await?;
                w.ping().await?;
                Ok::<_, cgc_core::writer::WriterError>(w)
            })
        })
        .map_err(to_py_err)?;
        Ok(Self { inner: writer })
    }

    /// Run `RETURN 1` against the driver. Used by tests + callers who want
    /// to confirm liveness before kicking off a long indexing job.
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let rt = runtime();
        let inner = self.inner.clone();
        let result: std::result::Result<(), cgc_core::writer::WriterError> =
            py.allow_threads(|| rt.block_on(async move { inner.ping().await }));
        result.map_err(to_py_err)
    }

    /// Write File nodes, directory chain, and CONTAINS edges for a batch
    /// of parsed files.
    ///
    /// `all_file_data` is the list of per-file dicts produced by the
    /// parser (same shape the Python writer receives). Only `path` and
    /// `is_dependency` are read here — symbol writes live in separate
    /// methods.
    fn write_file_tree(
        &self,
        py: Python<'_>,
        all_file_data: &Bound<'_, PyList>,
        repo_path: &str,
    ) -> PyResult<usize> {
        // Extract into Rust-owned values under the GIL, then release.
        let mut files: Vec<FileRow> = Vec::with_capacity(all_file_data.len());
        for item in all_file_data.iter() {
            let d: &Bound<'_, PyDict> = item.downcast()?;
            let path: String = d
                .get_item("path")?
                .ok_or_else(|| PyRuntimeError::new_err("file_data missing 'path'"))?
                .extract()?;
            let is_dependency: bool = match d.get_item("is_dependency")? {
                Some(v) => v.extract().unwrap_or(false),
                None => false,
            };
            files.push(FileRow {
                path,
                is_dependency,
            });
        }
        let repo_path = repo_path.to_owned();
        let inner = self.inner.clone();
        let rt = runtime();
        let result: std::result::Result<usize, cgc_core::writer::WriterError> = py
            .allow_threads(|| {
                rt.block_on(async move { inner.write_file_tree(&files, &repo_path).await })
            });
        result.map_err(to_py_err)
    }

    /// MERGE a Repository node. Idempotent.
    fn add_repository(
        &self,
        py: Python<'_>,
        path: &str,
        name: &str,
        is_dependency: bool,
    ) -> PyResult<()> {
        let rt = runtime();
        let inner = self.inner.clone();
        let path = path.to_owned();
        let name = name.to_owned();
        let result: std::result::Result<(), cgc_core::writer::WriterError> = py
            .allow_threads(|| {
                rt.block_on(async move {
                    inner.add_repository(&path, &name, is_dependency).await
                })
            });
        result.map_err(to_py_err)
    }
}
