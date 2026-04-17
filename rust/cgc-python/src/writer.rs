//! PyO3 bindings for the Rust graph writer.
//!
//! A single shared multi-thread tokio runtime is lazily initialised on first
//! use and retained for the lifetime of the process. Each call drops the GIL
//! via `py.allow_threads` and blocks on the runtime handle while the write
//! executes. Neo4j transactions are issued from the tokio worker threads —
//! multiple Python callers land on distinct workers, so writes run in
//! parallel even though the Python caller is synchronous.

use std::collections::HashMap;

use neo4rs::{BoltMap, BoltString, BoltType};
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::runtime::{Builder, Runtime};

use cgc_core::writer::{
    ClassFnRow, FileRow, GraphWriter, ImportRow, InheritanceLinkRow, NestedFnRow, ParamRow,
    SymbolBatch, SYMBOL_LABELS,
};

use crate::pyany_bolt::py_any_to_bolt;

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

    /// Write symbol nodes (Function/Class/Trait/…) and their File CONTAINS
    /// edges for every parsed file.
    ///
    /// Only the label lists in `SYMBOL_LABELS` are processed — extra keys
    /// in the file dict are ignored.
    fn write_symbols(
        &self,
        py: Python<'_>,
        all_file_data: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let mut by_label: HashMap<&'static str, Vec<(BoltMap, String)>> = HashMap::new();

        for file in all_file_data.iter() {
            let file_dict: &Bound<'_, PyDict> = file.downcast()?;
            let file_path: String = file_dict
                .get_item("path")?
                .ok_or_else(|| PyRuntimeError::new_err("file_data missing 'path'"))?
                .extract()?;

            for (py_key, label) in SYMBOL_LABELS.iter() {
                let Some(list_val) = file_dict.get_item(py_key)? else {
                    continue;
                };
                let list: &Bound<'_, PyList> = match list_val.downcast() {
                    Ok(l) => l,
                    Err(_) => continue,
                };
                for item in list.iter() {
                    let item_dict: &Bound<'_, PyDict> = match item.downcast() {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    let mut map = BoltMap::new();
                    for (k, v) in item_dict.iter() {
                        let key: String = k.extract()?;
                        map.put(BoltString::from(key.as_str()), py_any_to_bolt(&v)?);
                    }
                    if *label == "Function" {
                        let cc = BoltString::from("cyclomatic_complexity");
                        if !map.value.contains_key(&cc) {
                            map.put(cc, BoltType::from(1i64));
                        }
                    }
                    by_label
                        .entry(label)
                        .or_default()
                        .push((map, file_path.clone()));
                }
            }
        }

        let batches: Vec<SymbolBatch> = by_label
            .into_iter()
            .map(|(label, rows)| SymbolBatch {
                label: label.to_string(),
                rows,
            })
            .collect();

        let inner = self.inner.clone();
        let rt = runtime();
        let result: std::result::Result<(), cgc_core::writer::WriterError> = py
            .allow_threads(|| rt.block_on(async move { inner.write_symbols(batches).await }));
        result.map_err(to_py_err)
    }

    /// Write HAS_PARAMETER, Class CONTAINS Function, and nested-function
    /// CONTAINS edges.
    ///
    /// Relies on Function and Class nodes already existing (i.e.
    /// write_symbols must have run). Extracts the batches from the same
    /// parser dicts used by write_symbols.
    fn write_function_edges(
        &self,
        py: Python<'_>,
        all_file_data: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let mut params: Vec<ParamRow> = Vec::new();
        let mut class_fns: Vec<ClassFnRow> = Vec::new();
        let mut nested_fns: Vec<NestedFnRow> = Vec::new();

        for file in all_file_data.iter() {
            let fd: &Bound<'_, PyDict> = file.downcast()?;
            let file_path: String = fd
                .get_item("path")?
                .ok_or_else(|| PyRuntimeError::new_err("file_data missing 'path'"))?
                .extract()?;
            let Some(fns_val) = fd.get_item("functions")? else {
                continue;
            };
            let fns_list: &Bound<'_, PyList> = match fns_val.downcast() {
                Ok(l) => l,
                Err(_) => continue,
            };

            for item in fns_list.iter() {
                let fn_dict: &Bound<'_, PyDict> = match item.downcast() {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let func_name: String = fn_dict
                    .get_item("name")?
                    .and_then(|v| v.extract().ok())
                    .unwrap_or_default();
                let line_number: i64 = fn_dict
                    .get_item("line_number")?
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);

                // HAS_PARAMETER edges from args
                if let Some(args_val) = fn_dict.get_item("args")? {
                    if let Ok(args) = args_val.downcast::<PyList>() {
                        for arg in args.iter() {
                            let arg_name: String = match arg.extract() {
                                Ok(s) => s,
                                Err(_) => continue,
                            };
                            params.push(ParamRow {
                                func_name: func_name.clone(),
                                line_number,
                                arg_name,
                                file_path: file_path.clone(),
                            });
                        }
                    }
                }

                // Class CONTAINS Function
                if let Some(cls_val) = fn_dict.get_item("class_context")? {
                    if !cls_val.is_none() {
                        if let Ok(cls_name) = cls_val.extract::<String>() {
                            if !cls_name.is_empty() {
                                class_fns.push(ClassFnRow {
                                    class_name: cls_name,
                                    func_name: func_name.clone(),
                                    func_line: line_number,
                                    file_path: file_path.clone(),
                                });
                            }
                        }
                    }
                }

                // Nested function CONTAINS
                let ctx_type: Option<String> = fn_dict
                    .get_item("context_type")?
                    .and_then(|v| v.extract().ok());
                if ctx_type.as_deref() == Some("function_definition") {
                    if let Some(outer_val) = fn_dict.get_item("context")? {
                        if let Ok(outer) = outer_val.extract::<String>() {
                            if !outer.is_empty() {
                                nested_fns.push(NestedFnRow {
                                    outer,
                                    inner_name: func_name.clone(),
                                    inner_line: line_number,
                                    file_path: file_path.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        let inner = self.inner.clone();
        let rt = runtime();
        let result: std::result::Result<(), cgc_core::writer::WriterError> = py
            .allow_threads(|| {
                rt.block_on(async move {
                    inner
                        .write_function_edges(&params, &class_fns, &nested_fns)
                        .await
                })
            });
        result.map_err(to_py_err)
    }

    /// Write `File -[:IMPORTS]-> Module` edges for every parsed file.
    fn write_imports(
        &self,
        py: Python<'_>,
        all_file_data: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let mut imports: Vec<ImportRow> = Vec::new();
        for file in all_file_data.iter() {
            let fd: &Bound<'_, PyDict> = file.downcast()?;
            let file_path: String = fd
                .get_item("path")?
                .ok_or_else(|| PyRuntimeError::new_err("file_data missing 'path'"))?
                .extract()?;
            let Some(imports_val) = fd.get_item("imports")? else {
                continue;
            };
            let imports_list: &Bound<'_, PyList> = match imports_val.downcast() {
                Ok(l) => l,
                Err(_) => continue,
            };
            for item in imports_list.iter() {
                let imp: &Bound<'_, PyDict> = match item.downcast() {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let name: String = imp
                    .get_item("name")?
                    .and_then(|v| v.extract().ok())
                    .unwrap_or_default();
                let alias: Option<String> = imp
                    .get_item("alias")?
                    .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
                let full_import_name: String = imp
                    .get_item("full_import_name")?
                    .and_then(|v| v.extract().ok())
                    .unwrap_or_else(|| name.clone());
                let line_number: i64 = imp
                    .get_item("line_number")?
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);
                imports.push(ImportRow {
                    name,
                    alias,
                    full_import_name,
                    line_number,
                    file_path: file_path.clone(),
                });
            }
        }

        let inner = self.inner.clone();
        let rt = runtime();
        let result: std::result::Result<(), cgc_core::writer::WriterError> = py
            .allow_threads(|| rt.block_on(async move { inner.write_imports(&imports).await }));
        result.map_err(to_py_err)
    }

    /// Write `Class -[:INHERITS]-> Class` edges from the resolver output.
    fn write_inheritance(
        &self,
        py: Python<'_>,
        inheritance_batch: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let mut links: Vec<InheritanceLinkRow> = Vec::with_capacity(inheritance_batch.len());
        for item in inheritance_batch.iter() {
            let d: &Bound<'_, PyDict> = item.downcast()?;
            let child_name: String = d
                .get_item("child_name")?
                .and_then(|v| v.extract().ok())
                .unwrap_or_default();
            let path: String = d
                .get_item("path")?
                .and_then(|v| v.extract().ok())
                .unwrap_or_default();
            let parent_name: String = d
                .get_item("parent_name")?
                .and_then(|v| v.extract().ok())
                .unwrap_or_default();
            let resolved_parent_file_path: String = d
                .get_item("resolved_parent_file_path")?
                .and_then(|v| v.extract().ok())
                .unwrap_or_default();
            links.push(InheritanceLinkRow {
                child_name,
                path,
                parent_name,
                resolved_parent_file_path,
            });
        }

        let inner = self.inner.clone();
        let rt = runtime();
        let result: std::result::Result<(), cgc_core::writer::WriterError> = py
            .allow_threads(|| rt.block_on(async move { inner.write_inheritance(&links).await }));
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
