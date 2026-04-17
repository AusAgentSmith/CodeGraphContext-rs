//! Conversion from `PyAny` → `neo4rs::BoltType`.
//!
//! Covers the value shapes that tree-sitter parser dicts actually contain:
//! None, bool, int, float, str, and homogeneous lists of those. Nested
//! dicts are converted recursively so arbitrary parser payloads round-trip.
//! Anything not recognisable (e.g. a custom object) becomes its `str()`
//! repr, matching the Python sanitize_props fallback.

use neo4rs::{BoltList, BoltMap, BoltString, BoltType};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

pub fn py_any_to_bolt(value: &Bound<'_, PyAny>) -> PyResult<BoltType> {
    if value.is_none() {
        return Ok(BoltType::Null(neo4rs::BoltNull));
    }
    // PyBool must be checked before PyInt (bool is a subclass of int).
    if let Ok(b) = value.downcast::<PyBool>() {
        return Ok(BoltType::from(b.is_true()));
    }
    if let Ok(i) = value.downcast::<PyInt>() {
        let n: i64 = i.extract()?;
        return Ok(BoltType::from(n));
    }
    if let Ok(f) = value.downcast::<PyFloat>() {
        let n: f64 = f.extract()?;
        return Ok(BoltType::from(n));
    }
    if let Ok(s) = value.downcast::<PyString>() {
        return Ok(BoltType::from(s.to_str()?.to_owned()));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut out = BoltList::new();
        for item in list.iter() {
            out.value.push(py_any_to_bolt(&item)?);
        }
        return Ok(BoltType::List(out));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut out = BoltMap::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            out.put(BoltString::from(key.as_str()), py_any_to_bolt(&v)?);
        }
        return Ok(BoltType::Map(out));
    }
    // Fallback: str() repr.
    let s = value.str()?.to_str()?.to_owned();
    Ok(BoltType::from(s))
}
