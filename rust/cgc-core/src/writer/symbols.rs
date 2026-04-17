//! Symbol writes: one MERGE per parsed Function/Class/Trait/… plus its
//! File->CONTAINS->Symbol edge.
//!
//! Two concerns this module handles that the File-tree writer does not:
//!
//! 1. **Dynamic labels.** Cypher can't parameterise labels, so we issue one
//!    query template per label. The label set is the fixed whitelist in
//!    mod.rs — new symbol kinds must be added there.
//!
//! 2. **Per-batch property type coercion.** Neo4j tolerates heterogeneous
//!    types per property, but parser output mixes None / "" / absent for
//!    the same key across rows; without coercion, `SET n += row` will
//!    unset properties when the key is missing and conflict with earlier
//!    writes when types change. We pick the dominant non-null type for
//!    each key in each batch and coerce every row to match.

use std::collections::{HashMap, HashSet};

use neo4rs::{BoltList, BoltMap, BoltNull, BoltString, BoltType};

use super::{validate_label, GraphWriter, Result, DEFAULT_BATCH_SIZE};

/// Labels this writer knows how to emit, in priority order. The string
/// keys match the parser's per-file dict keys.
pub const SYMBOL_LABELS: &[(&str, &str)] = &[
    ("functions", "Function"),
    ("classes", "Class"),
    ("traits", "Trait"),
    ("variables", "Variable"),
    ("interfaces", "Interface"),
    ("macros", "Macro"),
    ("structs", "Struct"),
    ("enums", "Enum"),
    ("unions", "Union"),
    ("records", "Record"),
    ("properties", "Property"),
];

/// Rows for a single label's MERGE batch. Each entry is (row, file_path).
/// `file_path` travels outside the map so we can dedupe on it.
pub struct SymbolBatch {
    pub label: String,
    pub rows: Vec<(BoltMap, String)>,
}

/// The dominant type of a property across a batch, used for coercion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum DominantType {
    List,
    Integer,
    Boolean,
    String,
}

impl DominantType {
    fn of(value: &BoltType) -> Option<Self> {
        match value {
            BoltType::Null(_) => None,
            BoltType::List(_) => Some(Self::List),
            BoltType::Integer(_) => Some(Self::Integer),
            BoltType::Boolean(_) => Some(Self::Boolean),
            BoltType::String(_) => Some(Self::String),
            BoltType::Float(_) => Some(Self::Integer), // coalesce with int
            _ => Some(Self::String),
        }
    }
}

impl GraphWriter {
    /// Write all symbol batches (MERGE per label + File CONTAINS).
    ///
    /// Two-phase per label so the CONTAINS MATCH can't race the node
    /// MERGE: first fan out N parallel MERGE batches, await them all,
    /// then fan out N parallel CONTAINS batches. Within each phase
    /// chunks don't conflict (different nodes). Across phases we need
    /// the barrier because a CONTAINS chunk could target nodes a MERGE
    /// chunk in the same phase is still writing.
    pub async fn write_symbols(&self, batches: Vec<SymbolBatch>) -> Result<()> {
        for batch in batches {
            if batch.rows.is_empty() {
                continue;
            }
            let label = validate_label(&batch.label)?.to_string();
            let normalized = normalize_batch(batch.rows);

            let merge_q = format!(
                "UNWIND $batch AS row \
                 MERGE (n:{label} {{name: row.name, path: row._fp, line_number: row.line_number}}) \
                 SET n += row"
            );
            let contains_q = format!(
                "UNWIND $batch AS row \
                 MATCH (f:File {{path: row._fp}}) \
                 MATCH (n:{label} {{name: row.name, path: row._fp, line_number: row.line_number}}) \
                 MERGE (f)-[:CONTAINS]->(n)"
            );

            let payload: Vec<BoltType> = normalized.into_iter().map(BoltType::Map).collect();
            self.run_parallel_chunks(&merge_q, payload.clone(), DEFAULT_BATCH_SIZE)
                .await?;
            self.run_parallel_chunks(&contains_q, payload, DEFAULT_BATCH_SIZE)
                .await?;
        }
        Ok(())
    }
}

/// Apply per-key dominant-type coercion and dedupe by (name, file_path,
/// line_number). Returns the final list of BoltMaps that include `_fp`
/// for the merge key match.
fn normalize_batch(rows: Vec<(BoltMap, String)>) -> Vec<BoltMap> {
    if rows.is_empty() {
        return Vec::new();
    }

    // 1. Collect all keys across the batch (excluding the `_file_path`
    //    tag which we carry separately).
    let mut all_keys: HashSet<String> = HashSet::new();
    for (m, _) in rows.iter() {
        for k in m.value.keys() {
            let k: &str = k.value.as_str();
            if k != "_file_path" {
                all_keys.insert(k.to_string());
            }
        }
    }

    // 2. For each key, count non-null occurrences by type and pick the
    //    dominant one.
    let mut dominant: HashMap<String, DominantType> = HashMap::new();
    for key in &all_keys {
        let bolt_key = BoltString::from(key.as_str());
        let mut counts: HashMap<DominantType, usize> = HashMap::new();
        for (m, _) in rows.iter() {
            if let Some(v) = m.value.get(&bolt_key) {
                if let Some(t) = DominantType::of(v) {
                    *counts.entry(t).or_insert(0) += 1;
                }
            }
        }
        let chosen = counts
            .into_iter()
            .max_by_key(|(_, c)| *c)
            .map(|(t, _)| t)
            .unwrap_or(DominantType::String);
        dominant.insert(key.clone(), chosen);
    }

    // 3. Coerce each row + attach _fp (the MERGE path key).
    let mut normalized: Vec<BoltMap> = Vec::with_capacity(rows.len());
    for (mut row, file_path) in rows {
        row.value
            .remove(&BoltString::from("_file_path"));
        for key in &all_keys {
            let bolt_key = BoltString::from(key.as_str());
            let target = dominant[key];
            let current = row.value.remove(&bolt_key);
            let coerced = coerce(current, target);
            row.value.insert(bolt_key, coerced);
        }
        row.value
            .insert(BoltString::from("_fp"), BoltType::from(file_path));
        normalized.push(row);
    }

    // 4. Dedupe on (name, _fp, line_number).
    let mut seen: HashSet<(String, String, i64)> = HashSet::new();
    let mut out: Vec<BoltMap> = Vec::with_capacity(normalized.len());
    for row in normalized {
        let name = extract_string(&row, "name").unwrap_or_default();
        let fp = extract_string(&row, "_fp").unwrap_or_default();
        let line = extract_int(&row, "line_number").unwrap_or(0);
        if seen.insert((name, fp, line)) {
            out.push(row);
        }
    }
    out
}

fn coerce(value: Option<BoltType>, target: DominantType) -> BoltType {
    match target {
        DominantType::List => match value {
            Some(BoltType::List(l)) if !l.value.is_empty() => BoltType::List(l),
            Some(BoltType::String(s)) if !s.value.is_empty() => {
                // Attempt to parse a JSON-encoded list, else wrap.
                if let Ok(parsed) = serde_json::from_str::<Vec<serde_json::Value>>(&s.value) {
                    let mut bl = BoltList::new();
                    for v in parsed {
                        bl.value.push(json_to_bolt_string(v));
                    }
                    if bl.value.is_empty() {
                        bl.value.push(BoltType::from(String::new()));
                    }
                    BoltType::List(bl)
                } else {
                    let mut bl = BoltList::new();
                    bl.value.push(BoltType::String(s));
                    BoltType::List(bl)
                }
            }
            _ => {
                let mut bl = BoltList::new();
                bl.value.push(BoltType::from(String::new()));
                BoltType::List(bl)
            }
        },
        DominantType::Integer => match value {
            Some(BoltType::Integer(i)) => BoltType::Integer(i),
            Some(BoltType::Float(f)) => BoltType::from(f.value as i64),
            Some(BoltType::String(s)) => BoltType::from(s.value.parse::<i64>().unwrap_or(0)),
            Some(BoltType::Boolean(b)) => BoltType::from(if b.value { 1i64 } else { 0 }),
            _ => BoltType::from(0i64),
        },
        DominantType::Boolean => match value {
            Some(BoltType::Boolean(b)) => BoltType::Boolean(b),
            Some(BoltType::Null(_)) | None => BoltType::from(false),
            Some(BoltType::Integer(i)) => BoltType::from(i.value != 0),
            Some(BoltType::String(s)) => BoltType::from(!s.value.is_empty()),
            _ => BoltType::from(true),
        },
        DominantType::String => match value {
            Some(BoltType::String(s)) => BoltType::String(s),
            Some(BoltType::Null(_)) | None => BoltType::from(String::new()),
            Some(BoltType::List(l)) => {
                // Serialise lists to JSON text when the dominant type is string.
                let json = serde_json::to_string(&bolt_list_to_json(l))
                    .unwrap_or_else(|_| "[]".to_string());
                BoltType::from(json)
            }
            Some(v) => BoltType::from(format!("{v:?}")),
        },
    }
}

fn json_to_bolt_string(v: serde_json::Value) -> BoltType {
    match v {
        serde_json::Value::String(s) => BoltType::from(s),
        other => BoltType::from(other.to_string()),
    }
}

fn bolt_list_to_json(list: BoltList) -> serde_json::Value {
    let items: Vec<serde_json::Value> = list
        .value
        .into_iter()
        .map(bolt_to_json)
        .collect();
    serde_json::Value::Array(items)
}

fn bolt_to_json(v: BoltType) -> serde_json::Value {
    match v {
        BoltType::Null(_) => serde_json::Value::Null,
        BoltType::Boolean(b) => serde_json::Value::Bool(b.value),
        BoltType::Integer(i) => serde_json::Value::Number(i.value.into()),
        BoltType::Float(f) => serde_json::Number::from_f64(f.value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        BoltType::String(s) => serde_json::Value::String(s.value),
        BoltType::List(l) => bolt_list_to_json(l),
        _ => serde_json::Value::Null,
    }
}

fn extract_string(m: &BoltMap, key: &str) -> Option<String> {
    match m.value.get(&BoltString::from(key))? {
        BoltType::String(s) => Some(s.value.clone()),
        _ => None,
    }
}

fn extract_int(m: &BoltMap, key: &str) -> Option<i64> {
    match m.value.get(&BoltString::from(key))? {
        BoltType::Integer(i) => Some(i.value),
        _ => None,
    }
}

// Suppress dead-code lint on BoltNull import (used in coerce None arm).
#[allow(dead_code)]
fn _touch_null() -> BoltNull {
    BoltNull
}
