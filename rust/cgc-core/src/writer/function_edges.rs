//! Secondary edges anchored on Function nodes:
//!   - HAS_PARAMETER: function's arguments as Parameter nodes
//!   - Class CONTAINS Function: when a function has a class_context
//!   - Function CONTAINS Function: for nested function definitions
//!
//! All three require the Function and Class nodes to already exist, so
//! these run after write_symbols.

use neo4rs::{BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result, DEFAULT_BATCH_SIZE};

/// (function_name, line_number, arg_name, file_path)
pub struct ParamRow {
    pub func_name: String,
    pub line_number: i64,
    pub arg_name: String,
    pub file_path: String,
}

/// A function defined inside a class body.
pub struct ClassFnRow {
    pub class_name: String,
    pub func_name: String,
    pub func_line: i64,
    pub file_path: String,
}

/// A function defined inside another function.
pub struct NestedFnRow {
    pub outer: String,
    pub inner_name: String,
    pub inner_line: i64,
    pub file_path: String,
}

impl GraphWriter {
    pub async fn write_function_edges(
        &self,
        params: &[ParamRow],
        class_fns: &[ClassFnRow],
        nested_fns: &[NestedFnRow],
    ) -> Result<()> {
        if !params.is_empty() {
            let rows: Vec<BoltType> = params
                .iter()
                .map(|p| {
                    let mut m = BoltMap::new();
                    m.put(BoltString::from("func_name"), BoltType::from(p.func_name.clone()));
                    m.put(BoltString::from("line_number"), BoltType::from(p.line_number));
                    m.put(BoltString::from("arg_name"), BoltType::from(p.arg_name.clone()));
                    m.put(BoltString::from("file_path"), BoltType::from(p.file_path.clone()));
                    BoltType::Map(m)
                })
                .collect();
            self.run_parallel_chunks(
                "UNWIND $batch AS row \
                 MATCH (fn:Function {name: row.func_name, path: row.file_path, line_number: row.line_number}) \
                 MERGE (p:Parameter {name: row.arg_name, path: row.file_path, function_line_number: row.line_number}) \
                 MERGE (fn)-[:HAS_PARAMETER]->(p)",
                rows,
                DEFAULT_BATCH_SIZE,
            )
            .await?;
        }

        if !class_fns.is_empty() {
            let rows: Vec<BoltType> = class_fns
                .iter()
                .map(|c| {
                    let mut m = BoltMap::new();
                    m.put(BoltString::from("class_name"), BoltType::from(c.class_name.clone()));
                    m.put(BoltString::from("func_name"), BoltType::from(c.func_name.clone()));
                    m.put(BoltString::from("func_line"), BoltType::from(c.func_line));
                    m.put(BoltString::from("file_path"), BoltType::from(c.file_path.clone()));
                    BoltType::Map(m)
                })
                .collect();
            self.run_parallel_chunks(
                "UNWIND $batch AS row \
                 MATCH (c:Class {name: row.class_name, path: row.file_path}) \
                 MATCH (fn:Function {name: row.func_name, path: row.file_path, line_number: row.func_line}) \
                 MERGE (c)-[:CONTAINS]->(fn)",
                rows,
                DEFAULT_BATCH_SIZE,
            )
            .await?;
        }

        if !nested_fns.is_empty() {
            let rows: Vec<BoltType> = nested_fns
                .iter()
                .map(|n| {
                    let mut m = BoltMap::new();
                    m.put(BoltString::from("outer"), BoltType::from(n.outer.clone()));
                    m.put(BoltString::from("inner_name"), BoltType::from(n.inner_name.clone()));
                    m.put(BoltString::from("inner_line"), BoltType::from(n.inner_line));
                    m.put(BoltString::from("file_path"), BoltType::from(n.file_path.clone()));
                    BoltType::Map(m)
                })
                .collect();
            self.run_parallel_chunks(
                "UNWIND $batch AS row \
                 MATCH (outer:Function {name: row.outer, path: row.file_path}) \
                 MATCH (inner:Function {name: row.inner_name, path: row.file_path, line_number: row.inner_line}) \
                 MERGE (outer)-[:CONTAINS]->(inner)",
                rows,
                DEFAULT_BATCH_SIZE,
            )
            .await?;
        }

        Ok(())
    }
}
