//! `CALLS` edges — the resolved call graph.
//!
//! Six variants, keyed on the label combination of caller→callee:
//! Function→Function, Function→Class, Class→Function, Class→Class,
//! File→Function, File→Class. File-caller edges omit caller_line_number
//! in the MATCH because a File has no line. The edge payload
//! (line_number / args / full_call_name) is identical in every case.

use neo4rs::{BoltList, BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result};

/// One row emitted by the resolution pass per resolved call site.
pub struct CallRow {
    pub caller_name: String,
    pub caller_file_path: String,
    /// Ignored for File-caller groups.
    pub caller_line_number: i64,
    pub called_name: String,
    pub called_file_path: String,
    pub line_number: i64,
    pub args: Vec<String>,
    pub full_call_name: String,
}

const BATCH_SIZE: usize = 1000;

/// Which of the six variants a batch belongs to.
#[derive(Clone, Copy)]
pub enum CallGroup {
    FnToFn,
    FnToCls,
    ClsToFn,
    ClsToCls,
    FileToFn,
    FileToCls,
}

impl CallGroup {
    fn label(self) -> &'static str {
        match self {
            Self::FnToFn => "fn->fn",
            Self::FnToCls => "fn->cls",
            Self::ClsToFn => "cls->fn",
            Self::ClsToCls => "cls->cls",
            Self::FileToFn => "file->fn",
            Self::FileToCls => "file->cls",
        }
    }

    fn cypher(self) -> &'static str {
        match self {
            Self::FnToFn => {
                "UNWIND $batch AS row \
                 MATCH (caller:Function {name: row.caller_name, path: row.caller_file_path, line_number: row.caller_line_number}) \
                 MATCH (called:Function {name: row.called_name, path: row.called_file_path}) \
                 MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(called)"
            }
            Self::FnToCls => {
                "UNWIND $batch AS row \
                 MATCH (caller:Function {name: row.caller_name, path: row.caller_file_path, line_number: row.caller_line_number}) \
                 MATCH (called:Class {name: row.called_name, path: row.called_file_path}) \
                 MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(called)"
            }
            Self::ClsToFn => {
                "UNWIND $batch AS row \
                 MATCH (caller:Class {name: row.caller_name, path: row.caller_file_path, line_number: row.caller_line_number}) \
                 MATCH (called:Function {name: row.called_name, path: row.called_file_path}) \
                 MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(called)"
            }
            Self::ClsToCls => {
                "UNWIND $batch AS row \
                 MATCH (caller:Class {name: row.caller_name, path: row.caller_file_path, line_number: row.caller_line_number}) \
                 MATCH (called:Class {name: row.called_name, path: row.called_file_path}) \
                 MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(called)"
            }
            Self::FileToFn => {
                "UNWIND $batch AS row \
                 MATCH (caller:File {path: row.caller_file_path}) \
                 MATCH (called:Function {name: row.called_name, path: row.called_file_path}) \
                 MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(called)"
            }
            Self::FileToCls => {
                "UNWIND $batch AS row \
                 MATCH (caller:File {path: row.caller_file_path}) \
                 MATCH (called:Class {name: row.called_name, path: row.called_file_path}) \
                 MERGE (caller)-[:CALLS {line_number: row.line_number, args: row.args, full_call_name: row.full_call_name}]->(called)"
            }
        }
    }
}

impl GraphWriter {
    pub async fn write_call_group(&self, group: CallGroup, rows: &[CallRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let payload: Vec<BoltType> = rows.iter().map(row_to_bolt).collect();
        self.run_parallel_chunks(group.cypher(), payload, BATCH_SIZE).await
    }

    pub async fn write_call_groups(
        &self,
        fn_to_fn: &[CallRow],
        fn_to_cls: &[CallRow],
        cls_to_fn: &[CallRow],
        cls_to_cls: &[CallRow],
        file_to_fn: &[CallRow],
        file_to_cls: &[CallRow],
    ) -> Result<()> {
        let total = fn_to_fn.len()
            + fn_to_cls.len()
            + cls_to_fn.len()
            + cls_to_cls.len()
            + file_to_fn.len()
            + file_to_cls.len();
        self.write_call_group(CallGroup::FnToFn, fn_to_fn).await?;
        self.write_call_group(CallGroup::FnToCls, fn_to_cls).await?;
        self.write_call_group(CallGroup::ClsToFn, cls_to_fn).await?;
        self.write_call_group(CallGroup::ClsToCls, cls_to_cls).await?;
        self.write_call_group(CallGroup::FileToFn, file_to_fn).await?;
        self.write_call_group(CallGroup::FileToCls, file_to_cls).await?;
        let _ = total;
        Ok(())
    }
}

fn row_to_bolt(row: &CallRow) -> BoltType {
    let mut m = BoltMap::new();
    m.put(
        BoltString::from("caller_name"),
        BoltType::from(row.caller_name.clone()),
    );
    m.put(
        BoltString::from("caller_file_path"),
        BoltType::from(row.caller_file_path.clone()),
    );
    m.put(
        BoltString::from("caller_line_number"),
        BoltType::from(row.caller_line_number),
    );
    m.put(
        BoltString::from("called_name"),
        BoltType::from(row.called_name.clone()),
    );
    m.put(
        BoltString::from("called_file_path"),
        BoltType::from(row.called_file_path.clone()),
    );
    m.put(
        BoltString::from("line_number"),
        BoltType::from(row.line_number),
    );
    let mut args = BoltList::new();
    for a in &row.args {
        args.value.push(BoltType::from(a.clone()));
    }
    m.put(BoltString::from("args"), BoltType::List(args));
    m.put(
        BoltString::from("full_call_name"),
        BoltType::from(row.full_call_name.clone()),
    );
    BoltType::Map(m)
}

#[allow(dead_code)]
pub fn call_group_label(group: CallGroup) -> &'static str {
    group.label()
}
