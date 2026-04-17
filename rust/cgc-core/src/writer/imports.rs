//! `IMPORTS` edges from File -> Module.
//!
//! Modules are MERGEd by name (no path), which means a single Module
//! node can be imported by many files — matching the Python writer's
//! behaviour. `alias` and `full_import_name` are coalesced so later
//! writes never blank out earlier values.

use neo4rs::{query, BoltList, BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result};

pub struct ImportRow {
    pub name: String,
    pub alias: Option<String>,
    pub full_import_name: String,
    pub line_number: i64,
    pub file_path: String,
}

const BATCH_SIZE: usize = 500;

impl GraphWriter {
    pub async fn write_imports(&self, imports: &[ImportRow]) -> Result<()> {
        if imports.is_empty() {
            return Ok(());
        }
        let rows: Vec<BoltType> = imports
            .iter()
            .map(|i| {
                let mut m = BoltMap::new();
                m.put(BoltString::from("name"), BoltType::from(i.name.clone()));
                let alias = match &i.alias {
                    Some(a) => BoltType::from(a.clone()),
                    None => BoltType::Null(neo4rs::BoltNull),
                };
                m.put(BoltString::from("alias"), alias);
                m.put(
                    BoltString::from("full_import_name"),
                    BoltType::from(i.full_import_name.clone()),
                );
                m.put(
                    BoltString::from("line_number"),
                    BoltType::from(i.line_number),
                );
                m.put(
                    BoltString::from("file_path"),
                    BoltType::from(i.file_path.clone()),
                );
                BoltType::Map(m)
            })
            .collect();
        for chunk in rows.chunks(BATCH_SIZE) {
            let list = BoltList {
                value: chunk.to_vec(),
            };
            self.graph()
                .run(
                    query(
                        "UNWIND $batch AS row \
                         MATCH (f:File {path: row.file_path}) \
                         MERGE (m:Module {name: row.name}) \
                         SET m.alias = row.alias, \
                             m.full_import_name = coalesce(row.full_import_name, m.full_import_name) \
                         MERGE (f)-[r:IMPORTS]->(m) \
                         SET r.line_number = row.line_number, r.alias = row.alias",
                    )
                    .param("batch", BoltType::List(list)),
                )
                .await?;
        }
        Ok(())
    }
}
