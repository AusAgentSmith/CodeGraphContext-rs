//! INHERITS edges between Class nodes.
//!
//! Upstream, resolution::inheritance::build_inheritance produces a list
//! of child→parent links each keyed by child name+path and resolved
//! parent name+path. This writer is a thin MERGE over that list.

use neo4rs::{query, BoltList, BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result};

pub struct InheritanceLinkRow {
    pub child_name: String,
    pub path: String,
    pub parent_name: String,
    pub resolved_parent_file_path: String,
}

const BATCH_SIZE: usize = 500;

impl GraphWriter {
    pub async fn write_inheritance(&self, links: &[InheritanceLinkRow]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let rows: Vec<BoltType> = links
            .iter()
            .map(|l| {
                let mut m = BoltMap::new();
                m.put(
                    BoltString::from("child_name"),
                    BoltType::from(l.child_name.clone()),
                );
                m.put(BoltString::from("path"), BoltType::from(l.path.clone()));
                m.put(
                    BoltString::from("parent_name"),
                    BoltType::from(l.parent_name.clone()),
                );
                m.put(
                    BoltString::from("resolved_parent_file_path"),
                    BoltType::from(l.resolved_parent_file_path.clone()),
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
                         MATCH (child:Class {name: row.child_name, path: row.path}) \
                         MATCH (parent:Class {name: row.parent_name, path: row.resolved_parent_file_path}) \
                         MERGE (child)-[:INHERITS]->(parent)",
                    )
                    .param("batch", BoltType::List(list)),
                )
                .await?;
        }
        Ok(())
    }
}
