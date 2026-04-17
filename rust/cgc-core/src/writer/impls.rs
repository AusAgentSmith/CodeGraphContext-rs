//! `IMPLEMENTS` edges from a concrete type (Struct / Enum / Class) to a
//! Trait node for Rust `impl Trait for Type` blocks.
//!
//! The edge is scoped by the file the impl was declared in (the type
//! side), since `impl Foo for Bar` attaches to the Bar definition in
//! that file. The Trait on the other end is matched globally by name
//! — a Trait's node has no single "home" file from the impl's
//! perspective and the indexed trait may live in another file.
//!
//! If the referenced Trait hasn't been indexed (e.g. foreign trait from
//! a dependency we haven't scanned), no edge is emitted. This matches
//! the existing behaviour of INHERITS resolution.

use neo4rs::{query, BoltList, BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result};

pub struct ImplRow {
    pub type_name: String,
    pub trait_name: String,
    pub file_path: String,
    pub line_number: i64,
}

const BATCH_SIZE: usize = 500;

impl GraphWriter {
    pub async fn write_impls(&self, impls: &[ImplRow]) -> Result<()> {
        if impls.is_empty() {
            return Ok(());
        }
        let rows: Vec<BoltType> = impls
            .iter()
            .map(|i| {
                let mut m = BoltMap::new();
                m.put(
                    BoltString::from("type_name"),
                    BoltType::from(i.type_name.clone()),
                );
                m.put(
                    BoltString::from("trait_name"),
                    BoltType::from(i.trait_name.clone()),
                );
                m.put(
                    BoltString::from("file_path"),
                    BoltType::from(i.file_path.clone()),
                );
                m.put(
                    BoltString::from("line_number"),
                    BoltType::from(i.line_number),
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
                         MATCH (t) \
                         WHERE (t:Struct OR t:Enum OR t:Class) \
                           AND t.name = row.type_name \
                           AND t.path = row.file_path \
                         MATCH (tr:Trait {name: row.trait_name}) \
                         MERGE (t)-[r:IMPLEMENTS]->(tr) \
                         SET r.line_number = row.line_number",
                    )
                    .param("batch", BoltType::List(list)),
                )
                .await?;
        }
        Ok(())
    }
}
