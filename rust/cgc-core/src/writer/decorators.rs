//! Promote Python decorators from string properties to first-class
//! `:Decorator` nodes with `:DECORATED_BY` edges.
//!
//! Why: the parser stores each symbol's decorators as a list of raw
//! strings (`["@app.route('/')", "@login_required"]`). That's searchable
//! with LIKE but useless for "give me all functions decorated with
//! `app.route`" structural queries. Promoting to a node per unique
//! decorator name lets users traverse the graph directly.
//!
//! Identity: one `:Decorator` node per normalised name (stripping the
//! leading `@` and any `(...)` arguments). `@app.route('/a')` and
//! `@app.route('/b')` both point to the same `Decorator{name:
//! "app.route"}` node; the full raw expression and line number live on
//! the edge.

use neo4rs::{BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result, DEFAULT_BATCH_SIZE};

/// One `(decorated_symbol)-[:DECORATED_BY]->(Decorator)` edge.
///
/// `symbol_label` must be "Function" or "Class" — the writer uses it to
/// form the dynamic-label MATCH.
pub struct DecoratorRow {
    pub symbol_label: String,
    pub symbol_name: String,
    pub symbol_line: i64,
    pub file_path: String,
    pub decorator_name: String,
    pub decorator_raw: String,
    pub line_number: i64,
}

impl GraphWriter {
    pub async fn write_decorators(&self, rows: &[DecoratorRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // Group by symbol label so each UNWIND batch speaks a single
        // dynamic label. Cypher cannot parameterise labels.
        let mut by_label: std::collections::HashMap<String, Vec<BoltType>> =
            std::collections::HashMap::new();
        for r in rows {
            let mut m = BoltMap::new();
            m.put(
                BoltString::from("symbol_name"),
                BoltType::from(r.symbol_name.clone()),
            );
            m.put(
                BoltString::from("symbol_line"),
                BoltType::from(r.symbol_line),
            );
            m.put(
                BoltString::from("file_path"),
                BoltType::from(r.file_path.clone()),
            );
            m.put(
                BoltString::from("decorator_name"),
                BoltType::from(r.decorator_name.clone()),
            );
            m.put(
                BoltString::from("decorator_raw"),
                BoltType::from(r.decorator_raw.clone()),
            );
            m.put(
                BoltString::from("line_number"),
                BoltType::from(r.line_number),
            );
            by_label
                .entry(r.symbol_label.clone())
                .or_default()
                .push(BoltType::Map(m));
        }
        for (label, batch) in by_label {
            // Whitelist check — symbol_label came from the parser output
            // but we format! it into the Cypher string so a bad label
            // here would allow Cypher injection.
            match label.as_str() {
                "Function" | "Class" => {}
                _ => continue,
            }
            let cypher = format!(
                "UNWIND $batch AS row \
                 MERGE (d:Decorator {{name: row.decorator_name}}) \
                 WITH d, row \
                 MATCH (s:{label} {{name: row.symbol_name, path: row.file_path, line_number: row.symbol_line}}) \
                 MERGE (s)-[r:DECORATED_BY]->(d) \
                 SET r.line_number = row.line_number, r.raw = row.decorator_raw"
            );
            self.run_parallel_chunks(&cypher, batch, DEFAULT_BATCH_SIZE)
                .await?;
        }
        Ok(())
    }
}

/// Strip `@` prefix and trim from the first `(`. `@app.route('/')` →
/// `app.route`. Preserves dotted / colon paths.
pub fn normalise_decorator_name(raw: &str) -> String {
    let no_at = raw.trim().trim_start_matches('@');
    match no_at.find('(') {
        Some(i) => no_at[..i].trim().to_string(),
        None => no_at.trim().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalise_decorator_name() {
        assert_eq!(normalise_decorator_name("@login_required"), "login_required");
        assert_eq!(
            normalise_decorator_name("@app.route('/users')"),
            "app.route"
        );
        assert_eq!(
            normalise_decorator_name("@pytest.fixture(scope='module')"),
            "pytest.fixture"
        );
        assert_eq!(normalise_decorator_name("staticmethod"), "staticmethod");
    }
}
