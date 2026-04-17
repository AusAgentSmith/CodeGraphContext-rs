//! File and directory node writes.
//!
//! This is the "tree" half of `add_files_batch_to_graph`: the File nodes,
//! the directory chain from each file up to the Repository root, and the
//! CONTAINS edges that link them. Symbol writes (Function/Class/etc.),
//! parameters, and imports live in separate modules and run after this.

use std::path::{Component, PathBuf};

use neo4rs::{query, BoltList, BoltMap, BoltString, BoltType};

use super::{GraphWriter, Result};

/// Per-file input row from the parser.
#[derive(Debug, Clone)]
pub struct FileRow {
    pub path: String,
    pub is_dependency: bool,
}

const BATCH_SIZE: usize = 500;

impl GraphWriter {
    /// Write File nodes, Directory chains, and their CONTAINS edges.
    ///
    /// Given the list of files produced by the parser and the repo root,
    /// this materialises:
    ///   * one File node per input row
    ///   * one Directory node per unique ancestor directory between the
    ///     repo root and each file
    ///   * `Repository -[:CONTAINS]-> Directory` / `Directory -[:CONTAINS]->
    ///     Directory` edges forming the tree
    ///   * `Repository -[:CONTAINS]-> File` or `Directory -[:CONTAINS]->
    ///     File` for each file's immediate parent
    ///
    /// The `repo_path` is treated as already resolved; input file paths
    /// are resolved relative to it. Files outside the repo fall back to
    /// their bare file name for `relative_path` (matches the Python
    /// fallback behaviour).
    pub async fn write_file_tree(&self, files: &[FileRow], repo_path: &str) -> Result<usize> {
        if files.is_empty() {
            return Ok(0);
        }

        let repo_root = PathBuf::from(repo_path);

        let mut file_entries: Vec<BoltType> = Vec::with_capacity(files.len());
        // Directory entries keyed by parent label so we can batch by label
        // (Cypher can't parameterise labels).
        let mut dir_entries_from_repo: Vec<BoltType> = Vec::new();
        let mut dir_entries_from_dir: Vec<BoltType> = Vec::new();
        let mut file_parents_from_repo: Vec<BoltType> = Vec::new();
        let mut file_parents_from_dir: Vec<BoltType> = Vec::new();

        for row in files {
            let file_path = PathBuf::from(&row.path);
            let file_name = file_path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| row.path.clone());

            let (rel_path, rel_parts) = match file_path.strip_prefix(&repo_root) {
                Ok(rel) => {
                    let parts: Vec<String> = rel
                        .components()
                        .filter_map(|c| match c {
                            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                            _ => None,
                        })
                        .collect();
                    (rel.to_string_lossy().into_owned(), parts)
                }
                Err(_) => (file_name.clone(), vec![file_name.clone()]),
            };

            file_entries.push(bolt_map([
                ("path", &row.path),
                ("name", &file_name),
                ("relative_path", &rel_path),
            ])
            .with_bool("is_dependency", row.is_dependency)
            .into());

            // Build the chain: repo -> dir1 -> dir2 -> ... -> file's parent.
            // `rel_parts` includes the file name as the last part. Everything
            // before it is a directory we need to materialise.
            let mut parent_path = repo_path.to_string();
            let mut parent_is_root = true;
            let dir_parts = if rel_parts.is_empty() {
                &[][..]
            } else {
                &rel_parts[..rel_parts.len() - 1]
            };

            for part in dir_parts {
                let dir_path = format!("{}/{}", parent_path.trim_end_matches('/'), part);
                let entry: BoltType = bolt_map([
                    ("parent_path", &parent_path),
                    ("dir_path", &dir_path),
                    ("dir_name", part),
                ])
                .into();
                if parent_is_root {
                    dir_entries_from_repo.push(entry);
                } else {
                    dir_entries_from_dir.push(entry);
                }
                parent_path = dir_path;
                parent_is_root = false;
            }

            let fp_entry: BoltType = bolt_map([
                ("parent_path", &parent_path),
                ("file_path", &row.path),
            ])
            .into();
            if parent_is_root {
                file_parents_from_repo.push(fp_entry);
            } else {
                file_parents_from_dir.push(fp_entry);
            }
        }

        // 1. Files
        self.run_batched(
            "UNWIND $batch AS row \
             MERGE (f:File {path: row.path}) \
             SET f.name = row.name, \
                 f.relative_path = row.relative_path, \
                 f.is_dependency = row.is_dependency",
            file_entries,
        )
        .await?;

        // 2. Directory chain — grouped by parent label
        self.run_batched(
            "UNWIND $batch AS row \
             MATCH (p:Repository {path: row.parent_path}) \
             MERGE (d:Directory {path: row.dir_path}) \
             SET d.name = row.dir_name \
             MERGE (p)-[:CONTAINS]->(d)",
            dir_entries_from_repo,
        )
        .await?;
        self.run_batched(
            "UNWIND $batch AS row \
             MATCH (p:Directory {path: row.parent_path}) \
             MERGE (d:Directory {path: row.dir_path}) \
             SET d.name = row.dir_name \
             MERGE (p)-[:CONTAINS]->(d)",
            dir_entries_from_dir,
        )
        .await?;

        // 3. File-parent CONTAINS edges
        self.run_batched(
            "UNWIND $batch AS row \
             MATCH (p:Repository {path: row.parent_path}) \
             MATCH (f:File {path: row.file_path}) \
             MERGE (p)-[:CONTAINS]->(f)",
            file_parents_from_repo,
        )
        .await?;
        self.run_batched(
            "UNWIND $batch AS row \
             MATCH (p:Directory {path: row.parent_path}) \
             MATCH (f:File {path: row.file_path}) \
             MERGE (p)-[:CONTAINS]->(f)",
            file_parents_from_dir,
        )
        .await?;

        Ok(files.len())
    }

    /// Run a UNWIND query against successive 500-row chunks of `batch`.
    async fn run_batched(&self, cypher: &str, batch: Vec<BoltType>) -> Result<()> {
        for chunk in batch.chunks(BATCH_SIZE) {
            let list = BoltList {
                value: chunk.to_vec(),
            };
            let q = query(cypher).param("batch", BoltType::List(list));
            self.graph().run(q).await?;
        }
        Ok(())
    }
}

// ── Small BoltMap builder to cut the verbosity of the per-row construction ──

struct BoltMapBuilder {
    map: BoltMap,
}

impl BoltMapBuilder {
    fn with_bool(mut self, key: &str, value: bool) -> Self {
        self.map.put(BoltString::from(key), BoltType::from(value));
        self
    }
}

impl From<BoltMapBuilder> for BoltType {
    fn from(b: BoltMapBuilder) -> Self {
        BoltType::Map(b.map)
    }
}

/// Build a BoltMap from (key, &str) pairs. Strings are cloned into Bolt.
fn bolt_map<'a, I>(pairs: I) -> BoltMapBuilder
where
    I: IntoIterator<Item = (&'a str, &'a String)>,
{
    let mut m = BoltMap::new();
    for (k, v) in pairs {
        m.put(BoltString::from(k), BoltType::from(v.clone()));
    }
    BoltMapBuilder { map: m }
}
