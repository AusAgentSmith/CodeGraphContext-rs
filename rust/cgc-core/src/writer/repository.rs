//! Repository-level writes: the root node every file attaches to.

use neo4rs::query;

use super::{GraphWriter, Result};

impl GraphWriter {
    /// MERGE the root Repository node. Idempotent.
    pub async fn add_repository(
        &self,
        path: &str,
        name: &str,
        is_dependency: bool,
    ) -> Result<()> {
        let q = query(
            "MERGE (r:Repository {path: $path}) \
             SET r.name = $name, r.is_dependency = $is_dependency",
        )
        .param("path", path)
        .param("name", name)
        .param("is_dependency", is_dependency);
        self.graph().run(q).await?;
        Ok(())
    }
}
