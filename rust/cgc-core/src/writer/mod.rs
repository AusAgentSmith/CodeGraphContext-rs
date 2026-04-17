//! Direct-to-Neo4j graph writer (bypasses the Python Bolt driver).
//!
//! Ownership split:
//! - The Rust writer owns a neo4rs connection pool used for all index-time
//!   writes. GIL is released for every call so multiple Rust threads can
//!   submit transactions concurrently.
//! - The Python `neo4j` driver remains the owner of read queries
//!   (code_finder.py's ~1100 LOC of Cypher). Two pools to the same DB is
//!   fine; Neo4j supports many concurrent connections.
//!
//! Transactions are per-method, not per-file. Each writer method opens one
//! transaction, batches its writes, and commits.

mod calls;
mod decorators;
mod files;
mod function_edges;
mod imports;
mod impls;
mod inheritance;
mod repository;
mod symbols;

pub use calls::{CallGroup, CallRow};
pub use decorators::{normalise_decorator_name, DecoratorRow};
pub use files::FileRow;
pub use function_edges::{ClassFnRow, NestedFnRow, ParamRow};
pub use imports::ImportRow;
pub use impls::ImplRow;
pub use inheritance::InheritanceLinkRow;
pub use symbols::{SymbolBatch, SYMBOL_LABELS};

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt, TryStreamExt};
use neo4rs::{query, BoltList, BoltType, ConfigBuilder, Graph, Query};
use thiserror::Error;

/// How many UNWIND batches to keep in flight concurrently. Each one
/// runs on its own Neo4j worker thread — on the write path we're
/// bottlenecked on Neo4j saturating one core per query, so fanning
/// out N batches scales write throughput ~linearly up to server core
/// count. 10 picks up most of the win without starving `cgc watch`
/// from its own Bolt connection.
pub const BATCH_CONCURRENCY: usize = 10;

/// Default rows per UNWIND. Submodules can override when a Cypher
/// template is notably cheaper/heavier than average.
pub const DEFAULT_BATCH_SIZE: usize = 500;

#[derive(Error, Debug)]
pub enum WriterError {
    #[error("neo4j error: {0}")]
    Neo4rs(#[from] neo4rs::Error),

    #[error("neo4j deserialisation error: {0}")]
    Deserialization(#[from] neo4rs::DeError),

    #[error("invalid label: {0}")]
    InvalidLabel(String),
}

pub type Result<T> = std::result::Result<T, WriterError>;

/// Whitelist of node labels allowed in dynamic-label Cypher.
///
/// Cypher does not parameterize labels; dynamic labels are built via
/// `format!`. We enforce a fixed whitelist so a corrupted parser output
/// cannot inject arbitrary Cypher via a label string.
const ALLOWED_LABELS: &[&str] = &[
    "Repository",
    "Directory",
    "File",
    "Module",
    "Function",
    "Class",
    "Trait",
    "Variable",
    "Interface",
    "Macro",
    "Struct",
    "Enum",
    "Union",
    "Record",
    "Property",
    "Parameter",
];

pub fn validate_label(label: &str) -> Result<&str> {
    if ALLOWED_LABELS.contains(&label) {
        Ok(label)
    } else {
        Err(WriterError::InvalidLabel(label.to_string()))
    }
}

/// Handle to a Neo4j connection pool.
///
/// Cheap to clone (Arc internally). Shared across threads.
#[derive(Clone)]
pub struct GraphWriter {
    graph: Arc<Graph>,
    database: Option<String>,
}

impl GraphWriter {
    /// Connect to Neo4j. URI supports both `bolt://` and `neo4j://` schemes.
    pub async fn connect(
        uri: &str,
        user: &str,
        password: &str,
        database: Option<String>,
    ) -> Result<Self> {
        let mut builder = ConfigBuilder::default()
            .uri(uri)
            .user(user)
            .password(password);
        if let Some(db) = database.as_deref() {
            builder = builder.db(db);
        }
        let config = builder.build()?;
        let graph = Graph::connect(config).await?;
        Ok(Self {
            graph: Arc::new(graph),
            database,
        })
    }

    /// Expose the inner Graph for methods that need direct access.
    pub(crate) fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Return the configured database name, if any.
    pub(crate) fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Run a single UNWIND with `rows` as `$batch`. No chunking — caller
    /// must pre-size the batch. Useful for one-shot writes that don't
    /// need the chunking helpers below.
    pub(crate) async fn run_unwind(&self, cypher: &str, rows: Vec<BoltType>) -> Result<()> {
        let list = BoltList { value: rows };
        self.graph
            .run(query(cypher).param("batch", BoltType::List(list)))
            .await?;
        Ok(())
    }

    /// Chunk `rows` by `batch_size` and submit up to `BATCH_CONCURRENCY`
    /// UNWINDs concurrently. Each chunk becomes one Neo4j transaction on
    /// its own worker thread. Transient errors (deadlocks, lock timeouts
    /// — Neo4j's `TransientError.*` class) retry with exponential
    /// backoff; any other error fails the whole fan-out.
    ///
    /// Safe when batches are independent (no row in chunk A depends on
    /// state written by chunk B). Same-key MERGEs across concurrent
    /// batches serialise via the unique-constraint lock inside Neo4j —
    /// under heavy contention that serialisation can escalate into a
    /// deadlock, which is why we retry transient errors rather than
    /// failing fast.
    pub(crate) async fn run_parallel_chunks(
        &self,
        cypher: &str,
        rows: Vec<BoltType>,
        batch_size: usize,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let chunks: Vec<Vec<BoltType>> =
            rows.chunks(batch_size).map(|c| c.to_vec()).collect();
        stream::iter(chunks)
            .map(|chunk| {
                let graph = self.graph.clone();
                let cypher = cypher.to_string();
                async move { run_with_retry(&graph, &cypher, chunk).await }
            })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// Lightweight connectivity check — runs `RETURN 1`.
    ///
    /// Used by `init_writer` in the PyO3 bindings to fail fast if auth or
    /// routing is wrong, rather than erroring on the first real write.
    pub async fn ping(&self) -> Result<()> {
        let mut result = self.graph.execute(neo4rs::query("RETURN 1 AS n")).await?;
        let _row = result.next().await?;
        Ok(())
    }
}

/// Retry loop for one UNWIND chunk. neo4rs doesn't expose error-class
/// tags we can match on; we inspect the error string for Neo4j's
/// `TransientError.*` codes (DeadlockDetected, LockClientStopped,
/// LockAcquisitionTimeout). Backoff is exponential with a small jitter
/// seed from the attempt count so concurrent retries don't collide.
async fn run_with_retry(
    graph: &Graph,
    cypher: &str,
    chunk: Vec<BoltType>,
) -> Result<()> {
    const MAX_RETRIES: u32 = 6;
    let mut attempt: u32 = 0;
    loop {
        let q = query(cypher).param(
            "batch",
            BoltType::List(BoltList { value: chunk.clone() }),
        );
        match graph.run(q).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if attempt < MAX_RETRIES && is_transient(&e) {
                    attempt += 1;
                    // 50ms, 100ms, 200ms, 400ms, 800ms, 1600ms
                    let base = 50u64 * (1u64 << (attempt - 1).min(5));
                    // Jitter: attempt-derived, keeps concurrent retries
                    // from beating in lockstep.
                    let jitter = (attempt as u64 * 17) % 40;
                    tokio::time::sleep(Duration::from_millis(base + jitter)).await;
                    continue;
                }
                return Err(WriterError::from(e));
            }
        }
    }
}

fn is_transient(e: &neo4rs::Error) -> bool {
    let s = format!("{e}");
    s.contains("TransientError")
        || s.contains("DeadlockDetected")
        || s.contains("LockAcquisitionTimeout")
        || s.contains("LockClientStopped")
}
