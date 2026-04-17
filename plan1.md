# Recommendations for CodeGraphContext-rs (Py+Rust focus)

Tiered by impact on the MCP context-delivery experience. Each item includes scope so you can judge effort.

## Tier 1 — Biggest experience wins

### 1. Replace tree-sitter call/inheritance edges with type-resolved indexing
The single highest-leverage change. Tree-sitter `CALLS`/`INHERITS` are name-matched and lossy on Rust trait dispatch, generics, macros, and Python dynamic dispatch. This is *the* thing that makes Claude doubt the graph and fall back to grep.

- **Rust**: invoke `rust-analyzer` with SCIP output (`rust-analyzer scip .`). Ingest the SCIP index for symbols, definitions, references, trait impls, macro expansions.
- **Python**: use `scip-python` (or pyright-based equivalent) for resolved imports, method dispatch on known types, decorator targets.
- Keep tree-sitter as the fallback for files the semantic indexer rejects (broken code, partial projects, unknown dialects) — so you keep tree-sitter's error tolerance without its ceiling.

### 2. Specialize the Neo4j schema for Py + Rust
With 20 languages gone, stop mapping everything to generic `Class`/`Function`. Add first-class nodes for concepts that actually matter:

- Rust: `Trait`, `Impl`, `Macro`, `AsyncFn`, relationships `IMPLEMENTS_TRAIT`, `EXPANDS_TO`.
- Python: `Decorator` as node, `DECORATED_BY`, `OVERRIDES` for MRO, `AsyncFn`.
- Expose these as new `analyze_code_relationships` query types (`find_trait_impls`, `find_macro_expansions`, `find_decorator_usages`).

This directly improves the answers Claude gets back — more specific edges mean shorter, more precise MCP responses.

## Tier 2 — Architectural cleanup that enables Tier 1

### 3. Move the Neo4j writer into Rust
Today: Rust parses → PyO3 copy → Python builds Cypher → Bolt. Target: Rust parses → Rust-side `neo4rs` writer → Bolt. Eliminates an FFI hop per file and keeps parsed output in-process. Initial-index time drops meaningfully on large repos.

Pairs naturally with Tier 1 #1: SCIP output is already Rust-native, so the whole parse → resolve → write pipeline stays in one process.

### 4. Delete SCIP protobuf scaffolding and multi-lang fallbacks
- `scip_pb2.py` (2.5k LOC) and the generic `scip_indexer.py` go away — or get repurposed as the *consumer* of rust-analyzer/scip-python output (Tier 1 #1), not a generic cross-lang layer.
- Prune `code_finder.py`'s polyglot branching (~1160 lines → likely halves).
- Delete 18 of 20 language parsers under `rust/cgc-core/src/lang/` and the corresponding `tools/languages/` + `query_tool_languages/`.

### 5. Move the file watcher into Rust
Use `notify` crate directly, co-located with the parser and writer. Fewer missed events on rename-heavy editing, no IPC for incremental updates. Smaller win than #3 but compounds with it.

## Tier 3 — MCP surface polish

### 6. Shrink the tool schema
- Drop `visualize_graph_query` (you don't use Neo4j Browser).
- Drop `add_package_to_graph`'s language enum down to `["python", "rust"]`.
- Consider merging `find_dead_code`, `find_most_complex_functions`, `calculate_cyclomatic_complexity` into a single `code_metrics` tool — three separate tools eat context for one conceptual capability.
- Keep `execute_cypher_query` — it's the escape hatch and costs little.

Each tool removed/merged saves ~100–200 tokens per session in Claude's tool list.

### 7. Tighten tool descriptions
`analyze_code_relationships` currently lists 15 `query_type` values in a single enum. With Tier 1 #2 you'll add more — group them in the description by category (callers, types, imports, metrics) so Claude picks faster.

## Explicitly not worth doing

- **Rewriting the MCP server in Rust.** Stdio JSON-RPC; Neo4j latency dominates.
- **Rewriting `code_finder.py` in Rust.** It generates Cypher strings; the DB runs the query.
- **Rewriting the job queue / handlers.** Not a bottleneck, and Python dispatch is genuinely convenient here.
- **Adding more languages back later.** The specialized schema (Tier 1 #2) is the point — generalizing it back out undoes the win.

## Suggested sequencing

1. **Delete first** (Tier 2 #4, Tier 3 #6): smallest risk, immediate context savings, makes the codebase legible for the bigger changes.
2. **Rust writer** (Tier 2 #3): isolated refactor, measurable indexing speedup, sets up the pipeline for #1.
3. **Type-resolved indexing** (Tier 1 #1): the actual feature work. Ship Rust side first (rust-analyzer SCIP), then Python.
4. **Schema specialization** (Tier 1 #2): done in lockstep with #3 as resolved data lands.
5. **Watcher in Rust** (Tier 2 #5): last, once the Rust pipeline is the primary path.

Rough order-of-magnitude effort: steps 1–2 are days, step 3 is weeks, steps 4–5 are days each riding on the earlier work.
