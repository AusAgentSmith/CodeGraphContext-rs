# Claude guidance for this project

## Prefer `codegraphcontext` MCP tools for structural questions

This repo exposes a Neo4j-backed code graph through the `codegraphcontext` MCP server. When a question is about **structure, relationships, or scale**, prefer the MCP tools over reading files:

- **Call graph**: who calls `X`, what does `X` call, full call chains → `analyze_code_relationships`
- **Type hierarchy**: what extends `X`, what `X` inherits, overrides of `run()` → `analyze_code_relationships`
- **Finding code**: by name, pattern, content, decorator, argument → `find_code`
- **Repo-wide searches**: "all usages of", "everywhere that imports X", "every function returning Y" → `find_code` or `execute_cypher_query`
- **Metrics**: dead code, cyclomatic complexity, most complex functions → `find_dead_code`, `calculate_cyclomatic_complexity`, `find_most_complex_functions`
- **Arbitrary graph queries**: read-only Cypher via `execute_cypher_query`

Grep/Read is still the right choice for:
- Reading a specific file you already have a path for
- Quick local questions scoped to one or two files
- Content you'd only find via raw text (comments, string literals Claude hasn't indexed into the graph)

## When the graph might be stale

The graph reflects whatever was last indexed. If you've made significant edits in this session, `cgc` has a watcher (`cgc watch`) that updates incrementally, but MCP-mode consumers won't auto-reindex. If a graph answer contradicts what you can see in the working tree, trust the working tree and flag the staleness to the user.

## Project context

- **Backend**: Neo4j only (Bolt at `bolt://100.92.54.45:7687`). KùzuDB / FalkorDB support was intentionally removed — do not suggest switching back.
- **Indexing engine**: Rust (PyO3) — the tree-sitter parsing lives under `rust/cgc-core/src/lang/`. The Python side orchestrates jobs, writes to Neo4j, and exposes the MCP interface.
- **MCP server entry**: `src/codegraphcontext/server.py` (`MCPServer`), tools defined in `src/codegraphcontext/tools/`.
