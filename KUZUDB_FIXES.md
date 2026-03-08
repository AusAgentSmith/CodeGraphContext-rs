# KùzuDB Integration Fixes - Summary

## Issues Identified and Resolved

### 1. **Polymorphic MERGE Not Supported**
**Problem**: KùzuDB doesn't support creating relationships where endpoints can be multiple node types (e.g., `MATCH (n) WHERE (n:Function OR n:Class)`).

**Error**: `Binder exception: Create rel bound by multiple node labels is not supported.`

**Solution**: Modified `graph_builder.py` to use separate, specific queries for each type combination:
- Function → Function
- Function → Class  
- Class → Function
- Class → Class
- File → Function
- File → Class

Each query uses `OPTIONAL MATCH` with `WHERE IS NOT NULL` checks and returns a count to determine if the relationship was created.

### 2. **ORDER BY Scope Issues with DISTINCT**
**Problem**: KùzuDB requires `ORDER BY` to use result aliases when `DISTINCT` is present, not direct property access.

**Error**: `Binder exception: Variable <name> is not in scope.`

**Solution**: Updated all `ORDER BY` clauses in `code_finder.py` to use result aliases:
- Changed `ORDER BY caller.is_dependency` → `ORDER BY caller_is_dependency`
- Changed `ORDER BY called.name` → `ORDER BY called_function`

### 3. **Schema Property Mismatches**
**Problem**: KùzuDB schema was missing properties that the indexer tries to set (`end_line`, `decorators`).

**Solution**: Updated `database_kuzu.py` node table schemas to include:
- `end_line INT64` for all code element types
- `decorators STRING[]` for Function and Class types

### 4. **Variable-Length Path Syntax Limitations** ✅ **RESOLVED**
**Problem**: KùzuDB doesn't allow binding the end node in variable-length path patterns.

**Error**: `Parser exception: Invalid input <(end>` when using `(start)-[:CALLS*]->(end)`

**Solution**: Use anonymous end nodes `()` and extract the target using path functions:
```cypher
MATCH p = (start:Function)-[:CALLS*1..5]->()
WITH p, nodes(p) as path_nodes
WITH list_extract(path_nodes, size(path_nodes)) as end_node
WHERE end_node.name = $target_name
```

**Key KùzuDB Path Functions**:
- `nodes(path)` - Returns array of nodes in path
- `relationships(path)` - Returns array of relationships
- `length(path)` - Returns path length
- `size(list)` - Returns list size
- `list_extract(list, index)` - Extracts element at index (1-based)

## Files Modified

1. **`src/codegraphcontext/tools/graph_builder.py`**
   - Replaced polymorphic MERGE queries with type-specific fallback queries
   - Added KùzuDB-specific workarounds for relationship creation

2. **`src/codegraphcontext/tools/code_finder.py`**
   - Updated ORDER BY clauses to use result aliases
   - Fixed scope issues in query methods
   - **Updated variable-length path queries** (`find_all_callers`, `find_all_callees`, `find_function_call_chain`)

3. **`src/codegraphcontext/core/database_kuzu.py`**
   - Added missing schema properties (`end_line`, `decorators`)
   - Added `name` property for backend identification

## Testing Results

✅ **Working**:
- Repository indexing
- Function/Class/Variable extraction
- Basic queries (find, stats, list)
- CALLS relationship creation
- CONTAINS relationship creation
- **Variable-length path queries** (analyze callers, analyze callees, analyze chain)

⚠️ **Known Limitations**:
- Cannot bind end node in variable-length patterns (workaround implemented)
- Union label syntax (`MATCH (n:Function|Class)`) not supported
- FOREACH clause not supported
- COALESCE with node variables in MERGE not supported
- Polymorphic MERGE not supported (workaround implemented)

## Recommendations

1. **Disable debug logging**: Remove or comment out the DEBUG_KUZU print statements in `database_kuzu.py` for production use.

2. **Performance consideration**: Variable-length path queries with anonymous end nodes may be slower than Neo4j's direct binding. Monitor performance on large codebases.

3. **Document limitations**: Update user documentation to note KùzuDB-specific syntax differences.

4. **Future work**: Consider adding a query optimizer layer that automatically rewrites queries based on the active backend.

## KùzuDB Version Tested
- **KùzuDB 0.11.3**
