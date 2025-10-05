# MCPServer

The `MCPServer` class is the core of the CodeGraphContext application. It orchestrates all the major components, including database management, background job tracking, file system watching, and tool handling.

## Initialization

The server is initialized with an asyncio event loop. Upon initialization, it sets up the following components:

-   `DatabaseManager`: Manages the connection to the Neo4j database.
-   `JobManager`: Tracks the status of background jobs, such as code indexing.
-   `CodeWatcher`: Monitors the file system for changes to keep the code graph up-to-date.
-   `GraphBuilder`: Builds the code graph from the source code.
-   `CodeFinder`: Provides tools for searching and analyzing the code graph.

## Tool Manifest

The `MCPServer` exposes a set of tools to the AI assistant. These tools are defined in the `_init_tools` method. Here is a list of available tools:

### `add_code_to_graph`
Performs a one-time scan of a local folder to add its code to the graph. Ideal for indexing libraries, dependencies, or projects not being actively modified. Returns a job ID for background processing.

### `check_job_status`
Check the status and progress of a background job.

### `list_jobs`
List all background jobs and their current status.

### `find_code`
Find relevant code snippets related to a keyword (e.g., function name, class name, or content).

### `analyze_code_relationships`
Analyze code relationships like 'who calls this function' or 'class hierarchy'. Supported query types include: find_callers, find_callees, find_all_callers, find_all_callees, find_importers, who_modifies, class_hierarchy, overrides, dead_code, call_chain, module_deps, variable_scope, find_complexity, find_functions_by_argument, find_functions_by_decorator.

### `watch_directory`
Performs an initial scan of a directory and then continuously monitors it for changes, automatically keeping the graph up-to-date. Ideal for projects under active development. Returns a job ID for the initial scan.

### `execute_cypher_query`

This is an advanced tool for directly querying the underlying Neo4j graph using a read-only Cypher query. It should be used as a fallback when other tools cannot answer very specific or complex questions about the code graph.

**Schema Overview:**

The code graph has the following structure:

*   **Nodes:**
    *   `Repository`: Represents a project repository.
    *   `File`: Represents a single source code file.
    *   `Module`: Represents a Python module.
    *   `Class`: Represents a class definition.
    *   `Function`: Represents a function or method definition.
*   **Properties:**
    *   Nodes have properties like `name`, `path`, `cyclomatic_complexity` (for functions), and `code`.
*   **Relationships:**
    *   `CONTAINS`: e.g., `(:File)-[:CONTAINS]->(:Function)`
    *   `CALLS`: e.g., `(:Function)-[:CALLS]->(:Function)`
    *   `IMPORTS`: e.g., `(:File)-[:IMPORTS]->(:Module)`
    *   `INHERITS`: e.g., `(:Class)-[:INHERITS]->(:Class)`

### `add_package_to_graph`
Add a Python package to Neo4j graph by discovering its location. Returns immediately with job ID.

### `find_dead_code`
Find potentially unused functions (dead code) across the entire indexed codebase, optionally excluding functions with specific decorators.

### `calculate_cyclomatic_complexity`
Calculate the cyclomatic complexity of a specific function to measure its complexity.

### `find_most_complex_functions`
Find the most complex functions in the codebase based on cyclomatic complexity.

### `list_indexed_repositories`
List all indexed repositories.

### `delete_repository`
Delete an indexed repository from the graph.

### `visualize_graph_query`
Generates a URL to visualize the results of a Cypher query in the Neo4j Browser. The user can open this URL in their web browser to see the graph visualization.

### `list_watched_paths`
Lists all directories currently being watched for live file changes.

### `unwatch_directory`
Stops watching a directory for live file changes.

## Other Methods

-   `get_database_status()`: Returns the current connection status of the Neo4j database.
-   `get_local_package_path(package_name)`: Finds the local installation path of a Python package.
-   `run()`: Runs the main server loop, listening for JSON-RPC requests from stdin.
-   `shutdown()`: Gracefully shuts down the server and its components.
