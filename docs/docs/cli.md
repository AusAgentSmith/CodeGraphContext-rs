# CLI Reference

The CodeGraphContext CLI provides a set of commands to manage the server, index your code, and interact with the code graph.

## `cgc setup`

Runs the interactive setup wizard to configure the server and database connection. This helps users set up a local Docker-based Neo4j instance or connect to a remote one.

**Usage:**
```bash
cgc setup
```

## `cgc start`

Starts the CodeGraphContext MCP server, which listens for JSON-RPC requests from stdin.

**Usage:**
```bash
cgc start
```

## `cgc index [PATH]`

Indexes a directory or file by adding it to the code graph. If no path is provided, it indexes the current directory.

**Arguments:**
*   `PATH` (optional): Path to the directory or file to index. Defaults to the current directory.

**Usage:**
```bash
cgc index /path/to/your/project
```

### Ignoring Files (`.cgcignore`)

You can tell CodeGraphContext to ignore specific files and directories by creating a `.cgcignore` file in the root of your project. This file uses the same syntax as `.gitignore`.

When you run `cgc index`, the command will look for a `.cgcignore` file in the directory being indexed and exclude any files or directories that match the patterns in the file.

**Example `.cgcignore` file:**
```
# Ignore build artifacts
/build/
/dist/

# Ignore dependencies
/node_modules/
/vendor/

# Ignore logs
*.log
```


## `cgc delete <PATH>`

Deletes a repository from the code graph.

**Arguments:**
*   `PATH` (required): Path of the repository to delete from the code graph.

**Usage:**
```bash
cgc delete /path/to/your/project
```

## `cgc visualize [QUERY]`

Generates a URL to visualize a Cypher query in the Neo4j Browser. If no query is provided, a default query will be used.

**Arguments:**
*   `QUERY` (optional): The Cypher query to visualize.

**Usage:**
```bash
cgc visualize "MATCH (n) RETURN n"
```

## `cgc list_repos`

Lists all indexed repositories.

**Usage:**
```bash
cgc list_repos
```

## `cgc add_package <PACKAGE_NAME>`

Adds a Python package to the code graph.

**Arguments:**
*   `PACKAGE_NAME` (required): Name of the Python package to add.

**Usage:**
```bash
cgc add_package requests
```

## `cgc cypher <QUERY>`

Executes a read-only Cypher query.

**Arguments:**
*   `QUERY` (required): The read-only Cypher query to execute.

**Usage:**
```bash
cgc cypher "MATCH (n:Function) RETURN n.name"
```

## `cgc list_mcp_tools`

Lists all available tools and their descriptions.

**Usage:**
```bash
cgc list_mcp_tools
```

## `cgc help`

Show the main help message and exit.

**Usage:**
```bash
cgc help
```

## `cgc version`

Show the application version.

**Usage:**
```bash
cgc --version
```
or
```bash
cgc version
```