# CodeGraphContext
[![Build Status](https://github.com/Shashankss1205/CodeGraphContext/actions/workflows/test.yml/badge.svg)](https://github.com/Shashankss1205/CodeGraphContext/actions/workflows/test.yml)
[![PyPI version](https://img.shields.io/pypi/v/codegraphcontext)](https://pypi.org/project/codegraphcontext/)
[![PyPI downloads](https://img.shields.io/pypi/dm/codegraphcontext)](https://pypi.org/project/codegraphcontext/)
[![GitHub stars](https://img.shields.io/github/stars/Shashankss1205/CodeGraphContext?style=social)](https://github.com/Shashankss1205/CodeGraphContext/stargazers)
[![License](https://img.shields.io/github/license/Shashankss1205/CodeGraphContext)](LICENSE)

An MCP server that indexes local code into a graph database to provide context to AI assistants.

## Project Details
- **Version:** 0.1.10
- **Authors:** Shashank Shekhar Singh <shashankshekharsingh1205@gmail.com>
- **License:** MIT License (See [LICENSE](LICENSE) for details)
- **Website:** [CodeGraphContext](http://code-graph-context.vercel.app/)

## Features

-   **Code Indexing:** Analyzes Python code and builds a knowledge graph of its components.
-   **Relationship Analysis:** Query for callers, callees, class hierarchies, and more.
-   **Live Updates:** Watches local files for changes and automatically updates the graph.
-   **Interactive Setup:** A user-friendly command-line wizard for easy setup.

## Used By

CodeGraphContext is already being explored by developers and projects for:

- **Static code analysis in AI assistants**
- **Graph-based visualization of Python projects**
- **Dead code and complexity detection**

If you’re using CodeGraphContext in your project, feel free to open a PR and add it here! 🚀

## Dependencies

- `neo4j>=5.15.0`
- `watchdog>=3.0.0`
- `requests>=2.31.0`
- `stdlibs>=2023.11.18`
- `typer[all]>=0.9.0`
- `rich>=13.7.0`
- `inquirerpy>=0.3.4`
- `python-dotenv>=1.0.0`

## Getting Started

1.  **Install:** `pip install codegraphcontext`
2.  **Setup:** `cgc setup`
    This interactive command guides you through configuring your Neo4j database connection. It offers several options:
    *   **Local Setup (Docker Recommended):** Helps you set up a local Neo4j instance using Docker, which is the easiest way to get started.
    *   **Local Setup (Linux Binary):** For Debian-based Linux systems (like Ubuntu), `cgc setup` can automate the installation of Neo4j directly on your machine.
    *   **Hosted Setup:** Allows you to connect to an existing remote Neo4j database (e.g., Neo4j AuraDB).
    Upon successful configuration, `cgc setup` will generate two important files:
    *   `mcp.json`: Contains the MCP client configuration for CodeGraphContext.
    *   `~/.codegraphcontext/.env`: Stores your Neo4j connection credentials securely.
3.  **Start:** `cgc start`


## MCP Client Configuration

Add the following to your MCP client's configuration:

```json
{
  "mcpServers": {
    "CodeGraphContext": {
      "command": "cgc",
      "args": [
        "start"
      ],
      "env": {
        "NEO4J_URI": "************",
        "NEO4J_USER": "************",
        "NEO4J_PASSWORD": "**************"
      },
      "tools": {
        "alwaysAllow": [
          "list_imports",
          "add_code_to_graph",
          "add_package_to_graph",
          "check_job_status",
          "list_jobs",
          "find_code",
          "analyze_code_relationships",
          "watch_directory",
          "find_dead_code",
          "execute_cypher_query",
          "calculate_cyclomatic_complexity",
          "find_most_complex_functions",
          "list_indexed_repositories",
          "delete_repository"
        ],
        "disabled": false
      },
      "disabled": false,
      "alwaysAllow": []
    }
  }
}
```

## Natural Language Interaction Examples

Once the server is running, you can interact with it through your AI assistant using plain English. Here are some examples of what you can say:

### Indexing and Watching Files

-   **To index a new project:**
    -   "Please index the code in the `/path/to/my-project` directory."
    OR
    -   "Add the project at `~/dev/my-other-project` to the code graph."


-   **To start watching a directory for live changes:**
    -   "Watch the `/path/to/my-active-project` directory for changes."
    OR
    -   "Keep the code graph updated for the project I'm working on at `~/dev/main-app`."

    When you ask to watch a directory, the system performs two actions at once:
    1.  It kicks off a full scan to index all the code in that directory. This process runs in the background, and you'll receive a `job_id` to track its progress.
    2.  It begins watching the directory for any file changes to keep the graph updated in real-time.

    This means you can start by simply telling the system to watch a directory, and it will handle both the initial indexing and the continuous updates automatically.

### Querying and Understanding Code

-   **Finding where code is defined:**
    -   "Where is the `process_payment` function?"
    -   "Find the `User` class for me."
    -   "Show me any code related to 'database connection'."

-   **Analyzing relationships and impact:**
    -   "What other functions call the `get_user_by_id` function?"
    -   "If I change the `calculate_tax` function, what other parts of the code will be affected?"
    -   "Show me the inheritance hierarchy for the `BaseController` class."
    -   "What methods does the `Order` class have?"

-   **Exploring dependencies:**
    -   "Which files import the `requests` library?"
    -   "Find all implementations of the `render` method."

-   **Advanced Call Chain and Dependency Tracking (Spanning Hundreds of Files):**
    The CodeGraphContext excels at tracing complex execution flows and dependencies across vast codebases. Leveraging the power of graph databases, it can identify direct and indirect callers and callees, even when a function is called through multiple layers of abstraction or across numerous files. This is invaluable for:
    -   **Impact Analysis:** Understand the full ripple effect of a change to a core function.
    -   **Debugging:** Trace the path of execution from an entry point to a specific bug.
    -   **Code Comprehension:** Grasp how different parts of a large system interact.

    -   "Show me the full call chain from the `main` function to `process_data`."
    -   "Find all functions that directly or indirectly call `validate_input`."
    -   "What are all the functions that `initialize_system` eventually calls?"
    -   "Trace the dependencies of the `DatabaseManager` module."

-   **Code Quality and Maintenance:**
    -   "Is there any dead or unused code in this project?"
    -   "Calculate the cyclomatic complexity of the `process_data` function in `src/utils.py`."
    -   "Find the 5 most complex functions in the codebase."

-   **Repository Management:**
    -   "List all currently indexed repositories."
    -   "Delete the indexed repository at `/path/to/old-project`."

## Contributing

Contributions are welcome! 🎉  
Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.
If you have ideas for new features, integrations, or improvements, open an [issue](https://github.com/Shashankss1205/CodeGraphContext/issues) or submit a PR.

Join discussions and help shape the future of CodeGraphContext.
