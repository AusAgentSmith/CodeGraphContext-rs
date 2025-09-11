# src/codegraphcontext/cli/main.py
import typer
from rich.console import Console
import asyncio
import logging
import json
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from codegraphcontext.server import MCPServer
from .setup_wizard import run_setup_wizard

# Set the log level for the noisy neo4j logger to WARNING
logging.getLogger("neo4j").setLevel(logging.WARNING) # <-- ADD THIS LINE

app = typer.Typer(
    name="cgc",
    help="CodeGraphContext: An MCP server for AI-powered code analysis.",
    add_completion=False,
)
console = Console()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

@app.command()
def setup():
    """
    Run the interactive setup wizard to configure the server and database.
    """
    run_setup_wizard()

@app.command()
def start():
    """
    Start the CodeGraphContext MCP server.
    """
    console.print("[bold green]Starting CodeGraphContext Server...[/bold green]")
    
    # 1. Prefer loading environment variables from mcp.json in the current directory
    mcp_file_path = Path.cwd() / "mcp.json"
    if mcp_file_path.exists():
        try:
            with open(mcp_file_path, "r") as f:
                mcp_config = json.load(f)
            
            server_env = mcp_config.get("mcpServers", {}).get("CodeGraphContext", {}).get("env", {})
            for key, value in server_env.items():
                os.environ[key] = value
            console.print("[green]Loaded Neo4j credentials from local mcp.json.[/green]")
        except Exception as e:
            console.print(f"[bold red]Error loading mcp.json:[/bold red] {e}")
            console.print("[yellow]Attempting to start server without mcp.json environment variables.[/yellow]")
    else:
        # 2. If no local mcp.json, try to load from ~/.codegraphcontext/.env
        global_env_path = Path.home() / ".codegraphcontext" / ".env"
        if global_env_path.exists():
            try:
                load_dotenv(dotenv_path=global_env_path)
                console.print(f"[green]Loaded Neo4j credentials from global .env file: {global_env_path}[/green]")
            except Exception as e:
                console.print(f"[bold red]Error loading global .env file from {global_env_path}:[/bold red] {e}")
                console.print("[yellow]Attempting to start server without .env environment variables.[/yellow]")
        else:
            # Fallback: try to load from a .env file found by find_dotenv (searches up the tree)
            try:
                dotenv_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
                if dotenv_path:
                    load_dotenv(dotenv_path)
                    console.print(f"[green]Loaded Neo4j credentials from global .env file: {dotenv_path}[/green]")
                else:
                    console.print("[yellow]No local mcp.json or global .env file found. Attempting to start server without explicit Neo4j credentials.[/yellow]")
            except Exception as e:
                console.print(f"[bold red]Error loading global .env file:[/bold red] {e}")
                console.print("[yellow]Attempting to start server without .env environment variables.[/yellow]")

    server = None
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        server = MCPServer(loop=loop)
        loop.run_until_complete(server.run())
    except ValueError as e:
        console.print(f"[bold red]Configuration Error:[/bold red] {e}")
        console.print("Please run `cgc setup` to configure the server.")
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Server stopped by user.[/bold yellow]")
    finally:
        if server:
            server.shutdown()
        loop.close()


@app.command()
def tool(
    name: str = typer.Argument(..., help="The name of the tool to call."),
    args: str = typer.Argument("{}", help="A JSON string of arguments for the tool."),
):
    """
    Directly call a CodeGraphContext tool.
    Note: This command instantiates a new, independent MCP server instance for each call.
    Therefore, it does not share state (like job IDs) with a server started via `cgc start`.

    This command can be used for:\n
    - `add_code_to_graph`: Index a new project or directory. Args: `path` (str), `is_dependency` (bool, optional)\n
    - `add_package_to_graph`: Add a Python package to the graph. Args: `package_name` (str), `is_dependency` (bool, optional)\n
    - `find_code`: Search for code snippets. Args: `query` (str)\n
    - `analyze_code_relationships`: Analyze code relationships (e.g., callers, callees). Args: `query_type` (str), `target` (str), `context` (str, optional)\n
    - `watch_directory`: Start watching a directory for changes. Args: `path` (str)\n
    - `execute_cypher_query`: Run direct Cypher queries. Args: `cypher_query` (str)\n
    - `list_imports`: List imports from files. Args: `path` (str), `language` (str, optional), `recursive` (bool, optional)\n
    - `find_dead_code`: Find potentially unused functions. Args: None\n
    - `calculate_cyclomatic_complexity`: Calculate function complexity. Args: `function_name` (str), `file_path` (str, optional)\n
    - `find_most_complex_functions`: Find the most complex functions. Args: `limit` (int, optional)\n
    - `list_indexed_repositories`: List indexed repositories. Args: None\n
    - `delete_repository`: Delete an indexed repository. Args: `repo_path` (str)
    """

    # This is a placeholder for a more advanced tool caller that would
    # connect to the running server via a different mechanism (e.g., a socket).
    # For now, it's a conceptual part of the CLI.
    console.print(f"Calling tool [bold cyan]{name}[/bold cyan] with args: {args}")
    console.print("[yellow]Note: This is a placeholder for direct tool invocation.[/yellow]")

@app.command()
def help(ctx: typer.Context):
    """Show this message and exit."""
    root_ctx = ctx.parent or ctx
    typer.echo(root_ctx.get_help())


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """
    CodeGraphContext: An MCP server for AI-powered code analysis.
    """
    if ctx.invoked_subcommand is None:
        console.print("[bold green]👋 Welcome to CodeGraphContext (cgc)![/bold green]\n")
        console.print("👉 Run [cyan]cgc setup[/cyan] to configure the server and database.")
        console.print("👉 Run [cyan]cgc start[/cyan] to launch the server.")
        console.print("👉 Run [cyan]cgc help[/cyan] to see all available commands.\n")
        console.print("👉 Running [green]codegraphcontext [white]works the same as using [green]cgc")
