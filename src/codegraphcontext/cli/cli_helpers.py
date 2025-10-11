# src/codegraphcontext/cli/cli_helpers.py
import asyncio
import json
import urllib.parse
from pathlib import Path
import time
from rich.console import Console
from rich.table import Table

from ..core.database import DatabaseManager
from ..core.jobs import JobManager
from ..tools.code_finder import CodeFinder
from ..tools.graph_builder import GraphBuilder
from ..tools.package_resolver import get_local_package_path

console = Console()


def _initialize_services():
    """Initializes and returns core service managers."""
    console.print("[dim]Initializing services and database connection...[/dim]")
    db_manager = DatabaseManager()
    try:
        db_manager.get_driver()
    except ValueError as e:
        console.print(f"[bold red]Database Connection Error:[/bold red] {e}")
        console.print("Please ensure your Neo4j credentials are correct and the database is running.")
        return None, None, None
    
    # The GraphBuilder requires an event loop, even for synchronous-style execution
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    graph_builder = GraphBuilder(db_manager, JobManager(), loop)
    code_finder = CodeFinder(db_manager)
    console.print("[dim]Services initialized.[/dim]")
    return db_manager, graph_builder, code_finder


def index_helper(path: str):
    """Synchronously indexes a repository."""
    time_start = time.time()
    services = _initialize_services()
    if not all(services):
        return

    db_manager, graph_builder, code_finder = services
    path_obj = Path(path).resolve()

    if not path_obj.exists():
        console.print(f"[red]Error: Path does not exist: {path_obj}[/red]")
        db_manager.close_driver()
        return

    indexed_repos = code_finder.list_indexed_repositories()
    if any(Path(repo["path"]).resolve() == path_obj for repo in indexed_repos):
        console.print(f"[yellow]Repository '{path}' is already indexed. Skipping.[/yellow]")
        db_manager.close_driver()
        return

    console.print(f"Starting indexing for: {path_obj}")
    console.print("[yellow]This may take a few minutes for large repositories...[/yellow]")

    async def do_index():
        await graph_builder.build_graph_from_path_async(path_obj, is_dependency=False)

    try:
        asyncio.run(do_index())
        time_end = time.time()
        elapsed = time_end - time_start
        console.print(f"[green]Successfully finished indexing: {path} in {elapsed:.2f} seconds[/green]")
    except Exception as e:
        console.print(f"[bold red]An error occurred during indexing:[/bold red] {e}")
    finally:
        db_manager.close_driver()


def add_package_helper(package_name: str, language: str):
    """Synchronously indexes a package."""
    services = _initialize_services()
    if not all(services):
        return

    db_manager, graph_builder, code_finder = services

    package_path_str = get_local_package_path(package_name, language)
    if not package_path_str:
        console.print(f"[red]Error: Could not find package '{package_name}' for language '{language}'.[/red]")
        db_manager.close_driver()
        return

    package_path = Path(package_path_str)
    
    indexed_repos = code_finder.list_indexed_repositories()
    if any(repo.get("name") == package_name for repo in indexed_repos if repo.get("is_dependency")):
        console.print(f"[yellow]Package '{package_name}' is already indexed. Skipping.[/yellow]")
        db_manager.close_driver()
        return

    console.print(f"Starting indexing for package '{package_name}' at: {package_path}")
    console.print("[yellow]This may take a few minutes...[/yellow]")

    async def do_index():
        await graph_builder.build_graph_from_path_async(package_path, is_dependency=True)

    try:
        asyncio.run(do_index())
        console.print(f"[green]Successfully finished indexing package: {package_name}[/green]")
    except Exception as e:
        console.print(f"[bold red]An error occurred during package indexing:[/bold red] {e}")
    finally:
        db_manager.close_driver()


def list_repos_helper():
    """Lists all indexed repositories."""
    services = _initialize_services()
    if not all(services):
        return
    
    db_manager, _, code_finder = services
    
    try:
        repos = code_finder.list_indexed_repositories()
        if not repos:
            console.print("[yellow]No repositories indexed yet.[/yellow]")
            return

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Name", style="dim")
        table.add_column("Path")
        table.add_column("Type")

        for repo in repos:
            repo_type = "Dependency" if repo.get("is_dependency") else "Project"
            table.add_row(repo["name"], repo["path"], repo_type)
        
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]An error occurred:[/bold red] {e}")
    finally:
        db_manager.close_driver()


def delete_helper(repo_path: str):
    """Deletes a repository from the graph."""
    services = _initialize_services()
    if not all(services):
        return

    db_manager, graph_builder, _ = services
    
    try:
        graph_builder.delete_repository_from_graph(repo_path)
        console.print(f"[green]Successfully deleted repository: {repo_path}[/green]")
    except Exception as e:
        console.print(f"[bold red]An error occurred:[/bold red] {e}")
    finally:
        db_manager.close_driver()


def cypher_helper(query: str):
    """Executes a read-only Cypher query."""
    services = _initialize_services()
    if not all(services):
        return

    db_manager, _, _ = services
    
    # Replicating safety checks from MCPServer
    forbidden_keywords = ['CREATE', 'MERGE', 'DELETE', 'SET', 'REMOVE', 'DROP', 'CALL apoc']
    if any(keyword in query.upper() for keyword in forbidden_keywords):
        console.print("[bold red]Error: This command only supports read-only queries.[/bold red]")
        db_manager.close_driver()
        return

    try:
        with db_manager.get_driver().session() as session:
            result = session.run(query)
            records = [record.data() for record in result]
            console.print(json.dumps(records, indent=2))
    except Exception as e:
        console.print(f"[bold red]An error occurred while executing query:[/bold red] {e}")
    finally:
        db_manager.close_driver()


def visualize_helper(query: str):
    """Generates a URL to visualize a Cypher query."""
    try:
        encoded_query = urllib.parse.quote(query)
        visualization_url = f"http://localhost:7474/browser/?cmd=edit&arg={encoded_query}"
        console.print("[green]Graph visualization URL:[/green]")
        console.print(visualization_url)
        console.print("Open the URL in your browser to see the graph.")
    except Exception as e:
        console.print(f"[bold red]An error occurred while generating URL:[/bold red] {e}")

