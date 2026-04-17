"""All graph DB writes for indexing (single persistence entry point).

All write paths delegate to the Rust-side `GraphWriter` backed by
`neo4rs`; the Python `neo4j` driver is retained only for reads and for
the delete queries used during repo teardown. Requires the
`codegraphcontext._cgc_rust` extension to be built and `NEO4J_URI` /
`NEO4J_PASSWORD` / (optional) `NEO4J_USERNAME` / `NEO4J_DATABASE` set
in the environment; absent either, `GraphWriter()` raises.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ....utils.debug_log import info_logger, warning_logger

try:
    from codegraphcontext._cgc_rust import GraphWriter as _RustGraphWriter
except ImportError as e:  # pragma: no cover - only hit when the wheel failed to build
    raise ImportError(
        "codegraphcontext._cgc_rust is not available. Build the Rust "
        "extension with `maturin develop --release` before using the "
        "indexer."
    ) from e


def _init_rust_writer() -> Optional[Any]:
    """Construct the Rust-backed writer from env, or None if env is absent.

    Returns None when `NEO4J_URI` / `NEO4J_PASSWORD` are not set — this
    lets read / delete tests instantiate the writer without a live DB.
    Any write call on such an instance will fail loudly with
    AttributeError on the missing `_rust` attribute use.

    If env is set but the driver fails to connect (bad URI, wrong
    credentials, unreachable host), the `_RustGraphWriter` constructor's
    ping check raises — propagated here so the caller sees a clear
    failure at construction time rather than mid-index.
    """
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE")
    if not uri or not password:
        return None
    return _RustGraphWriter(uri, user, password, database)


class GraphWriter:
    """Persists repository/file/symbol nodes and relationships.

    Writes run through the Rust `neo4rs` driver (self._rust). The Python
    `neo4j` driver (self.driver) is retained for:
      * reads issued by downstream tools (code_finder.py, etc.)
      * delete queries used by watchers / repo teardown
      * the minimal fallback node written for files that fail to parse
    """

    def __init__(self, driver: Any):
        self.driver = driver
        self._rust = _init_rust_writer()

    # ── Write API (Rust-backed) ──────────────────────────────────────────

    def add_repository_to_graph(self, repo_path: Path, is_dependency: bool = False) -> None:
        self._rust.add_repository(
            str(repo_path.resolve()), repo_path.name, is_dependency,
        )

    def add_file_to_graph(
        self,
        file_data: Dict[str, Any],
        repo_name: str,
        imports_map: dict,
        repo_path_str: Optional[str] = None,
    ) -> None:
        """Single-file shim over add_files_batch_to_graph.

        Used by pipeline.py for files that fall outside the Rust parser's
        supported set (e.g. .ipynb) and by the watcher on incremental
        updates.
        """
        if repo_path_str is None:
            repo_path_str = str(Path(file_data.get("repo_path", Path(file_data["path"]).parent)).resolve())
        self.add_files_batch_to_graph(
            [file_data], repo_name, imports_map, repo_path_str,
        )

    def add_files_batch_to_graph(
        self,
        all_file_data: List[Dict[str, Any]],
        repo_name: str,
        imports_map: dict,
        repo_path_str: str,
        on_progress: Optional[callable] = None,
    ) -> int:
        """Write all parsed files to the graph via the Rust writer.

        `imports_map` and `repo_name` are kept in the signature for
        compatibility with the pipeline; the Rust writer derives what
        it needs from the per-file dicts plus `repo_path_str`.
        """
        total = len(all_file_data)
        if total == 0:
            return 0
        if on_progress:
            on_progress(0, total, "Writing files to graph...")
        self._rust.write_file_tree(all_file_data, repo_path_str)
        if on_progress:
            on_progress(total // 2, total, "Writing symbols...")
        self._rust.write_symbols(all_file_data)
        if on_progress:
            on_progress(total * 3 // 4, total, "Writing relationships...")
        self._rust.write_function_edges(all_file_data)
        self._rust.write_imports(all_file_data)
        self._rust.write_impls(all_file_data)
        self._rust.write_decorators(all_file_data)
        if on_progress:
            on_progress(total, total, "Graph write complete")
        return total

    def write_inheritance_links(
        self,
        inheritance_batch: List[Dict[str, Any]],
    ) -> None:
        info_logger(
            f"[INHERITS] Resolved {len(inheritance_batch)} inheritance links. Writing to Neo4j..."
        )
        self._rust.write_inheritance(inheritance_batch)
        info_logger(
            f"[INHERITS] Complete: {len(inheritance_batch)} inheritance links processed."
        )

    def write_function_call_groups(
        self,
        fn_to_fn: List[Dict],
        fn_to_cls: List[Dict],
        cls_to_fn: List[Dict],
        cls_to_cls: List[Dict],
        file_to_fn: List[Dict],
        file_to_cls: List[Dict],
    ) -> None:
        total_all = (
            len(fn_to_fn) + len(fn_to_cls) + len(cls_to_fn)
            + len(cls_to_cls) + len(file_to_fn) + len(file_to_cls)
        )
        self._rust.write_call_groups(
            fn_to_fn, fn_to_cls, cls_to_fn, cls_to_cls, file_to_fn, file_to_cls,
        )
        info_logger(f"[CALLS] All complete: {total_all} CALLS relationships processed.")

    # ── Minimal fallback node (Python driver — parser error path) ────────

    def add_minimal_file_node(
        self, file_path: Path, repo_path: Path, is_dependency: bool = False
    ) -> None:
        """Write a bare File + directory chain for files that failed to parse.

        This is a cold path that runs once per parse error, so it stays on
        the Python driver rather than adding a PyO3 entry point.
        """
        file_path_str = str(file_path.resolve())
        file_name = file_path.name
        repo_name = repo_path.name
        repo_path_str = str(repo_path.resolve())

        with self.driver.session() as session:
            session.run(
                "MERGE (r:Repository {path: $repo_path}) SET r.name = $repo_name",
                repo_path=repo_path_str,
                repo_name=repo_name,
            )
            session.run(
                "MERGE (f:File {path: $file_path}) "
                "SET f.name = $file_name, f.is_dependency = $is_dependency",
                file_path=file_path_str,
                file_name=file_name,
                is_dependency=is_dependency,
            )

            try:
                relative_path_to_file = Path(file_path_str).relative_to(Path(repo_path_str))
            except ValueError:
                relative_path_to_file = Path(file_name)

            parent_path = repo_path_str
            parent_label = "Repository"
            for part in relative_path_to_file.parts[:-1]:
                current_path_str = str(Path(parent_path) / part)
                session.run(
                    f"MATCH (p:{parent_label} {{path: $parent_path}}) "
                    f"MERGE (d:Directory {{path: $current_path}}) "
                    f"SET d.name = $part "
                    f"MERGE (p)-[:CONTAINS]->(d)",
                    parent_path=parent_path,
                    current_path=current_path_str,
                    part=part,
                )
                parent_path = current_path_str
                parent_label = "Directory"

            session.run(
                f"MATCH (p:{parent_label} {{path: $parent_path}}) "
                f"MATCH (f:File {{path: $file_path}}) "
                f"MERGE (p)-[:CONTAINS]->(f)",
                parent_path=parent_path,
                file_path=file_path_str,
            )

    # ── Delete / read helpers (Python driver) ───────────────────────────

    def delete_file_from_graph(self, path: str) -> None:
        file_path_str = str(Path(path).resolve())
        with self.driver.session() as session:
            parents_res = session.run(
                "MATCH (f:File {path: $path})<-[:CONTAINS*]-(d:Directory) "
                "RETURN d.path as path ORDER BY d.path DESC",
                path=file_path_str,
            )
            parent_paths = [record["path"] for record in parents_res]

            session.run(
                "MATCH (f:File {path: $path}) "
                "OPTIONAL MATCH (f)-[:CONTAINS]->(element) "
                "DETACH DELETE f, element",
                path=file_path_str,
            )
            info_logger(f"Deleted file and its elements from graph: {file_path_str}")

            for p in parent_paths:
                session.run(
                    "MATCH (d:Directory {path: $path}) "
                    "WHERE NOT (d)-[:CONTAINS]->() "
                    "DETACH DELETE d",
                    path=p,
                )

    def delete_repository_from_graph(self, repo_path: str) -> bool:
        repo_path_str = str(Path(repo_path).resolve())
        path_prefix = repo_path_str + "/"
        with self.driver.session() as session:
            result = session.run(
                "MATCH (r:Repository {path: $path}) RETURN count(r) as cnt", path=repo_path_str
            ).single()
            if not result or result["cnt"] == 0:
                warning_logger(f"Attempted to delete non-existent repository: {repo_path_str}")
                return False

        for rel_type in ("CALLS", "INHERITS", "IMPORTS"):
            while True:
                with self.driver.session() as session:
                    result = session.run(
                        f"MATCH (a)-[r:{rel_type}]->(b) "
                        "WHERE a.path STARTS WITH $prefix OR b.path STARTS WITH $prefix "
                        "WITH r LIMIT 5000 DELETE r RETURN count(r) AS deleted",
                        prefix=path_prefix,
                    ).single()
                    deleted = result["deleted"] if result else 0
                if deleted == 0:
                    break
                info_logger(f"[DELETE] Removed {deleted} {rel_type} rels for {repo_path_str}")

        while True:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (a)-[r:CONTAINS]->(b) "
                    "WHERE a.path STARTS WITH $prefix OR a.path = $path "
                    "WITH r LIMIT 10000 DELETE r RETURN count(r) AS deleted",
                    prefix=path_prefix,
                    path=repo_path_str,
                ).single()
                deleted = result["deleted"] if result else 0
            if deleted == 0:
                break
            info_logger(f"[DELETE] Removed {deleted} CONTAINS rels for {repo_path_str}")

        for label in ("Function", "Class", "File"):
            while True:
                with self.driver.session() as session:
                    result = session.run(
                        f"MATCH (n:{label}) WHERE n.path STARTS WITH $prefix "
                        "WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS deleted",
                        prefix=path_prefix,
                    ).single()
                    deleted = result["deleted"] if result else 0
                if deleted == 0:
                    break
                info_logger(f"[DELETE] Removed {deleted} {label} nodes for {repo_path_str}")

        with self.driver.session() as session:
            session.run("MATCH (r:Repository {path: $path}) DETACH DELETE r", path=repo_path_str)

        info_logger(f"Deleted repository and its contents from graph: {repo_path_str}")
        return True

    def get_caller_file_paths(self, file_path_str: str) -> set:
        with self.driver.session() as session:
            result = session.run(
                "MATCH (caller)-[:CALLS]->(callee) "
                "WHERE callee.path = $path "
                "RETURN DISTINCT coalesce(caller.path, '') AS p",
                path=file_path_str,
            )
            return {r["p"] for r in result if r["p"] and r["p"] != file_path_str}

    def get_inheritance_neighbor_paths(self, file_path_str: str) -> set:
        with self.driver.session() as session:
            result = session.run(
                "MATCH (a)-[:INHERITS]->(b) "
                "WHERE a.path = $path OR b.path = $path "
                "RETURN DISTINCT CASE WHEN a.path = $path THEN b.path ELSE a.path END AS p",
                path=file_path_str,
            )
            return {r["p"] for r in result if r["p"] and r["p"] != file_path_str}

    def delete_outgoing_calls_from_files(self, file_paths: List[str]) -> None:
        with self.driver.session() as session:
            result = session.run(
                "MATCH (a)-[r:CALLS]->(b) WHERE a.path IN $paths DELETE r RETURN count(r) AS cnt",
                paths=file_paths,
            ).single()
            cnt = result["cnt"] if result else 0
        info_logger(f"[RELINK] Deleted {cnt} outgoing CALLS from {len(file_paths)} caller files")

    def delete_inherits_for_files(self, file_paths: List[str]) -> None:
        with self.driver.session() as session:
            result = session.run(
                "MATCH (a)-[r:INHERITS]->(b) WHERE a.path IN $paths OR b.path IN $paths "
                "DELETE r RETURN count(r) AS cnt",
                paths=file_paths,
            ).single()
            cnt = result["cnt"] if result else 0
        info_logger(f"[RELINK] Deleted {cnt} INHERITS for {len(file_paths)} affected files")

    def get_repo_class_lookup(self, repo_path: Path) -> Dict[str, set]:
        prefix = str(repo_path.resolve()) + "/"
        result_map: Dict[str, set] = {}
        with self.driver.session() as session:
            result = session.run(
                "MATCH (c:Class) WHERE c.path STARTS WITH $prefix "
                "RETURN c.name AS name, c.path AS path",
                prefix=prefix,
            )
            for record in result:
                path = record["path"]
                if path not in result_map:
                    result_map[path] = set()
                result_map[path].add(record["name"])
        return result_map

    def delete_relationship_links(self, repo_path: Path) -> None:
        repo_path_str = str(repo_path.resolve()) + "/"
        with self.driver.session() as session:
            result = session.run(
                "MATCH (a)-[r:CALLS]->(b) WHERE a.path STARTS WITH $prefix DELETE r RETURN count(r) AS cnt",
                prefix=repo_path_str,
            ).single()
            calls_deleted = result["cnt"] if result else 0

            result = session.run(
                "MATCH (a)-[r:INHERITS]->(b) WHERE a.path STARTS WITH $prefix DELETE r RETURN count(r) AS cnt",
                prefix=repo_path_str,
            ).single()
            inherits_deleted = result["cnt"] if result else 0

        info_logger(
            f"[RELINK] Cleared {calls_deleted} CALLS and {inherits_deleted} INHERITS before re-linking: {repo_path}"
        )
