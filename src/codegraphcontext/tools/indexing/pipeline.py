"""Orchestrates full-repo indexing (Tree-sitter path)."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ...core.jobs import JobManager, JobStatus
from ...utils.debug_log import debug_log, error_logger, info_logger
from .discovery import discover_files_to_index
from .persistence.writer import GraphWriter
from .pre_scan import pre_scan_for_imports
from .resolution.calls import build_function_call_groups
from .resolution.inheritance import build_inheritance


# Phase-marker output is always on except when CGC is running as an
# MCP server (stdout is JSON-RPC; stderr is available but noisy for
# the client). The MCP entry sets `CGC_QUIET_PROGRESS=1`.
_QUIET = os.environ.get("CGC_QUIET_PROGRESS") == "1"


def _phase(start: float, msg: str) -> None:
    """Print a progress marker to stderr, prefixed with elapsed-seconds-since-start.

    Stderr because stdout is reserved for MCP's JSON-RPC when cgc runs
    as a server; this output only shows up on the CLI.
    """
    if _QUIET:
        return
    elapsed = time.time() - start
    print(f"  [{elapsed:6.1f}s] {msg}", file=sys.stderr, flush=True)

# Try to load Rust engine for accelerated parsing
try:
    from .engine import (
        RUST_AVAILABLE,
        _RUST_SUPPORTED_LANGS,
        _rust_parse_files_parallel,
        _rust_pre_scan,
        _rust_parse_and_prescan,
        _rust_resolve_calls,
        _rust_resolve_inheritance,
    )
except ImportError:
    RUST_AVAILABLE = False


async def run_tree_sitter_index_async(
    path: Path,
    is_dependency: bool,
    job_id: Optional[str],
    cgcignore_path: Optional[str],
    writer: GraphWriter,
    job_manager: JobManager,
    parsers: Dict[str, str],
    get_parser: Callable[[str], Any],
    parse_file: Callable[[Path, Path, bool], Dict[str, Any]],
    add_minimal_file_node: Callable[[Path, Path, bool], None],
) -> None:
    """Parse all discovered files, write symbols, then inheritance + CALLS.

    When the Rust engine is available, files with supported languages are
    parsed in parallel via Rust for significant speedup. Unsupported
    languages (e.g. Kotlin, Perl) fall back to the Python parser.
    """
    if job_id:
        job_manager.update_job(job_id, status=JobStatus.RUNNING)

    t_start = time.time()
    _phase(t_start, f"discovering files in {path}")

    writer.add_repository_to_graph(path, is_dependency)
    repo_name = path.name

    files, _ignore_root = discover_files_to_index(path, cgcignore_path)
    _phase(t_start, f"discovered {len(files)} files")

    if job_id:
        job_manager.update_job(job_id, total_files=len(files))

    all_file_data: List[Dict[str, Any]] = []
    resolved_repo_path_str = str(path.resolve()) if path.is_dir() else str(path.parent.resolve())
    repo_path_resolved = path.resolve() if path.is_dir() else path.parent.resolve()

    # --- Combined pre-scan + parsing phase ---
    if RUST_AVAILABLE:
        # Split files into Rust-supported and fallback. Unsupported
        # extensions (YAML, TypeScript, TOML, ...) are dropped entirely:
        # we used to fall through to add_minimal_file_node for them,
        # which issues 3-5 synchronous Cypher queries *per file* and
        # turned a 3k-Rust-file repo into a 16-minute indexing job
        # because the repo also contained ~10k asset files.
        rust_files = []
        fallback_files = []
        for file in files:
            if not file.is_file():
                continue
            lang = parsers.get(file.suffix)
            if lang is None:
                continue
            if lang in _RUST_SUPPORTED_LANGS and file.suffix != ".ipynb":
                rust_files.append((file, lang))
            else:
                fallback_files.append(file)

        # Combined parse + pre-scan in one parallel pass (saves ~12% time)
        imports_map = {}
        if rust_files:
            _phase(t_start, f"parsing {len(rust_files)} files (Rust)")
            t_parse = time.time()
            specs = [(str(f), lang, is_dependency) for f, lang in rust_files]
            rust_results, imports_map = _rust_parse_and_prescan(specs)
            _phase(t_start,
                   f"parsed in {time.time() - t_parse:.1f}s ({len(imports_map)} symbols)")

            valid_rust_results = []
            for file_data in rust_results:
                if "error" not in file_data:
                    file_data["repo_path"] = str(repo_path_resolved)
                    valid_rust_results.append(file_data)

            if valid_rust_results:
                _phase(t_start, f"writing {len(valid_rust_results)} files to graph")
                writer.add_files_batch_to_graph(
                    valid_rust_results, repo_name, imports_map,
                    repo_path_str=resolved_repo_path_str,
                    on_progress=lambda _done, _total, msg: _phase(t_start, f"  {msg}"),
                )
                all_file_data.extend(valid_rust_results)

            if job_id:
                job_manager.update_job(job_id, processed_files=len(rust_files))

        # Fallback for unsupported files (.ipynb, Perl, etc.)
        processed_count = len(rust_files)
        for file in fallback_files:
            if job_id:
                job_manager.update_job(job_id, current_file=str(file))
            file_data = parse_file(repo_path_resolved, file, is_dependency)
            if "error" not in file_data:
                writer.add_file_to_graph(file_data, repo_name, imports_map, repo_path_str=resolved_repo_path_str)
                all_file_data.append(file_data)
            elif not file_data.get("unsupported"):
                add_minimal_file_node(file, repo_path_resolved, is_dependency)
            processed_count += 1

            if job_id:
                job_manager.update_job(job_id, processed_files=processed_count)
            if processed_count % 50 == 0:
                await asyncio.sleep(0)
    else:
        # Original Python-only path: separate pre-scan
        debug_log("Starting pre-scan to build imports map...")
        imports_map = pre_scan_for_imports(files, parsers.keys(), get_parser)
        debug_log(f"Pre-scan complete. Found {len(imports_map)} definitions.")
        processed_count = 0
        for file in files:
            if not file.is_file():
                continue
            if file.suffix not in parsers:
                continue
            if job_id:
                job_manager.update_job(job_id, current_file=str(file))
            repo_path = path.resolve() if path.is_dir() else file.parent.resolve()
            file_data = parse_file(repo_path, file, is_dependency)
            if "error" not in file_data:
                writer.add_file_to_graph(file_data, repo_name, imports_map, repo_path_str=resolved_repo_path_str)
                all_file_data.append(file_data)
            elif not file_data.get("unsupported"):
                add_minimal_file_node(file, repo_path, is_dependency)
            processed_count += 1

            if job_id:
                job_manager.update_job(job_id, processed_files=processed_count)
            if processed_count % 50 == 0:
                await asyncio.sleep(0)

    _phase(t_start, f"file writes complete ({len(all_file_data)} files parsed)")

    # --- Post-processing phase ---
    _phase(t_start, "resolving inheritance links")
    if RUST_AVAILABLE:
        inheritance_batch = _rust_resolve_inheritance(all_file_data, imports_map)
    else:
        inheritance_batch = build_inheritance(all_file_data, imports_map)
    _phase(t_start, f"writing {len(inheritance_batch)} INHERITS edges")
    writer.write_inheritance_links(inheritance_batch)

    _phase(t_start, "resolving function calls")
    if RUST_AVAILABLE:
        from ...cli.config_manager import get_config_value
        skip_external = (get_config_value("SKIP_EXTERNAL_RESOLUTION") or "false").lower() == "true"
        groups = _rust_resolve_calls(all_file_data, imports_map, skip_external)
    else:
        groups = build_function_call_groups(all_file_data, imports_map, None)
    total_calls = sum(len(g) for g in groups)
    _phase(t_start, f"writing {total_calls} CALLS edges")
    writer.write_function_call_groups(*groups)
    _phase(t_start, "indexing complete")

    if job_id:
        job_manager.update_job(job_id, status=JobStatus.COMPLETED, end_time=datetime.now())
