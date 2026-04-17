# src/codegraphcontext/core/watcher.py
"""
Live file-watching for the code graph.

The event source is the Rust `_cgc_rust.FileWatcher` (notify-backed).
A tiny poll-loop thread per watched directory drains events from Rust
and dispatches them into the existing `RepositoryEventHandler`, which
owns debouncing + incremental re-indexing — that logic is unchanged
from the watchdog-based implementation it replaces.

Shape of the events delivered to handler methods mirrors the old
watchdog `FileSystemEvent` contract (`is_directory`, `src_path`) so
the handler doesn't care which backend produced them.
"""
import threading
from dataclasses import dataclass
from pathlib import Path
import typing

from codegraphcontext._cgc_rust import FileWatcher as _RustFileWatcher

if typing.TYPE_CHECKING:
    from codegraphcontext.tools.graph_builder import GraphBuilder
    from codegraphcontext.core.jobs import JobManager

from codegraphcontext.utils.debug_log import debug_log, info_logger, error_logger, warning_logger


# Rust event kinds we actually care about. "other" is inotify metadata
# churn (atime / attrib etc.) and is skipped — it would only feed the
# debouncer noise. "renamed" arrives twice per rename (once for each
# side); we treat both as modifications, and the handler's existence
# check collapses the deleted-path side correctly.
_DISPATCH = {
    "created": "on_created",
    "modified": "on_modified",
    "removed": "on_deleted",
    "renamed": "on_modified",
}


@dataclass
class _FakeEvent:
    """Minimal shape matching watchdog's `FileSystemEvent` so handler
    code can stay generic across backends."""
    src_path: str
    is_directory: bool = False


class RepositoryEventHandler:
    """
    A dedicated event handler for a single repository being watched.

    Stateful: performs an initial scan of the repository to build a
    baseline, then uses that cached state to perform efficient
    incremental updates when files change, are created, or deleted.
    """
    def __init__(self, graph_builder: "GraphBuilder", repo_path: Path, debounce_interval=2.0, perform_initial_scan: bool = True):
        """
        Args:
            graph_builder: GraphBuilder instance used for all graph mutations.
            repo_path: Absolute path to the repository directory being watched.
            debounce_interval: Seconds to wait for a quiet period before re-indexing.
            perform_initial_scan: Skip the initial scan for tests / manual bootstrap.
        """
        super().__init__()
        self.graph_builder = graph_builder
        self.repo_path = repo_path
        self.debounce_interval = debounce_interval
        self.timers = {}

        # Caches for the repository's state.
        self.all_file_data = []
        self.imports_map = {}

        if perform_initial_scan:
            self._initial_scan()

    def _initial_scan(self):
        """Parse every file in the repo once and populate the initial graph."""
        info_logger(f"Performing initial scan for watcher: {self.repo_path}")
        supported_extensions = self.graph_builder.parsers.keys()
        all_files = [f for f in self.repo_path.rglob("*") if f.is_file() and f.suffix in supported_extensions]

        # 1. Pre-scan all files to get a global map of where every symbol is defined.
        self.imports_map = self.graph_builder.pre_scan_imports(all_files)

        # 2. Parse all files in detail and cache the parsed data.
        for f in all_files:
            parsed_data = self.graph_builder.parse_file(self.repo_path, f)
            if "error" not in parsed_data:
                self.all_file_data.append(parsed_data)

        # 3. After all files are parsed, create the relationships (e.g., function calls) between them.
        self.graph_builder.link_function_calls(self.all_file_data, self.imports_map)
        self.graph_builder.link_inheritance(self.all_file_data, self.imports_map)
        # Free memory — all_file_data is only needed during the linking pass.
        self.all_file_data.clear()
        info_logger(f"Initial scan and graph linking complete for: {self.repo_path}")

    def _debounce(self, event_path, action):
        """
        Schedule `action` to run after `debounce_interval` seconds of quiet.
        Subsequent events for the same path cancel the pending timer —
        IDEs tend to fire many saves in a burst, this collapses them.
        """
        if event_path in self.timers:
            self.timers[event_path].cancel()
        timer = threading.Timer(self.debounce_interval, action)
        timer.start()
        self.timers[event_path] = timer

    def _update_imports_map_for_file(self, changed_path: Path):
        """Re-scan a single file and merge its contributions into self.imports_map.
        Removes stale paths for the file before inserting new ones so renamed/deleted
        symbols don't leave dangling entries."""
        changed_str = str(changed_path.resolve())
        # Remove old contributions of this file from every symbol it previously exported.
        for symbol in list(self.imports_map.keys()):
            old_list = self.imports_map[symbol]
            if changed_str in old_list:
                new_list = [p for p in old_list if p != changed_str]
                if new_list:
                    self.imports_map[symbol] = new_list
                else:
                    del self.imports_map[symbol]
        # Merge new contributions (if the file still exists).
        if changed_path.exists():
            new_map = self.graph_builder.pre_scan_imports([changed_path])
            for symbol, paths in new_map.items():
                if symbol not in self.imports_map:
                    self.imports_map[symbol] = []
                self.imports_map[symbol].extend(paths)

    def _handle_modification(self, event_path_str: str):
        """
        Incremental update: only re-parse and re-link the changed file plus the files
        that previously called into it.  O(k) instead of O(n) for every event.

        Algorithm:
          1. Query Neo4j for files that have CALLS/INHERITS touching the changed file
             (must happen BEFORE nodes are deleted, so the graph still has the old edges).
          2. Update self.imports_map for the changed file only (O(1) file scan).
          3. update_file_in_graph — DETACH DELETE cleans up ALL CALLS/INHERITS on the
             changed file's nodes (both incoming and outgoing) automatically.
          4. Delete outgoing CALLS from affected *caller* files (their CALLS to the changed
             file were removed by DETACH DELETE, but their CALLS to unrelated files are
             still there; we must delete all their outgoing CALLS before re-creating so we
             don't leave stale CALLS to functions that have moved/been renamed).
          5. Re-parse only the affected subset (changed file + callers + inheritors).
          6. Build file_class_lookup cheaply from Neo4j (no full re-parse needed).
          7. Re-create CALLS/INHERITS for the subset only.
        """
        info_logger(f"File change detected (incremental update): {event_path_str}")
        changed_path = Path(event_path_str)
        changed_path_str = str(changed_path.resolve())
        supported_extensions = self.graph_builder.parsers.keys()

        # Step 1: Find affected neighbours BEFORE nodes are destroyed.
        caller_paths = self.graph_builder.get_caller_file_paths(changed_path_str)
        inheritor_paths = self.graph_builder.get_inheritance_neighbor_paths(changed_path_str)
        affected_paths = {changed_path_str} | caller_paths | inheritor_paths
        info_logger(
            f"[INCREMENTAL] affected={len(affected_paths)} files "
            f"(callers={len(caller_paths)}, inheritors={len(inheritor_paths)})"
        )

        # Step 2: Update imports_map for the changed file only.
        self._update_imports_map_for_file(changed_path)

        # Step 3: Delete + re-add nodes for the changed file.
        # DETACH DELETE inside update_file_in_graph removes all CALLS/INHERITS on its nodes.
        self.graph_builder.update_file_in_graph(changed_path, self.repo_path, self.imports_map)

        # Step 4: Clean up CALLS/INHERITS from the affected *caller/inheritor* files.
        # Their CALLS to the changed file were already removed by DETACH DELETE, but their
        # CALLS to other files are still intact.  We delete all their outgoing CALLS so we
        # can safely re-create the full set from scratch for the subset.
        other_callers = list(caller_paths)       # does NOT include changed_path_str
        other_inheritors = list(inheritor_paths)
        if other_callers:
            self.graph_builder.delete_outgoing_calls_from_files(other_callers)
        if other_inheritors:
            self.graph_builder.delete_inherits_for_files(other_inheritors)

        # Step 5: Re-parse only the affected subset.
        subset_file_data = []
        for path_str in affected_paths:
            p = Path(path_str)
            if p.exists() and p.suffix in supported_extensions:
                parsed = self.graph_builder.parse_file(self.repo_path, p)
                if "error" not in parsed:
                    subset_file_data.append(parsed)

        # Step 6: Get full-repo file_class_lookup from Neo4j (avoids re-parsing all files).
        # The changed file's new classes are already overlaid inside _create_all_function_calls.
        file_class_lookup = self.graph_builder.get_repo_class_lookup(self.repo_path)

        # Step 7: Re-create CALLS/INHERITS for the affected subset only.
        info_logger(f"[INCREMENTAL] Re-linking {len(subset_file_data)} files...")
        self.graph_builder.link_function_calls(subset_file_data, self.imports_map, file_class_lookup)
        self.graph_builder.link_inheritance(subset_file_data, self.imports_map)
        info_logger(f"[INCREMENTAL] Done. Graph refresh for {event_path_str} complete! ✅")

    # Event methods — called by _RustObserver's poll thread with
    # _FakeEvent instances shaped like watchdog events.
    def on_created(self, event):
        if not event.is_directory and Path(event.src_path).suffix in self.graph_builder.parsers:
            self._debounce(event.src_path, lambda: self._handle_modification(event.src_path))

    def on_modified(self, event):
        if not event.is_directory and Path(event.src_path).suffix in self.graph_builder.parsers:
            self._debounce(event.src_path, lambda: self._handle_modification(event.src_path))

    def on_deleted(self, event):
        if not event.is_directory and Path(event.src_path).suffix in self.graph_builder.parsers:
            self._debounce(event.src_path, lambda: self._handle_modification(event.src_path))


class _RustWatch:
    """One observed directory: a `FileWatcher`, its poll thread, and a
    stop signal. Returned by `_RustObserver.schedule` and handed back
    into `unschedule`."""
    __slots__ = ("path", "watcher", "thread", "stop_event")

    def __init__(self, path: str, watcher, thread: threading.Thread, stop_event: threading.Event):
        self.path = path
        self.watcher = watcher
        self.thread = thread
        self.stop_event = stop_event


class _RustObserver:
    """
    watchdog.Observer-compatible wrapper around `_cgc_rust.FileWatcher`.

    One daemon thread per watch polls the Rust queue and dispatches
    events to the handler. Thread stays alive until `unschedule` (or
    `stop`) sets its stop event; poll timeout is short so shutdown is
    responsive.
    """
    _POLL_TIMEOUT_MS = 500

    def __init__(self):
        self._watches = {}  # path_str -> _RustWatch
        self._lock = threading.Lock()

    def schedule(self, event_handler, path: str, recursive: bool = True):
        # The Rust watcher is always recursive. `recursive=False` would
        # require filtering sub-paths in the dispatcher — not something
        # any caller uses today, so we don't bother.
        _ = recursive
        watcher = _RustFileWatcher(path)
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._poll_loop,
            args=(event_handler, watcher, stop_event),
            name=f"cgc-watcher-{Path(path).name}",
            daemon=True,
        )
        thread.start()
        watch = _RustWatch(path=path, watcher=watcher, thread=thread, stop_event=stop_event)
        with self._lock:
            self._watches[path] = watch
        return watch

    def unschedule(self, watch: "_RustWatch"):
        watch.stop_event.set()
        # We don't join() here to stay symmetric with watchdog — the
        # daemon thread will exit on its next poll timeout.
        with self._lock:
            self._watches.pop(watch.path, None)

    def _poll_loop(self, handler, watcher, stop_event: threading.Event):
        """Background loop: drain events, dispatch to handler.

        Exceptions in user handler code are logged and swallowed — we
        don't want a bad event to kill the whole watcher.
        """
        while not stop_event.is_set():
            try:
                events = watcher.poll(self._POLL_TIMEOUT_MS)
            except Exception as e:
                error_logger(f"FileWatcher.poll failed: {e}")
                stop_event.set()
                return
            for kind, path in events:
                method_name = _DISPATCH.get(kind)
                if method_name is None:
                    continue  # "other" etc. — skip
                method = getattr(handler, method_name, None)
                if method is None:
                    continue
                event = _FakeEvent(src_path=path, is_directory=False)
                try:
                    method(event)
                except Exception as e:
                    error_logger(f"watcher handler {method_name} raised: {e}")

    # watchdog.Observer API compatibility --------------------------------

    def start(self):
        # Threads are started in schedule() so this is a no-op. We keep
        # it here because CodeWatcher.start() calls observer.start().
        pass

    def stop(self):
        with self._lock:
            watches = list(self._watches.values())
            self._watches.clear()
        for w in watches:
            w.stop_event.set()

    def is_alive(self) -> bool:
        with self._lock:
            return any(not w.stop_event.is_set() for w in self._watches.values())

    def join(self, timeout=None):
        with self._lock:
            threads = [w.thread for w in self._watches.values()]
        for t in threads:
            t.join(timeout)


class CodeWatcher:
    """
    Top-level watcher. Owns one `_RustObserver` and registers a
    `RepositoryEventHandler` per directory under `watch_directory`.
    """
    def __init__(self, graph_builder: "GraphBuilder", job_manager= "JobManager"):
        self.graph_builder = graph_builder
        self.observer = _RustObserver()
        self.watched_paths = set()
        self.watches = {}

    def watch_directory(self, path: str, perform_initial_scan: bool = True):
        """Schedule a directory to be watched for changes."""
        path_obj = Path(path).resolve()
        path_str = str(path_obj)

        if path_str in self.watched_paths:
            info_logger(f"Path already being watched: {path_str}")
            return {"message": f"Path already being watched: {path_str}"}

        event_handler = RepositoryEventHandler(self.graph_builder, path_obj, perform_initial_scan=perform_initial_scan)

        watch = self.observer.schedule(event_handler, path_str, recursive=True)
        self.watches[path_str] = watch
        self.watched_paths.add(path_str)
        info_logger(f"Started watching for code changes in: {path_str}")

        return {"message": f"Started watching {path_str}."}

    def unwatch_directory(self, path: str):
        """Stop watching a directory for changes."""
        path_obj = Path(path).resolve()
        path_str = str(path_obj)

        if path_str not in self.watched_paths:
            warning_logger(f"Attempted to unwatch a path that is not being watched: {path_str}")
            return {"error": f"Path not currently being watched: {path_str}"}

        watch = self.watches.pop(path_str, None)
        if watch:
            self.observer.unschedule(watch)

        self.watched_paths.discard(path_str)
        info_logger(f"Stopped watching for code changes in: {path_str}")
        return {"message": f"Stopped watching {path_str}."}

    def list_watched_paths(self) -> list:
        """Returns a list of all currently watched directory paths."""
        return list(self.watched_paths)

    def start(self):
        """Start the observer thread."""
        if not self.observer.is_alive():
            self.observer.start()
            info_logger("Code watcher observer thread started.")

    def stop(self):
        """Stop the observer thread gracefully."""
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
            info_logger("Code watcher observer thread stopped.")
