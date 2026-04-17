#!/usr/bin/env python3
"""
Index every immediate subdirectory of ROOT as a separate cgc run.

Useful when ROOT is a workspace with many repos — indexing them as
one giant session is fragile (one deadlock kills everything, `--force`
wipes the lot first). Per-repo runs mean:
  - failures are scoped to one repo, not the whole batch
  - existing indexes for other repos stay intact
  - progress is visible (1/40, 2/40, ...) rather than opaque

Usage:
    scripts/bulk_index.py /home/sprooty/Working
    scripts/bulk_index.py /home/sprooty/Working --force
    scripts/bulk_index.py /home/sprooty/Working --only apps/Arz,apps/rustnzbd
    scripts/bulk_index.py /home/sprooty/Working --skip node_modules,myotherrepos

The default skip list filters out obvious non-project directories and
anything starting with `.`.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_SKIP = {
    "node_modules",
    "target",
    "dist",
    "build",
    ".git",
    "__pycache__",
    "venv",
    ".venv",
}


def _looks_like_project(path: Path) -> bool:
    """A directory is worth indexing if it has a recognisable root marker."""
    markers = (
        "Cargo.toml",
        "pyproject.toml",
        "setup.py",
        "package.json",
        ".git",
        "requirements.txt",
    )
    return any((path / m).exists() for m in markers)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="Workspace root to scan")
    parser.add_argument(
        "--force", action="store_true",
        help="Pass --force to each per-repo cgc index (wipes + rebuilds)",
    )
    parser.add_argument(
        "--only", type=str, default=None,
        help="Comma-separated list of repo names (relative to root) to include",
    )
    parser.add_argument(
        "--skip", type=str, default=None,
        help="Comma-separated list of repo names to exclude "
             "(added to default skip list)",
    )
    parser.add_argument(
        "--cgc", type=str, default="cgc",
        help="Path to the cgc binary (default: first on PATH)",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    if not root.is_dir():
        print(f"not a directory: {root}", file=sys.stderr)
        return 2

    skip = set(DEFAULT_SKIP)
    if args.skip:
        skip |= {s.strip() for s in args.skip.split(",") if s.strip()}
    only = None
    if args.only:
        only = {s.strip() for s in args.only.split(",") if s.strip()}

    # Enumerate candidate subdirectories. Nested structures like
    # `apps/Arz` mean we can't just look at immediate children — walk
    # two levels and accept any path with a project marker.
    candidates: list[Path] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir() or child.name.startswith(".") or child.name in skip:
            continue
        if _looks_like_project(child):
            candidates.append(child)
            continue
        # Second-level: e.g. apps/Arz, libs/nzb-core
        for grandchild in sorted(child.iterdir()):
            if not grandchild.is_dir() or grandchild.name.startswith(".") or grandchild.name in skip:
                continue
            if _looks_like_project(grandchild):
                candidates.append(grandchild)

    if only:
        candidates = [
            c for c in candidates
            if any(
                c.relative_to(root).as_posix() == name
                or c.name == name
                for name in only
            )
        ]

    if not candidates:
        print("no indexable projects found under", root, file=sys.stderr)
        return 1

    print(f"Found {len(candidates)} projects under {root}:")
    for c in candidates:
        print(f"  {c.relative_to(root)}")
    print()

    results: list[tuple[Path, str, float]] = []
    started = time.time()

    for i, path in enumerate(candidates, start=1):
        rel = path.relative_to(root)
        label = f"[{i}/{len(candidates)}] {rel}"
        print(f"{label}  indexing...", flush=True)
        t0 = time.time()
        cmd = [args.cgc, "index", str(path)]
        if args.force:
            cmd.append("--force")
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            print(f"  ✗ {args.cgc} not on PATH", file=sys.stderr)
            return 127

        elapsed = time.time() - t0
        if proc.returncode == 0:
            status = "ok"
            # Pull out the stats block cgc prints on success, if any.
            last_stats = [
                line for line in proc.stdout.splitlines()
                if "Functions" in line or "Classes" in line
                or "Indexed" in line or "files" in line.lower()
            ][-5:]
            print(f"  ✓ done in {elapsed:.1f}s")
            for s in last_stats:
                print(f"    {s.strip()}")
        else:
            status = "fail"
            # Last few lines of stderr almost always contain the reason.
            tail = (proc.stderr or proc.stdout).splitlines()
            msg = "\n    ".join(tail[-8:]) if tail else "(no output)"
            print(f"  ✗ failed in {elapsed:.1f}s  exit={proc.returncode}")
            print(f"    {msg}")

        results.append((path, status, elapsed))

    total = time.time() - started
    oks = sum(1 for _, s, _ in results if s == "ok")
    fails = len(results) - oks
    print()
    print(f"Done. {oks}/{len(results)} succeeded in {total/60:.1f} min.")
    if fails:
        print()
        print("Failed repos:")
        for path, status, elapsed in results:
            if status != "ok":
                print(f"  {path.relative_to(root)}  ({elapsed:.1f}s)")
        print()
        print("Rerun just the failures:")
        failed_names = ",".join(
            p.relative_to(root).as_posix()
            for p, s, _ in results if s != "ok"
        )
        flag = " --force" if args.force else ""
        print(f"  {sys.argv[0]} {root}{flag} --only {failed_names}")

    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
