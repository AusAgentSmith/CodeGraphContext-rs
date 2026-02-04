#!/bin/bash

# KùzuDB CLI Comprehensive Test Suite
# Tests all available CLI commands against sample_project

set -e  # Exit on error
export CGC_RUNTIME_DB_TYPE=kuzudb

echo "=========================================="
echo "KùzuDB CLI Comprehensive Test Suite"
echo "=========================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0
TOTAL=0

test_command() {
    local name="$1"
    local cmd="$2"
    TOTAL=$((TOTAL + 1))
    
    echo -n "[$TOTAL] Testing: $name ... "
    
    if eval "$cmd" > /tmp/cgc_test_$TOTAL.log 2>&1; then
        echo -e "${GREEN}✓ PASS${NC}"
        PASSED=$((PASSED + 1))
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}"
        FAILED=$((FAILED + 1))
        echo "  Command: $cmd"
        echo "  Output:"
        tail -20 /tmp/cgc_test_$TOTAL.log | sed 's/^/    /'
        return 1
    fi
}

# Clean start
echo "Cleaning KùzuDB database..."
rm -rf ~/.codegraphcontext/kuzudb
echo ""

# ==========================================
# PROJECT MANAGEMENT COMMANDS
# ==========================================
echo "=== PROJECT MANAGEMENT ==="

test_command "index (full path)" \
    ".venv/bin/cgc index tests/fixtures/sample_projects/sample_project/"

test_command "list repositories" \
    ".venv/bin/cgc list"

test_command "stats (overall)" \
    ".venv/bin/cgc stats"

test_command "stats (specific repo)" \
    ".venv/bin/cgc stats tests/fixtures/sample_projects/sample_project/"

# ==========================================
# DISCOVERY COMMANDS - FIND
# ==========================================
echo ""
echo "=== DISCOVERY: FIND ==="

test_command "find pattern" \
    ".venv/bin/cgc find pattern 'run'"

test_command "find content" \
    ".venv/bin/cgc find content 'argparse'"

test_command "find type function" \
    ".venv/bin/cgc find type function"

test_command "find type class" \
    ".venv/bin/cgc find type class"

test_command "find type variable" \
    ".venv/bin/cgc find type variable"

# ==========================================
# ANALYSIS COMMANDS - CALLS
# ==========================================
echo ""
echo "=== ANALYSIS: CALLS ==="

test_command "analyze callers (direct)" \
    ".venv/bin/cgc analyze callers run"

test_command "analyze calls (what function calls)" \
    ".venv/bin/cgc analyze calls run"

# Note: No --all flag, these commands already do transitive analysis
# test_command "analyze chain (call chain)" \
#     ".venv/bin/cgc analyze chain run main"

# ==========================================
# ANALYSIS COMMANDS - STRUCTURE
# ==========================================
echo ""
echo "=== ANALYSIS: STRUCTURE ==="

test_command "analyze deps (dependencies)" \
    ".venv/bin/cgc analyze deps tests/fixtures/sample_projects/sample_project/cli_and_dunder.py"

test_command "analyze complexity" \
    ".venv/bin/cgc analyze complexity run"

test_command "analyze dead-code" \
    ".venv/bin/cgc analyze dead-code"

# ==========================================
# QUERY COMMANDS
# ==========================================
echo ""
echo "=== QUERY COMMANDS ==="

test_command "query (cypher)" \
    ".venv/bin/cgc query 'MATCH (f:Function) RETURN f.name LIMIT 5'"

# ==========================================
# SHORTCUT COMMANDS
# ==========================================
echo ""
echo "=== SHORTCUT COMMANDS ==="

test_command "shortcut: ls (list)" \
    ".venv/bin/cgc ls"

test_command "shortcut: i (index current dir)" \
    "cd tests/fixtures/sample_projects/sample_project && ../../../../.venv/bin/cgc i"

# ==========================================
# UTILITY COMMANDS
# ==========================================
echo ""
echo "=== UTILITY COMMANDS ==="

test_command "version" \
    ".venv/bin/cgc version"

test_command "help" \
    ".venv/bin/cgc help"

test_command "doctor" \
    ".venv/bin/cgc doctor"

test_command "config show" \
    ".venv/bin/cgc config show"

# ==========================================
# WATCH COMMANDS
# ==========================================
echo ""
echo "=== WATCH COMMANDS ==="

test_command "watching (list watched)" \
    ".venv/bin/cgc watching"

# Note: Skipping actual watch/unwatch as they're async

# ==========================================
# ADVANCED: Re-index with more data
# ==========================================
echo ""
echo "=== ADVANCED: FULL INDEX ==="

echo "Re-indexing with full sample_projects..."
rm -rf ~/.codegraphcontext/kuzudb
.venv/bin/cgc index tests/fixtures/sample_projects/ > /dev/null 2>&1

test_command "find pattern (broader dataset)" \
    ".venv/bin/cgc find pattern 'add'"

test_command "find content (broader dataset)" \
    ".venv/bin/cgc find content 'def'"

test_command "stats (after full index)" \
    ".venv/bin/cgc stats"

test_command "analyze tree (inheritance)" \
    ".venv/bin/cgc analyze tree Calculator" || true

test_command "analyze variable" \
    ".venv/bin/cgc analyze variable parser" || true

# ==========================================
# CLEANUP COMMANDS
# ==========================================
echo ""
echo "=== CLEANUP COMMANDS ==="

test_command "clean (remove orphans)" \
    ".venv/bin/cgc clean"

test_command "delete repository" \
    ".venv/bin/cgc delete tests/fixtures/sample_projects/sample_project/"

# ==========================================
# SUMMARY
# ==========================================
echo ""
echo "=========================================="
echo "TEST SUMMARY"
echo "=========================================="
echo "Total:  $TOTAL"
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ $FAILED test(s) failed. Check logs in /tmp/cgc_test_*.log${NC}"
    exit 1
fi
