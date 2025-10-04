
import pytest
import os

# Path to the sample JavaScript project used in tests
SAMPLE_JS_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "sample_project_javascript"))

# ==============================================================================
# == EXPECTED RELATIONSHIPS
# ==============================================================================

EXPECTED_STRUCTURE = [
    ("functions.js", "regularFunction", "Function"),
    ("functions.js", "greetPerson", "Function"),
    ("functions.js", "functionExpression", "Function"),
    ("functions.js", "arrowFunction", "Function"),
    ("classes.js", "Person", "Class"),
    ("classes.js", "Employee", "Class"),
    ("classes.js", "BankAccount", "Class"),
    ("objects.js", "calculator", "Variable"),
]

EXPECTED_INHERITANCE = [
    pytest.param("Employee", "classes.js", "Person", "classes.js", id="Employee inherits from Person"),
]

EXPECTED_CALLS = [
    pytest.param("getData", "asyncAwait.js", None, "fetchData", "asyncAwait.js", None, id="asyncAwait.getData->fetchData", marks=pytest.mark.skip(reason="JS parser does not yet detect all call relationships.")),
    pytest.param("orchestrator", "functions.js", None, "regularFunction", "functions.js", None, id="functions.orchestrator->regularFunction", marks=pytest.mark.skip(reason="JS parser does not yet detect all call relationships.")),
]

EXPECTED_IMPORTS = [
    pytest.param("importer.js", "defaultExport", "exporter.js", "defaultExportedFunction", id="importer.js imports defaultExport", marks=pytest.mark.xfail(reason="Symbol-level import relationships are not yet implemented for JavaScript.")),
    pytest.param("importer.js", "exportedFunction", "exporter.js", "exportedFunction", id="importer.js imports exportedFunction", marks=pytest.mark.xfail(reason="Symbol-level import relationships are not yet implemented for JavaScript.")),
    pytest.param("importer.js", "ExportedClass", "exporter.js", "ExportedClass", id="importer.js imports ExportedClass", marks=pytest.mark.xfail(reason="Symbol-level import relationships are not yet implemented for JavaScript.")),
]


# ==============================================================================
# == TEST IMPLEMENTATIONS
# ==============================================================================

def check_query(graph, query, description):
    """Helper function to execute a Cypher query and assert that a match is found."""
    try:
        result = graph.query(query)
    except Exception as e:
        pytest.fail(f"Query failed for {description} with error: {e}\nQuery was:\n{query}")

    assert result is not None, f"Query for {description} returned None.\nQuery was:\n{query}"
    assert len(result) > 0, f"Query for {description} returned no records.\nQuery was:\n{query}"
    assert result[0].get('count', 0) > 0, f"No match found for {description}.\nQuery was:\n{query}"

@pytest.mark.parametrize("file_name, item_name, item_label", EXPECTED_STRUCTURE)
def test_file_contains_item(graph, file_name, item_name, item_label):
    """Verifies that a File node correctly CONTAINS a Function or Class node."""
    description = f"CONTAINS from [{file_name}] to [{item_name}]"
    abs_file_path = os.path.join(SAMPLE_JS_PROJECT_PATH, file_name)
    query = f"""
    MATCH (f:File {{path: '{abs_file_path}'}})-[:CONTAINS]->(item:{item_label} {{name: '{item_name}'}})
    RETURN count(*) AS count
    """
    check_query(graph, query, description)

@pytest.mark.parametrize("child_name, child_file, parent_name, parent_file", EXPECTED_INHERITANCE)
def test_inheritance_relationship(graph, child_name, child_file, parent_name, parent_file):
    """Verifies that an INHERITS relationship exists between two classes."""
    description = f"INHERITS from [{child_name}] to [{parent_name}]"
    child_path = os.path.join(SAMPLE_JS_PROJECT_PATH, child_file)
    parent_path = os.path.join(SAMPLE_JS_PROJECT_PATH, parent_file)
    query = f"""
    MATCH (child:Class {{name: '{child_name}', file_path: '{child_path}'}})-[:INHERITS]->(parent:Class {{name: '{parent_name}', file_path: '{parent_path}'}})
    RETURN count(*) as count
    """
    check_query(graph, query, description)

@pytest.mark.parametrize("caller_name, caller_file, caller_class, callee_name, callee_file, callee_class", EXPECTED_CALLS)
def test_function_call_relationship(graph, caller_name, caller_file, caller_class, callee_name, callee_file, callee_class):
    """Verifies that a CALLS relationship exists by checking for nodes first, then the relationship."""
    caller_path = os.path.join(SAMPLE_JS_PROJECT_PATH, caller_file)
    callee_path = os.path.join(SAMPLE_JS_PROJECT_PATH, callee_file)

    if caller_class:
        caller_match = f"(caller_class:Class {{name: '{caller_class}', file_path: '{caller_path}'}})-[:CONTAINS]->(caller:Function {{name: '{caller_name}'}})"
    else:
        caller_match = f"(caller:Function {{name: '{caller_name}', file_path: '{caller_path}'}})"

    if callee_class:
        callee_match = f"(callee_class:Class {{name: '{callee_class}', file_path: '{callee_path}'}})-[:CONTAINS]->(callee:Function {{name: '{callee_name}'}})"
    else:
        callee_match = f"(callee:Function {{name: '{callee_name}', file_path: '{callee_path}'}})"

    relationship_description = f"CALLS from [{caller_name}] to [{callee_name}]"
    relationship_query = f"""
    MATCH {caller_match}
    MATCH {callee_match}
    MATCH (caller)-[:CALLS]->(callee)
    RETURN count(*) as count
    """
    check_query(graph, relationship_query, relationship_description)

@pytest.mark.parametrize("importing_file, imported_symbol_alias, exporting_file, original_symbol_name", EXPECTED_IMPORTS)
def test_import_relationship(graph, importing_file, imported_symbol_alias, exporting_file, original_symbol_name):
    """Verifies that a specific IMPORTS relationship exists between a file and a symbol from another file."""
    description = f"IMPORTS from [{importing_file}] of symbol [{original_symbol_name}] as [{imported_symbol_alias}] from [{exporting_file}]"
    importing_path = os.path.join(SAMPLE_JS_PROJECT_PATH, importing_file)
    exporting_path = os.path.join(SAMPLE_JS_PROJECT_PATH, exporting_file)

    # This query is designed to fail until the feature is implemented.
    # It checks for a direct relationship between the importing file and the specific imported function/class.
    query = f"""
    MATCH (importer:File {{path: '{importing_path}'}})-[:IMPORTS]->(symbol:Node {{name: '{original_symbol_name}', file_path: '{exporting_path}'}})
    RETURN count(*) as count
    """
    check_query(graph, query, description)
