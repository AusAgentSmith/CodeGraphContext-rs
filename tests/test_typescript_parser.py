import pytest
from pathlib import Path
import time

# Import the helper function from the conftest.py file
from .conftest import call_tool

# Define the path to the TypeScript sample project
TS_SAMPLE_PROJECT_PATH = str(Path(__file__).parent / "sample_project_typescript")

def wait_for_job_completion(server, job_id, timeout=60):
    """Helper function to wait for an indexing job to complete."""
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            pytest.fail(f"Job {job_id} timed out after {timeout} seconds.")
        
        status_result = call_tool(server, "check_job_status", {"job_id": job_id})
        job_status = status_result.get("job", {}).get("status")
        
        if job_status == "completed":
            break
        elif job_status in ["failed", "cancelled"]:
            pytest.fail(f"Indexing job failed with status: {job_status}")
            
        time.sleep(1)

# This test uses the 'server' fixture from conftest.py
# Pytest automatically starts the cgc server and passes in a communication function.
def test_typescript_interfaces_and_types_are_indexed(server):
    """
    This is an integration test. It indexes the TypeScript sample project 
    and then queries the live database to verify the results.
    """
    # ARRANGE: First, delete any previous data for this project to ensure a clean slate.
    call_tool(server, "delete_repository", {"repo_path": TS_SAMPLE_PROJECT_PATH})

    # ACT (Part 1): Index the entire TypeScript sample project.
    add_result = call_tool(server, "add_code_to_graph", {"path": TS_SAMPLE_PROJECT_PATH})
    assert add_result.get("success") is True, "Failed to start indexing job"
    job_id = add_result.get("job_id")
    
    # Wait for the indexing to finish.
    wait_for_job_completion(server, job_id)

    # ACT (Part 2): Query the database for the interfaces and types we expect to find.
    interface_query = "MATCH (i:Interface {name: 'User'}) RETURN i.name AS name"
    type_alias_query = "MATCH (t:TypeAlias {name: 'UserID'}) RETURN t.name AS name"

    interface_result = call_tool(server, "execute_cypher_query", {"cypher_query": interface_query})
    type_alias_result = call_tool(server, "execute_cypher_query", {"cypher_query": type_alias_query})

    # ASSERT: Check that the queries returned the expected results from the database.
    assert interface_result.get("success") is True
    assert len(interface_result.get("results", [])) == 1, "Should find exactly one 'User' interface"
    assert interface_result["results"][0]["name"] == "User"

    assert type_alias_result.get("success") is True
    assert len(type_alias_result.get("results", [])) == 1, "Should find exactly one 'UserID' type alias"
    assert type_alias_result["results"][0]["name"] == "UserID"