
import subprocess
import json
import os
import time
import pytest

@pytest.fixture(scope="module")
def setup_env():
    env_content = """
NEO4J_URI=neo4j+s://44df5fd5.databases.neo4j.io
NEO4J_USERNAME=44df5fd5
NEO4J_PASSWORD=vSwK0dBCmaaMEQKFvWWFc7bPAdYlMAXFBlND-Tj-OEA
"""
    # The .env file should be in the directory where the command is run
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    with open(env_path, "w") as f:
        f.write(env_content)
    
    yield
    
    os.remove(env_path)

def test_jsonrpc_communication(setup_env):
    """
    Tests the JSON-RPC communication with the cgc server.
    """
    print("\n--- Starting test_jsonrpc_communication ---")
    process = None
    try:
        # Start the server
        print("Starting cgc server...")
        process = subprocess.Popen(
            ["cgc", "start"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # Run from the parent directory of tests, where .env will be
            cwd=os.path.join(os.path.dirname(__file__), "..")
        )

        # Wait for the server to be ready by reading stderr
        print("Waiting for server to be ready...")
        for line in iter(process.stderr.readline, ''):
            print(f"STDERR: {line.strip()}")
            if "MCP Server is running" in line:
                print("Server is ready.")
                break

        # Helper to send and receive
        def send_receive(request):
            print(f"--> Sending request: {json.dumps(request)}")
            process.stdin.write(json.dumps(request) + "\n")
            process.stdin.flush()
            
            # The first few lines from stdout might be startup messages
            while True:
                response_line = process.stdout.readline()
                print(f"<-- Received line: {response_line.strip()}")
                try:
                    # Try to parse as JSON. If it works, it's a response.
                    return json.loads(response_line)
                except json.JSONDecodeError:
                    # It's not JSON, so it's probably a startup message.
                    # Continue reading until we get a valid JSON response.
                    continue

        # 1. Initialize
        print("\n--- Step 1: Initialize ---")
        init_request = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        init_response = send_receive(init_request)
        assert init_response["id"] == 1
        assert "result" in init_response
        assert init_response["result"]["serverInfo"]["name"] == "CodeGraphContext"
        print("Initialize successful.")

        # 2. List tools
        print("\n--- Step 2: List tools ---")
        list_tools_request = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        list_tools_response = send_receive(list_tools_request)
        assert list_tools_response["id"] == 2
        assert "result" in list_tools_response
        assert "tools" in list_tools_response["result"]
        assert len(list_tools_response["result"]["tools"]) > 0
        print(f"Found {len(list_tools_response['result']['tools'])} tools.")

        # 3. Call a tool
        print("\n--- Step 3: Call a tool (list_indexed_repositories) ---")
        call_tool_request = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "list_indexed_repositories",
                "arguments": {}
            }
        }
        call_tool_response = send_receive(call_tool_request)
        assert call_tool_response["id"] == 3
        assert "result" in call_tool_response
        content = json.loads(call_tool_response["result"]["content"][0]["text"])
        assert content["success"] is True
        print("Tool call successful.")

    finally:
        if process:
            print("\n--- Tearing down test ---")
            print("Terminating server process.")
            process.terminate()
            process.wait()
            print("Server process terminated.")

