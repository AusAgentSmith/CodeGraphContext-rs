# Contributing to CodeGraphContext

We welcome contributions! Please follow these steps:

## General Guidelines

*   Ensure your code adheres to the existing style and conventions of the project.
*   Write clear, concise, and well-documented code.
*   All new features or bug fixes should be accompanied by appropriate tests.
*   Keep your pull requests focused on a single feature or bug fix.

## Setting up Your Development Environment

1.  Fork the repository.
2.  Set up your development environment: `pip install -e ".[dev]"`
3.  Create a new branch for your feature or bugfix (e.g., `git checkout -b feature/my-new-feature`).

## Debugging

To enable debug mode for detailed logging, locate the `debug_mode` variable in `src/codegraphcontext/tools/graph_builder.py` and set its value to `1`.

```python
# src/codegraphcontext/tools/graph_builder.py
debug_mode = 1
```

## Running Tests

Tests are located in the `tests/` directory and are run using `pytest`.

1.  Navigate to the root of the `CodeGraphContext` directory.
2.  Run all tests using the command: `pytest`
3.  To run specific tests, you can provide the path to the test file, for example: `pytest tests/test_tools.py`
4.  **Skipping Re-indexing:** To speed up test runs, especially during development, you can set the `CGC_SKIP_REINDEX` environment variable to `true`. This will prevent the test suite from re-indexing the sample project if it's already indexed.
    ```bash
    CGC_SKIP_REINDEX=true pytest
    ```

## Submitting Changes

1.  Write your code and add corresponding tests in the `tests/` directory.
2.  Ensure all tests pass and your code lints without errors.
3.  Commit your changes with a descriptive commit message.
4.  Submit a pull request to the `main` branch.


<!-- "Failed to check job status: 'JobManager' object has no attribute 'JobStatus'" -->