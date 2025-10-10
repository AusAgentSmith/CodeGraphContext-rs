# Real World Use Cases

CodeGraphContext can be a powerful ally in your daily coding journey. Here are some real-world scenarios where it can significantly boost your productivity and understanding of a codebase:

1.  **Onboarding New Developers:** A new team member can quickly get up to speed by asking questions like:
    -   "Where is the authentication logic handled?"
    -   "Show me the main entry point of the application."

2.  **Impact Analysis:** Before making a change, assess the potential ripple effects:
    -   "What other parts of the code will be affected if I change the `calculate_total` function?"

3.  **Code Review:** Gain context on pull requests faster:
    -   "Show me the callers of this new function."
    -   "Does this change introduce any new dependencies?"

4.  **Debugging:** Trace execution flows to pinpoint the source of a bug:
    -   "Show me the call chain from `handle_request` to `process_payment`."

5.  **Refactoring:** Identify and plan large-scale code changes:
    -   "Find all instances of the deprecated `OldApiClass`."
    -   "List all functions that use the `urllib` library so I can replace them with `requests`."

6.  **Identifying Code Smells:** Proactively find areas that need improvement:
    -   "Find the 10 most complex functions in the codebase."
    -   "Show me functions with more than 5 arguments."

7.  **Security Audits:** Search for potentially vulnerable code patterns:
    -   "Find all functions that use the `eval` function."
    -   "Show me where raw SQL queries are being executed."

8.  **Automated Documentation:** Use the tool's understanding of the code to generate documentation. (This is what this agent is doing!)

9.  **Dependency Management:** Understand how your project uses its dependencies:
    -   "Which files import the `requests` library?"
    -   "Are there any circular dependencies between modules?"

10. **Cleaning Up Unused Code:** Keep your codebase lean and maintainable:
    -   "Is there any dead or unused code in this project?"

11. **Exploring a Large Codebase:** Navigate large, unfamiliar projects with ease:
    -   "List all the classes in the `core` module."
    -   "What are the top-level functions in the `utils` directory?"

12. **Enforcing Coding Standards:** Check for adherence to team conventions:
    -   "Find all functions that are not decorated with `@log_execution`."
    -   "Show me all public methods that don't have a docstring."

13. **Discovering API Usage:** Find examples of how to use internal or external APIs:
    -   "Show me how other parts of the code use the `UserService`."

14. **Improving Test Coverage:** Identify areas that may lack test coverage:
    -   "Find all functions that are not called by any test files."

15. **Visualizing Code Structure:** Get a bird's-eye view of your project's architecture:
    -   "Generate a graph visualization of the `auth` module and its dependencies."

16. **Learning a New Framework:** Understand how a new framework operates by exploring its source code.

17. **Code Archeology:** Investigate legacy code to understand its history and purpose.

18. **Planning a Migration:** Identify all points of contact when migrating a library or framework:
    -   "Find all the places where the `old_payment_gateway` is used."

19. **Knowledge Sharing:** Use the code graph as a centralized, always-up-to-date knowledge base for your team.

20. **Automated Code Reviews:** Integrate CodeGraphContext into your CI/CD pipeline to automatically flag potential issues in pull requests.
