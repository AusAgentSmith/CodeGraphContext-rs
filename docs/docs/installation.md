# Installation

This page details the installation process for CodeGraphContext.

## Prerequisites

Before you begin, ensure you have the following installed:

-   **Python:** Version 3.8 or higher.
-   **AI Agentic Coding Tool:** An MCP-compatible AI assistant (e.g., Gemini, Claude, etc.) if you plan to use the MCP server.

## Getting Started

### 1. Install from PyPI

Install the `codegraphcontext` package directly from PyPI using pip:

```bash
pip install codegraphcontext
```

### 2. Configure the Server

Run the interactive setup wizard to configure your Neo4j database connection and development environment:

```bash
cgc setup
```

The setup wizard will guide you through the following options:

!!! details "Step-by-step guide for the setup wizard"

    When you run `cgc setup`, you will be presented with a series of questions. Here’s a guide on how to answer them:

    **1. Database Location:**

    You will first be asked where you want to set up your Neo4j database.

    *   **Local (Recommended):** Choose this if you want to run Neo4j on your own machine. This is the easiest option if you have Docker installed.
        *   **Docker:** The wizard will automatically create a `docker-compose.yml` file and start a Neo4j container.
        *   **Local Binary:** For Debian-based systems, the wizard can install Neo4j for you.
    *   **Hosted:** Choose this if you have a remote Neo4j database like AuraDB. You will be prompted to enter your connection details.
    *   **I already have an existing neo4j instance running:** Choose this if you already have a Neo4j instance running (local or remote) and you want to provide the credentials manually.

    *(Image placeholder: Screenshot of the database location prompt)*

    **2. IDE/CLI Configuration:**

    After setting up the database, the wizard will ask to configure your development environment.

    *   Choose your preferred IDE or CLI from the list. The wizard will automatically update the necessary configuration files.

    *(Image placeholder: Screenshot of the IDE/CLI selection prompt)*

    **3. Configuration Files:**

    Upon successful configuration, the wizard will create:

    *   `mcp.json`: In your current directory, for the MCP server configuration.
    *   `.env`: In `~/.codegraphcontext`, to store your Neo4j credentials securely.

#### Database Configuration

You can choose from three database setups:

-   **Local Setup (Docker Recommended):** The wizard can automatically set up a local Neo4j instance using Docker. This is the easiest way to get started if you have Docker and Docker Compose installed.
-   **Local Setup (Linux Binary):** For Debian-based systems like Ubuntu, the wizard can automate the installation of Neo4j directly on your machine. This requires `sudo` privileges.
-   **Hosted Setup:** If you have an existing remote Neo4j database (like Neo4j AuraDB), you can provide your credentials to connect to it.

#### IDE/CLI Configuration

After setting up the database, the wizard can automatically configure your preferred development tool to work with CodeGraphContext. Supported tools include:

-   VS Code
-   Cursor
-   Windsurf
-   Claude
-   Gemini CLI
-   ChatGPT Codex
-   Cline
-   RooCode
-   Amazon Q Developer

Upon successful configuration, the wizard will create the necessary files, including `mcp.json` for server configuration and a `.env` file for your credentials in `~/.codegraphcontext`.

### 3. Start the Server

Once the setup is complete, you can start the CodeGraphContext MCP server:

```bash
cgc start
```

The server will now be running and ready to accept requests from your AI assistant.