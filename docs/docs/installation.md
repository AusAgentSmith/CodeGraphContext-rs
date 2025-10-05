## 🧩 Installation Guide

Welcome to **CodeGraphContext**! This guide provides a clear and seamless path to installing and configuring the tool, from prerequisites to launching your server.

## ⚙️ Prerequisites

Ensure the following are installed before you begin:

- **Python**: Version 3.8 or higher.
- **AI Agentic Coding Tool**: An MCP-compatible AI assistant (e.g., Gemini, Claude) if you plan to use the MCP server.

## 🚀 Getting Started

Follow these steps to set up **CodeGraphContext** effortlessly.

### 1. Install from PyPI

Install the `codegraphcontext` package directly from PyPI using pip:

```bash
pip install codegraphcontext
```

### 2. Run the Setup Wizard

Launch the interactive setup wizard to configure your Neo4j database and development environment:

```bash
cgc setup
```

The wizard guides you through a series of intuitive prompts to tailor your setup.

## 🧭 "Step-by-Step Guide for the Setup Wizard"

When you run `cgc setup`, the wizard offers a thoughtful journey through configuration. Follow these steps to complete your setup with ease:

**1. Select Your Database Location**

Choose where your Neo4j database will reside:

- **Local (Recommended)**: Host Neo4j on your machine for simplicity.  
  - **Docker**: With Docker installed, the wizard crafts a `docker-compose.yml` file and launches a Neo4j container seamlessly.  
  - **Local Binary**: On Debian-based systems (e.g., Ubuntu), the wizard installs Neo4j directly with your permission.  
- **Hosted**: Connect to a remote Neo4j instance, such as AuraDB, by providing your database URI, username, and password.  
- **Existing Instance**: For an existing Neo4j server (local or remote), enter its connection credentials.


**2. Configure Your Development Environment**

Integrate CodeGraphContext with your preferred development tool for a harmonious workflow. Select from supported options:

- VS Code
- Cursor
- Windsurf
- Claude
- Gemini CLI
- ChatGPT Codex
- Cline
- RooCode
- Amazon Q Developer

The wizard automatically updates configuration files to align with your choice.


**3. Generate Configuration Files**

Upon completing the prompts, the wizard creates two essential files:

- **`mcp.json`**: Placed in your working directory, this file configures the MCP server.  
- **`.env`**: Stored securely in `~/.codegraphcontext`, this file safeguards your Neo4j credentials.

These files ensure smooth communication between CodeGraphContext, your Neo4j instance, and your AI assistant.

### 3. Start the Server

Once configuration is complete, launch the MCP server with:

```bash
cgc start
```

Your **CodeGraphContext** server is now active, ready to power AI-assisted graph queries.

## 🧱 Database Setup Options

Choose from three flexible database configurations:

| **Setup Type**            | **Description**                                      | **Requirements**              |
|---------------------------|------------------------------------------------------|-------------------------------|
| **Local (Docker)**        | Automatically sets up a Neo4j container.             | Docker & Docker Compose       |
| **Local (Linux Binary)**  | Installs Neo4j directly on Debian-based systems.     | sudo privileges               |
| **Hosted**                | Connects to a remote Neo4j instance (e.g., AuraDB).  | Connection credentials        |

## 💻 IDE/CLI Configuration Summary

The setup wizard streamlines integration with your preferred development tool, automatically generating configuration files for seamless interaction with the MCP server and database. Supported tools include:

- VS Code
- Cursor
- Windsurf
- Claude
- Gemini CLI
- ChatGPT Codex
- Cline
- RooCode
- Amazon Q Developer

## Next Steps

With **CodeGraphContext** installed and configured, you’re ready to explore its AI-powered capabilities. Happy coding ✨!
