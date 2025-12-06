# Snowflake Migration Pipeline

An agentic AI-powered database migration tool that automates the process of migrating databases to Snowflake using GPT-5.1 agents.

## Features

- **Multi-Database Support**: PostgreSQL, Teradata, MySQL, SQL Server, Oracle, MongoDB
- **3-Phase Migration**: Schema Analysis -> Migration Planning -> Execution
- **AI-Powered Debate**: Two planner agents debate to create optimal migration strategies
- **Self-Correcting Workers**: Agents retry and fix errors automatically (up to 7 attempts)
- **Dynamic Package Installation**: LLM agents install required database drivers on-the-fly
- **Visual Schema Diagrams**: Auto-generated Mermaid ER diagrams
- **Web UI**: Modern frontend for configuring and monitoring migrations

## Architecture

```
Phase 1: Schema Analyzer Agent
    |-- Connects to source database
    |-- Discovers tables, columns, relationships
    |-- Generates detailed schema catalog
    
Phase 2: Planner Agents (Alpha & Beta)
    |-- Debate migration strategy
    |-- Multiple rounds of critique and revision
    |-- Produces final migration plan
    
Phase 3: Worker Agent
    |-- Creates Snowflake schema and tables
    |-- Extracts and loads data
    |-- Adds foreign keys
    |-- Validates migration
```

## Quick Start

### Prerequisites

- Python 3.11+
- Azure OpenAI API access (GPT-5.1 Codex)
- Snowflake account

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Kshitij-01/snowflakemigration.git
   cd snowflakemigration
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   
   Or use a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

### Configuration

1. Copy the example credentials file:
   ```bash
   cp credentials.example.txt credentials.txt
   ```

2. Edit `credentials.txt` and replace all placeholder values with your actual credentials:
   - **Required**: Azure OpenAI credentials (API key, endpoint)
   - **Required**: Snowflake credentials (account, user, password, warehouse, database)
   - **Optional**: Source database credentials (PostgreSQL, Teradata, etc.) - can also be provided in Phase 1 instructions
   - **Optional**: AWS, Azure Key Vault, and other service credentials

   See `credentials.example.txt` for the complete structure with detailed comments.

   **Important**: The `credentials.txt` file is in `.gitignore` and will NOT be committed to version control. Always use `credentials.example.txt` as a template.

### Run the Web UI

```bash
python api_server.py
```

Open http://localhost:8000 in your browser.

### Example Migration Prompt

```
Teradata ClearScape
Host: your-host.env.clearscape.teradata.com
User: demo_user
Password: your-password
Schema: demo_user
```

## Project Structure

```
snowflakemigration/
|-- agents/
|   |-- schema_analyzer.py   # Phase 1: Schema discovery
|   |-- planner.py           # Phase 2: Migration planning debate
|   |-- executor.py          # Phase 3: Migration execution
|   |-- diagram_generator.py # Mermaid diagram generation
|-- frontend/
|   |-- index.html           # Web UI
|   |-- styles.css           # Styling
|   |-- app.js               # Frontend logic
|-- api_server.py            # FastAPI backend
|-- config.py                # Configuration management
|-- azure_openai_client.py   # Azure OpenAI client
|-- requirements.txt         # Python dependencies
|-- Dockerfile               # Container configuration
|-- deploy-azure.ps1         # Azure deployment script
```

## Supported Databases

| Source Database | Driver |
|----------------|--------|
| PostgreSQL | psycopg2-binary |
| Teradata | teradatasql |
| MySQL | pymysql |
| SQL Server | pyodbc |
| Oracle | cx_Oracle |
| MongoDB | pymongo |

## License

MIT

