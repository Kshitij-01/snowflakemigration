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

```bash
git clone https://github.com/Kshitij-01/snowflakemigration.git
cd snowflakemigration
pip install -r requirements.txt
```

### Configuration

Create a `credentials.txt` file:

```
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MIGRATION_DB
```

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

