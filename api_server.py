#!/usr/bin/env python
"""
Backend API Server for Snowflake Migration Pipeline

Provides REST API endpoints for the frontend to:
- Validate database connections
- Start migrations
- Poll migration status
"""

import asyncio
import datetime
import json
import os
import threading
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

# Import pipeline modules
from config import get_azure_openai_config, load_credentials

app = FastAPI(title="Snowflake Migration API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for migration status
migrations: Dict[str, Dict[str, Any]] = {}


# ============================================
# Request/Response Models
# ============================================

class PlannerConfig(BaseModel):
    alpha_model: str = "enmapper-gpt-5.1-codex"
    beta_model: str = "enmapper-gpt-5.1"
    debate_rounds: int = 2


class WorkerConfig(BaseModel):
    model: str = "enmapper-gpt-5.1-codex"
    effort: str = "medium"


class MigrationRequest(BaseModel):
    run_id: str
    phase1_instructions: str = ""
    phase2_instructions: str = ""
    phase3_instructions: str = ""
    planner: PlannerConfig = PlannerConfig()
    worker: WorkerConfig = WorkerConfig()


# ============================================
# API Endpoints
# ============================================

@app.get("/api/health")
def health_check():
    return {"status": "ok", "service": "Snowflake Migration API"}


@app.post("/api/migration/start")
def start_migration(request: MigrationRequest, background_tasks: BackgroundTasks):
    """Start a new migration and return migration ID."""
    migration_id = str(uuid.uuid4())[:8]
    
    # Create run folder
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_folder_name = f"{request.run_id}_{timestamp}"
    output_base = os.path.join(os.path.dirname(__file__), "output")
    run_folder = os.path.join(output_base, run_folder_name)
    os.makedirs(run_folder, exist_ok=True)
    
    # Initialize migration status
    migrations[migration_id] = {
        "id": migration_id,
        "run_id": request.run_id,
        "run_folder": run_folder,
        "run_folder_name": run_folder_name,
        "started_at": datetime.datetime.now().isoformat(),
        "complete": False,
        "success": False,
        "error": None,
        "phase1": {"status": "pending"},
        "phase2": {"status": "pending"},
        "phase3": {"status": "pending"},
        "logs": [],
    }
    
    # Run migration in background (credentials file can be uploaded after this,
    # but before Phase 3 which actually uses them)
    background_tasks.add_task(run_migration_pipeline, migration_id, request, run_folder)
    
    return {
        "migration_id": migration_id,
        "run_folder": run_folder_name,
        "status": "started"
    }


@app.get("/api/migration/{migration_id}/status")
def get_migration_status(migration_id: str):
    """Get current migration status."""
    if migration_id not in migrations:
        raise HTTPException(status_code=404, detail="Migration not found")
    
    status = migrations[migration_id].copy()
    
    # Get and clear pending logs
    logs = status.pop("logs", [])
    status["logs"] = logs
    migrations[migration_id]["logs"] = []  # Clear after sending
    
    return status


@app.get("/api/migrations")
def list_migrations():
    """List all migrations."""
    return [
        {
            "id": m["id"],
            "run_id": m["run_id"],
            "started_at": m["started_at"],
            "complete": m["complete"],
            "success": m["success"],
        }
        for m in migrations.values()
    ]


@app.post("/api/migration/{migration_id}/credentials")
async def upload_credentials_file(migration_id: str, file: UploadFile = File(...)):
    """Upload a custom credentials.txt file for the migration."""
    if migration_id not in migrations:
        raise HTTPException(status_code=404, detail="Migration not found")
    
    # Validate file type
    if not file.filename.endswith('.txt'):
        raise HTTPException(status_code=400, detail="File must be a .txt file")
    
    # Get run folder
    run_folder = migrations[migration_id].get("run_folder")
    if not run_folder:
        raise HTTPException(status_code=400, detail="Migration run folder not found")
    
    # Save credentials file to run folder
    credentials_path = os.path.join(run_folder, "credentials.txt")
    
    try:
        # Read file content
        content = await file.read()
        
        # Write to run folder
        with open(credentials_path, 'wb') as f:
            f.write(content)
        
        add_log(migration_id, f"Credentials file uploaded: {file.filename}", "info")
        
        return {
            "success": True,
            "filename": file.filename,
            "path": credentials_path,
            "message": "Credentials file uploaded successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save credentials file: {str(e)}")


@app.get("/api/migration/{migration_id}/diagram")
def generate_diagram(migration_id: str, background_tasks: BackgroundTasks):
    """Generate Mermaid ER diagram from the schema catalog."""
    if migration_id not in migrations:
        raise HTTPException(status_code=404, detail="Migration not found")
    
    migration = migrations[migration_id]
    
    # Check if Phase 1 is complete
    if migration["phase1"].get("status") != "complete":
        raise HTTPException(status_code=400, detail="Phase 1 not complete yet")
    
    # Check if diagram already generated
    if "mermaid_code" in migration:
        return {"mermaid_code": migration["mermaid_code"]}
    
    # Load catalog from file
    run_folder = migration["run_folder"]
    schema_agent_dir = os.path.join(run_folder, "schema_agent")
    
    catalog_files = [
        f for f in os.listdir(schema_agent_dir)
        if f.startswith("schema_catalog_") and f.endswith(".json")
    ]
    
    if not catalog_files:
        raise HTTPException(status_code=404, detail="No catalog found")
    
    catalog_files.sort(reverse=True)
    catalog_path = os.path.join(schema_agent_dir, catalog_files[0])
    
    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)
    
    # Generate diagram
    try:
        from agents.diagram_generator import DiagramGeneratorAgent
        
        azure_config = get_azure_openai_config(run_folder=run_folder)
        
        diagram_agent = DiagramGeneratorAgent(
            llm_config={
                "deployment": azure_config["deployment"],
                "api_key": azure_config["api_key"],
                "base_url": azure_config["base_url"],
                "api_version": azure_config["api_version"],
                "reasoning_effort": "low",  # Low effort for diagram generation
            }
        )
        
        mermaid_code = diagram_agent.generate_mermaid(catalog)
        
        # Cache the result
        migration["mermaid_code"] = mermaid_code
        
        # Also save to file
        diagram_path = os.path.join(schema_agent_dir, "schema_diagram.mmd")
        with open(diagram_path, "w", encoding="utf-8") as f:
            f.write(mermaid_code)
        
        return {"mermaid_code": mermaid_code}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Diagram generation failed: {str(e)}")


# ============================================
# Migration Pipeline Runner
# ============================================

def add_log(migration_id: str, message: str, log_type: str = "info"):
    """Add a log entry to the migration."""
    if migration_id in migrations:
        migrations[migration_id]["logs"].append({
            "message": message,
            "type": log_type,
            "time": datetime.datetime.now().isoformat()
        })


def load_source_config_from_run(run_folder: str) -> dict:
    """Load source database config saved by Phase 1 agent."""
    config_path = os.path.join(run_folder, "source_config.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Source config not found at {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_instructions_for_target(instructions: str, source_schema: str) -> str:
    """Parse Phase 3 instructions to extract target schema."""
    # Default: uppercase source schema
    target_schema = source_schema.upper()
    
    lines = instructions.split('\n')
    for line in lines:
        line_lower = line.lower().strip()
        if 'target' in line_lower and 'schema' in line_lower and ':' in line:
            parts = line.split(':', 1)
            if len(parts) == 2:
                schema_val = parts[1].strip().strip('"\'').upper()
                if schema_val:
                    target_schema = schema_val
                    break
    
    return target_schema


def run_migration_pipeline(migration_id: str, request: MigrationRequest, run_folder: str):
    """Run the full migration pipeline based on instructions."""
    try:
        add_log(migration_id, "Phase 1 will extract connection details from instructions", "info")
        
        # =========================================
        # PHASE 1: Schema Analysis
        # =========================================
        add_log(migration_id, "Starting Phase 1: Schema Analysis", "phase1")
        migrations[migration_id]["phase1"]["status"] = "running"
        
        try:
            catalog = run_phase1(migration_id, run_folder, request.phase1_instructions)
            migrations[migration_id]["phase1"]["status"] = "complete"
            migrations[migration_id]["phase1"]["tables"] = len(catalog.get("tables", []))
            migrations[migration_id]["phase1"]["relationships"] = len(catalog.get("relationships", []))
            migrations[migration_id]["phase1"]["tables_list"] = [
                {
                    "name": t.get("table_name"),
                    "rows": t.get("row_count", 0),
                    "columns": len(t.get("columns", [])),
                }
                for t in catalog.get("tables", [])
            ]
            add_log(migration_id, f"Phase 1 complete: {len(catalog.get('tables', []))} tables found", "success")
        except Exception as e:
            migrations[migration_id]["phase1"]["status"] = "failed"
            migrations[migration_id]["phase1"]["error"] = str(e)
            add_log(migration_id, f"Phase 1 failed: {e}", "error")
            raise
        
        # Load source config saved by Phase 1
        source_db = load_source_config_from_run(run_folder)
        source_schema = source_db.get("schema", "public")
        add_log(migration_id, f"Source: {source_db.get('host')}/{source_db.get('database')}.{source_schema}", "info")
        
        # Parse target schema from Phase 3 instructions
        target_schema = parse_instructions_for_target(request.phase3_instructions, source_schema)
        
        # Load Snowflake credentials (check run folder first for uploaded credentials)
        creds = load_credentials(run_folder=run_folder)
        target_db = {
            "account": creds.get("SNOWFLAKE_ACCOUNT", ""),
            "user": creds.get("SNOWFLAKE_USER", ""),
            "password": creds.get("SNOWFLAKE_PASSWORD", ""),
            "warehouse": creds.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": creds.get("SNOWFLAKE_DATABASE", "MIGRATION_DB"),
            "schema": target_schema,
        }
        
        add_log(migration_id, f"Target: Snowflake {target_db['database']}.{target_schema}", "info")
        
        # =========================================
        # PHASE 2: Migration Planning
        # =========================================
        add_log(migration_id, "Starting Phase 2: Migration Planning", "phase2")
        migrations[migration_id]["phase2"]["status"] = "running"
        
        try:
            plan_result = run_phase2(
                migration_id, run_folder, catalog, 
                source_schema, request.planner, request.phase2_instructions
            )
            migrations[migration_id]["phase2"]["status"] = "complete"
            migrations[migration_id]["phase2"]["rounds"] = request.planner.debate_rounds
            migrations[migration_id]["phase2"]["summary"] = "Migration plan created successfully"
            add_log(migration_id, "Phase 2 complete: Migration plan ready", "success")
        except Exception as e:
            migrations[migration_id]["phase2"]["status"] = "failed"
            migrations[migration_id]["phase2"]["error"] = str(e)
            add_log(migration_id, f"Phase 2 failed: {e}", "error")
            raise
        
        # =========================================
        # PHASE 3: Migration Execution
        # =========================================
        add_log(migration_id, "Starting Phase 3: Migration Execution", "phase3")
        migrations[migration_id]["phase3"]["status"] = "running"
        
        try:
            exec_result = run_phase3(
                migration_id, run_folder, catalog,
                source_db, target_db, request.worker, request.phase3_instructions
            )
            migrations[migration_id]["phase3"]["status"] = "complete"
            migrations[migration_id]["phase3"]["completed"] = exec_result.get("completed_tasks", 0)
            migrations[migration_id]["phase3"]["total"] = exec_result.get("total_tasks", 0)
            migrations[migration_id]["phase3"]["duration"] = round(exec_result.get("duration_seconds", 0), 1)
            migrations[migration_id]["phase3"]["results"] = exec_result.get("validation_results", [])
            add_log(migration_id, f"Phase 3 complete: {exec_result.get('completed_tasks')}/{exec_result.get('total_tasks')} tasks", "success")
        except Exception as e:
            migrations[migration_id]["phase3"]["status"] = "failed"
            migrations[migration_id]["phase3"]["error"] = str(e)
            add_log(migration_id, f"Phase 3 failed: {e}", "error")
            raise
        
        # Success
        migrations[migration_id]["complete"] = True
        migrations[migration_id]["success"] = True
        add_log(migration_id, "Migration completed successfully!", "success")
        
    except Exception as e:
        migrations[migration_id]["complete"] = True
        migrations[migration_id]["success"] = False
        migrations[migration_id]["error"] = str(e)


def run_phase1(migration_id: str, run_folder: str, instructions: str) -> dict:
    """Run Phase 1: Schema Analysis.
    
    The agent will:
    1. Parse connection details from instructions
    2. Save source_config.json to run_folder
    3. Analyze the schema and save catalog
    """
    from agents.schema_analyzer import SchemaAnalyzerAgent
    
    azure_config = get_azure_openai_config(reasoning_effort="low", run_folder=run_folder)
    
    # Create agent with instructions - it will extract connection details
    agent = SchemaAnalyzerAgent(
        instructions=instructions,
        output_dir=os.path.join(run_folder, "schema_agent"),
        run_folder=run_folder,  # For saving source_config.json
        llm_config={
            "deployment": azure_config["deployment"],
            "api_key": azure_config["api_key"],
            "base_url": azure_config["base_url"],
            "api_version": azure_config["api_version"],
            "reasoning_effort": azure_config["reasoning_effort"],
        },
        max_iterations=10,
        stable_rounds_required=2,
        kernel_timeout=300,
    )
    
    result = agent.analyze()
    
    if not result.get("success"):
        raise Exception(result.get("error", "Schema analysis failed"))
    
    # Load catalog
    schema_agent_dir = os.path.join(run_folder, "schema_agent")
    catalog_files = [
        f for f in os.listdir(schema_agent_dir)
        if f.startswith("schema_catalog_") and f.endswith(".json")
    ]
    catalog_files.sort(reverse=True)
    catalog_path = os.path.join(schema_agent_dir, catalog_files[0])
    
    with open(catalog_path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_phase2(migration_id: str, run_folder: str, catalog: dict, source_schema: str, 
               planner_config: PlannerConfig, instructions: str) -> dict:
    """Run Phase 2: Migration Planning."""
    from agents.planner import PlannerAgent, DebateRunner
    
    azure_config = get_azure_openai_config(run_folder=run_folder)
    
    # Build client configs for planners
    alpha_client_config = {
        "deployment": planner_config.alpha_model,
        "api_key": azure_config["api_key"],
        "base_url": azure_config["base_url"],
        "api_version": azure_config["api_version"],
        "reasoning_effort": "medium" if "codex" in planner_config.alpha_model else None,
    }
    
    beta_client_config = {
        "deployment": planner_config.beta_model,
        "api_key": azure_config["api_key"],
        "base_url": azure_config["base_url"],
        "api_version": azure_config["api_version"],
        "reasoning_effort": None,
    }
    
    # System prompts for planners
    alpha_system = """You are Planner Alpha, an expert database migration architect. 
Your role is to create detailed, safe, and efficient migration plans for moving databases to Snowflake.
Focus on correctness, data integrity, and providing complete DDL statements."""

    beta_system = """You are Planner Beta, a critical reviewer of database migration plans.
Your role is to find issues, suggest improvements, and ensure the migration plan is robust.
Focus on edge cases, performance concerns, and rollback strategies."""
    
    planner_alpha = PlannerAgent(
        name="Planner Alpha",
        client_config=alpha_client_config,
        system_prompt=alpha_system,
        max_tokens=16000,
    )
    
    planner_beta = PlannerAgent(
        name="Planner Beta",
        client_config=beta_client_config,
        system_prompt=beta_system,
        max_tokens=16000,
    )
    
    debate_runner = DebateRunner(
        planner_alpha=planner_alpha,
        planner_beta=planner_beta,
        output_dir=os.path.join(run_folder, "migration_plan"),
        max_rounds=planner_config.debate_rounds,
    )
    
    # Update status during debate
    def on_round(round_num, agent_name):
        migrations[migration_id]["phase2"]["round"] = round_num
        migrations[migration_id]["phase2"]["agent"] = agent_name
        add_log(migration_id, f"Debate round {round_num}: {agent_name}", "phase2")
    
    result = debate_runner.run_debate(catalog)
    
    return {"success": True, "rounds": planner_config.debate_rounds}


def run_phase3(migration_id: str, run_folder: str, catalog: dict,
               source_db: dict, target_db: dict, 
               worker_config: WorkerConfig, instructions: str) -> dict:
    """Run Phase 3: Migration Execution."""
    from agents.executor import MigrationExecutor
    
    azure_config = get_azure_openai_config(run_folder=run_folder)
    
    worker_llm_config = {
        "deployment": worker_config.model,
        "api_key": azure_config["api_key"],
        "base_url": azure_config["base_url"],
        "api_version": azure_config["api_version"],
        "reasoning_effort": worker_config.effort if "codex" in worker_config.model else None,
    }
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    execution_dir = os.path.join(run_folder, f"execution_{timestamp}")
    os.makedirs(execution_dir, exist_ok=True)
    
    executor = MigrationExecutor(
        worker_config=worker_llm_config,
        output_dir=execution_dir,
        source_db=source_db,
        target_db=target_db,
    )
    
    # Update status during execution
    def on_task(task_id, attempt):
        migrations[migration_id]["phase3"]["task"] = task_id
        migrations[migration_id]["phase3"]["attempt"] = attempt
        add_log(migration_id, f"Executing: {task_id} (attempt {attempt})", "phase3")
    
    result = executor.execute_migration(
        migration_plan="",
        catalog=catalog,
    )
    
    return result


# ============================================
# Static Files (Frontend)
# ============================================

# Serve frontend static files
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
async def serve_frontend():
    """Serve the frontend index.html"""
    index_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"status": "ok", "service": "Snowflake Migration API", "docs": "/docs"}

@app.get("/{filename:path}")
async def serve_static_files(filename: str):
    """Serve static files from frontend directory"""
    # Skip API routes
    if filename.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    
    file_path = os.path.join(os.path.dirname(__file__), "frontend", filename)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return FileResponse(file_path)
    
    # Fall back to index.html for SPA routing
    index_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    
    raise HTTPException(status_code=404, detail="Not found")


# ============================================
# Main
# ============================================

if __name__ == "__main__":
    import uvicorn
    print("Starting Snowflake Migration API Server...")
    print("Frontend available at: http://localhost:8000")
    print("API docs available at: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)

