"""Phase 3: Migration Execution - Boss and Worker agents."""

from __future__ import annotations

import datetime
import json
import os
import queue
from typing import Any, Dict, List, Optional

from jupyter_client import KernelManager

from azure_openai_client import AzureOpenAIChatCompletionClient, SimpleLLMMessage


class JupyterKernelSession:
    """Light wrapper around a local IPython kernel for executing code blocks."""

    def __init__(self, kernel_name: str = "python3", timeout: int = 600):
        self.kernel_name = kernel_name
        self.timeout = timeout
        self.km: Optional[KernelManager] = None
        self.kc = None
        self._started = False

    def start(self) -> None:
        """Start the kernel and open communication channels."""
        if self._started:
            return
        self.km = KernelManager(kernel_name=self.kernel_name)
        self.km.start_kernel()
        self.kc = self.km.client()
        self.kc.start_channels()
        self.kc.wait_for_ready(timeout=self.timeout)
        self._started = True

    def execute(self, code: str) -> str:
        """Execute the provided code and return the text output."""
        if not self._started or not self.kc:
            raise RuntimeError("Kernel session has not been started.")

        msg_id = self.kc.execute(code)
        output_chunks: List[str] = []

        while True:
            try:
                msg = self.kc.get_iopub_msg(timeout=self.timeout)
            except queue.Empty:
                raise TimeoutError("Jupyter kernel did not respond within timeout.")

            if msg.get("parent_header", {}).get("msg_id") != msg_id:
                continue

            msg_type = msg["header"]["msg_type"]
            content = msg.get("content", {})
            if msg_type == "stream":
                output_chunks.append(content.get("text", ""))
            elif msg_type == "execute_result":
                payload_text = content.get("data", {}).get("text/plain")
                if payload_text:
                    output_chunks.append(payload_text)
            elif msg_type == "error":
                traceback = "\n".join(content.get("traceback", []))
                raise RuntimeError(f"Kernel execution error:\n{traceback}")
            elif msg_type == "status" and content.get("execution_state") == "idle":
                break

        return "".join(output_chunks)

    def shutdown(self) -> None:
        """Stop the kernel and close the channels."""
        if self.kc:
            try:
                self.kc.stop_channels()
            except Exception:
                pass
            self.kc = None
        if self.km:
            try:
                self.km.shutdown_kernel(now=False)
            except Exception:
                pass
            self.km = None
        self._started = False


class WorkerAgent:
    """Worker agent that executes migration mega-tasks with retry logic."""

    RESULT_START = "TASK_RESULT_START"
    RESULT_END = "TASK_RESULT_END"

    def __init__(
        self,
        name: str,
        llm_config: Dict[str, Any],
        output_dir: str,
        max_attempts: int = 7,
        kernel_timeout: int = 600,
    ):
        self.name = name
        self.output_dir = output_dir
        self.max_attempts = max_attempts
        self.kernel_timeout = kernel_timeout
        os.makedirs(output_dir, exist_ok=True)

        self.llm_client = AzureOpenAIChatCompletionClient(
            deployment=llm_config.get("deployment", "enmapper-gpt-5.1-codex"),
            api_key=llm_config.get("api_key"),
            base_url=llm_config.get("base_url"),
            api_version=llm_config.get("api_version", "2024-12-01-preview"),
            reasoning_effort=llm_config.get("reasoning_effort", "medium"),
        )

        self.kernel: Optional[JupyterKernelSession] = None

    def start_kernel(self) -> None:
        """Start the Jupyter kernel for code execution."""
        if self.kernel is None:
            self.kernel = JupyterKernelSession(timeout=self.kernel_timeout)
            self.kernel.start()
            print(f"[{self.name}] Jupyter kernel started")

    def shutdown_kernel(self) -> None:
        """Shutdown the Jupyter kernel."""
        if self.kernel:
            self.kernel.shutdown()
            self.kernel = None
            print(f"[{self.name}] Jupyter kernel stopped")

    def execute_task(
        self,
        task_id: str,
        task_description: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a mega-task with retry logic."""
        print(f"\n[{self.name}] Starting task: {task_id}")
        print(f"[{self.name}] Description: {task_description[:300]}...")

        task_dir = os.path.join(self.output_dir, f"task_{task_id}")
        os.makedirs(task_dir, exist_ok=True)

        conversation: List[Dict[str, str]] = []
        last_error: Optional[str] = None
        last_output: Optional[str] = None
        success = False
        result_data: Dict[str, Any] = {}
        attempt = 0

        for attempt in range(1, self.max_attempts + 1):
            print(f"[{self.name}] Attempt {attempt}/{self.max_attempts}")

            code = self._ask_llm_for_code(
                task_id, task_description, context, attempt, last_error, last_output, conversation
            )

            if not code or len(code.strip()) < 50:
                print(f"[{self.name}] LLM returned insufficient code")
                last_error = "LLM returned empty or insufficient code"
                conversation.append({
                    "role": "user",
                    "content": f"Attempt {attempt}: {last_error}. Please provide complete Python code."
                })
                continue

            self._log_attempt(task_dir, attempt, "code", code)
            print(f"[{self.name}] Generated code ({len(code)} chars)")

            try:
                raw_output = self.kernel.execute(code)
                last_output = raw_output
                self._log_attempt(task_dir, attempt, "output", raw_output)
                print(f"[{self.name}] Execution output ({len(raw_output)} chars)")

                result_data = self._parse_task_result(raw_output)
                if result_data.get("success"):
                    success = True
                    print(f"[{self.name}] Task completed successfully on attempt {attempt}")
                    break
                else:
                    last_error = result_data.get("error") or result_data.get("message") or "Task reported failure"
                    print(f"[{self.name}] Task reported failure: {str(last_error)[:300]}")

            except Exception as exec_err:
                last_error = str(exec_err)
                last_output = None
                self._log_attempt(task_dir, attempt, "error", last_error)
                print(f"[{self.name}] Execution error: {last_error[:400]}")

            conversation.append({
                "role": "assistant",
                "content": f"Generated code:\n```python\n{code[:2000]}...\n```"
            })
            conversation.append({
                "role": "user",
                "content": f"Attempt {attempt} failed.\nError: {last_error}\nOutput: {(last_output or '')[:500]}\n\nPlease fix and try again."
            })

        return {
            "task_id": task_id,
            "success": success,
            "attempts": attempt,
            "result": result_data,
            "last_error": last_error if not success else None,
            "task_dir": task_dir,
        }

    def _ask_llm_for_code(
        self,
        task_id: str,
        task_description: str,
        context: Dict[str, Any],
        attempt: int,
        last_error: Optional[str],
        last_output: Optional[str],
        conversation: List[Dict[str, str]],
    ) -> str:
        """Ask the LLM to generate Python code for the mega-task."""
        source_db = context.get("source_db", {})
        target_db = context.get("target_db", {})
        
        # Get table info from catalog
        catalog = context.get("catalog", {})
        tables = catalog.get("tables", [])
        table_names = [t.get("table_name") for t in tables]

        # Determine source database type
        source_type = source_db.get('type', 'postgresql').lower()
        
        # Build source-specific connection info
        source_info = f"""=== SOURCE DATABASE ({source_type.upper()}) ===
Type: {source_type}
Host: {source_db.get('host')}
Port: {source_db.get('port', '')}
Database: {source_db.get('database', '')}
Schema: {source_db.get('schema', '')}
User: {source_db.get('user')}
Password: {source_db.get('password')}"""

        # Add type-specific hints
        source_hints = ""
        if source_type == "postgresql":
            source_hints = "\nUse psycopg2-binary. Tables are in the schema above, NOT 'public'."
        elif source_type == "teradata":
            source_hints = """
=== TERADATA CONNECTION - CRITICAL ===
Use teradatasql package with ONLY these parameters (NO port, NO encryptdata, NO logmech):
```
import teradatasql
conn = teradatasql.connect(
    host='hostname.env.clearscape.teradata.com',
    user='username',
    password='password',
    connect_timeout=30
)
```
NEVER add port=1025 or any other extra parameters - it will cause 'Unable to parse JSON connection parameters' error!
Query tables with: SELECT * FROM username.tablename (use the user/schema name as database prefix)
"""
        elif source_type == "mysql":
            source_hints = "\nUse pymysql or mysql-connector-python."
        elif source_type == "mongodb":
            source_hints = "\nUse pymongo. Handle ObjectId conversion to strings."
        elif source_type == "sqlserver":
            source_hints = "\nUse pyodbc with appropriate SQL Server driver."
        elif source_type == "oracle":
            source_hints = "\nUse cx_Oracle or oracledb package."

        system_prompt = f"""You are a Worker Agent executing database migration tasks. Write complete, executable Python code for a Jupyter kernel.

TASK: {task_description}

{source_info}{source_hints}

=== TARGET DATABASE (Snowflake) ===
Account: {target_db.get('account')}
User: {target_db.get('user')}
Password: {target_db.get('password')}
Warehouse: {target_db.get('warehouse', 'COMPUTE_WH')}
Database: {target_db.get('database')}
Schema: {target_db.get('schema', 'PUBLIC')}

=== CRITICAL SNOWFLAKE RULES ===
1. ALWAYS use UPPERCASE identifiers WITHOUT quotes for Snowflake
   CORRECT: CREATE TABLE ECOMMERCE.CUSTOMERS ...
   WRONG: CREATE TABLE "ecommerce"."customers" ...
   
2. Schema and table names must be UPPERCASE: ECOMMERCE, CUSTOMERS, ORDERS, etc.

3. When connecting to Snowflake, set the schema explicitly:
   conn = snowflake.connector.connect(..., schema='{target_db.get('schema', 'PUBLIC').upper()}')

=== SOURCE TABLES (in schema: {source_db.get('schema', 'default')}) ===
{table_names}

=== PACKAGE INSTALLATION (DO THIS FIRST!) ===
You have FULL POWER to install ANY Python package you need. Start your code with:
```
import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', package])

# Install required packages for {source_type} -> Snowflake migration
# Examples: teradatasql, psycopg2-binary, pymysql, pymongo, cx_Oracle, pyodbc
install_package('snowflake-connector-python')  # Always needed for target
# Add source database package as needed!
```

=== CODE REQUIREMENTS ===
1. ALWAYS install packages first using the install_package function above
2. Handle all errors with try/except
3. Close all connections in finally block
4. Print result using EXACT format:
   print('{self.RESULT_START}')
   print(json.dumps({{"success": True/False, "message": "...", "data": {{...}}}}))
   print('{self.RESULT_END}')

Generate ONLY executable Python code. No markdown explanations outside code blocks."""

        messages = [SimpleLLMMessage(role="system", content=system_prompt)]

        if attempt == 1:
            messages.append(SimpleLLMMessage(
                role="user",
                content=f"Execute this task:\n\n{task_description}\n\nRemember:\n- Source schema is '{source_db.get('schema')}' (NOT 'public')\n- Use UPPERCASE for all Snowflake identifiers\n- Print result between {self.RESULT_START} and {self.RESULT_END}"
            ))
        else:
            for msg in conversation[-4:]:
                messages.append(SimpleLLMMessage(role=msg["role"], content=msg["content"]))

            if last_error:
                error_context = f"Previous attempt failed:\nError: {last_error[:1500]}"
                if last_output:
                    error_context += f"\n\nOutput:\n{last_output[:800]}"
                messages.append(SimpleLLMMessage(
                    role="user",
                    content=f"{error_context}\n\nFix the code. Remember: source schema is '{source_db.get('schema')}', use UPPERCASE for Snowflake."
                ))

        result = self.llm_client.create(messages, max_tokens=16000)
        response_text = result.content if isinstance(result.content, str) else str(result.content or "")

        return self._extract_code(response_text)

    def _extract_code(self, response: str) -> str:
        """Extract Python code from LLM response."""
        if not response:
            return ""

        response = response.strip()

        if "```python" in response:
            start = response.find("```python") + len("```python")
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()

        if "```" in response:
            start = response.find("```") + 3
            newline_pos = response.find("\n", start)
            if newline_pos > start and newline_pos - start < 20:
                start = newline_pos + 1
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()

        return response

    def _parse_task_result(self, raw_output: str) -> Dict[str, Any]:
        """Parse the task result from kernel output."""
        start_idx = raw_output.find(self.RESULT_START)
        end_idx = raw_output.find(self.RESULT_END)

        if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
            lower_output = raw_output.lower()
            if "error" in lower_output or "exception" in lower_output or "traceback" in lower_output:
                return {"success": False, "error": f"Execution error: {raw_output[:1000]}"}
            # Check for success indicators
            if "success" in lower_output or "completed" in lower_output or "loaded" in lower_output:
                return {"success": True, "message": "Task appears successful based on output", "data": {"raw_output": raw_output[:500]}}
            return {"success": False, "error": f"No result markers found. Output: {raw_output[:500]}"}

        json_text = raw_output[start_idx + len(self.RESULT_START):end_idx].strip()
        try:
            return json.loads(json_text)
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"Failed to parse result JSON: {e}. Raw: {json_text[:300]}"}

    def _log_attempt(self, task_dir: str, attempt: int, log_type: str, content: str) -> None:
        """Log an attempt's code/output/error."""
        attempt_dir = os.path.join(task_dir, f"attempt_{attempt}")
        os.makedirs(attempt_dir, exist_ok=True)
        path = os.path.join(attempt_dir, f"{log_type}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)


class MigrationExecutor:
    """Orchestrates Phase 3 migration with consolidated mega-tasks."""

    def __init__(
        self,
        worker_config: Dict[str, Any],
        output_dir: str,
        source_db: Dict[str, Any],
        target_db: Dict[str, Any],
    ):
        self.output_dir = output_dir
        self.source_db = source_db
        self.target_db = target_db
        os.makedirs(output_dir, exist_ok=True)

        self.worker = WorkerAgent(
            name="Worker Agent",
            llm_config=worker_config,
            output_dir=os.path.join(output_dir, "worker"),
            max_attempts=7,
            kernel_timeout=600,
        )

        self.execution_log: List[Dict[str, Any]] = []

    def _build_mega_tasks(self, catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build consolidated mega-tasks from catalog."""
        tables = catalog.get("tables", [])
        source_schema = self.source_db.get("schema", "public")
        target_schema = self.target_db.get("schema", "PUBLIC").upper()
        
        # Build table dependency order (tables with no FKs first)
        table_deps = {}
        for t in tables:
            table_name = t.get("table_name")
            fks = t.get("foreign_keys", [])
            deps = set()
            for fk in fks:
                ref_table = fk.get("referred_table")
                if ref_table and ref_table != table_name:
                    deps.add(ref_table)
            table_deps[table_name] = deps
        
        # Topological sort
        ordered_tables = []
        remaining = set(table_deps.keys())
        while remaining:
            for t in list(remaining):
                if not (table_deps[t] & remaining):
                    ordered_tables.append(t)
                    remaining.remove(t)
                    break
            else:
                # Circular dependency - just add remaining
                ordered_tables.extend(remaining)
                break
        
        # Build table info for prompts
        table_info = []
        for t in tables:
            cols = []
            for c in t.get("columns", []):
                col_info = {
                    "name": c.get("name"),
                    "type": c.get("type"),
                    "nullable": c.get("nullable", True),
                }
                cols.append(col_info)
            table_info.append({
                "name": t.get("table_name"),
                "columns": cols,
                "primary_key": t.get("primary_key", []),
                "foreign_keys": t.get("foreign_keys", []),
                "row_count": t.get("row_count", 0),
            })
        
        table_info_json = json.dumps(table_info, indent=2)
        
        # Build 4 mega-tasks
        tasks = [
            {
                "task_id": "1_setup_and_create_tables",
                "description": f"""MEGA-TASK 1: Setup Snowflake and Create All Tables

Create the schema and ALL tables in Snowflake in the correct order.

TARGET SCHEMA: {target_schema} (use UPPERCASE, no quotes)
TARGET DATABASE: {self.target_db.get('database')}

TABLE CREATION ORDER (respecting foreign key dependencies):
{ordered_tables}

TABLE DEFINITIONS:
{table_info_json}

STEPS:
1. Connect to Snowflake
2. Create schema {target_schema} if not exists
3. Create each table with appropriate column types:
   - INTEGER/SERIAL -> NUMBER(38,0)
   - VARCHAR(n) -> VARCHAR(n)
   - NUMERIC(p,s) -> NUMBER(p,s)
   - TIMESTAMP -> TIMESTAMP_NTZ
   - TEXT -> VARCHAR(16777216)
   - DATE -> DATE
   - BOOLEAN -> BOOLEAN
4. Add PRIMARY KEY constraints
5. Verify all tables exist

DO NOT add foreign key constraints yet - that comes later.
Use UPPERCASE for all Snowflake identifiers (schema, table, column names).""",
            },
            {
                "task_id": "2_extract_and_load_all_data",
                "description": f"""MEGA-TASK 2: Extract ALL Data from PostgreSQL and Load into Snowflake

Extract data from ALL tables in PostgreSQL and load into Snowflake.

SOURCE: PostgreSQL schema '{source_schema}' (NOT 'public')
TARGET: Snowflake schema {target_schema}

TABLES TO MIGRATE (in this order to respect FK dependencies):
{ordered_tables}

TABLE ROW COUNTS:
{json.dumps({t.get('table_name'): t.get('row_count', 0) for t in tables}, indent=2)}

STEPS:
1. Connect to PostgreSQL (use schema '{source_schema}')
2. Connect to Snowflake
3. For each table in order:
   a. SELECT * FROM {source_schema}.<table_name>
   b. TRUNCATE the Snowflake table first (in case of re-run)
   c. INSERT all rows into Snowflake {target_schema}.<TABLE_NAME>
   d. Print progress: "Loaded X rows into <TABLE_NAME>"
4. Verify row counts match

IMPORTANT:
- Source tables are in schema '{source_schema}', NOT 'public'
- Use UPPERCASE for Snowflake table names
- Handle datetime/timestamp conversions properly
- Batch inserts for performance (executemany)""",
            },
            {
                "task_id": "3_add_foreign_keys",
                "description": f"""MEGA-TASK 3: Add Foreign Key Constraints in Snowflake

Add all foreign key constraints to the tables.

TARGET SCHEMA: {target_schema}

FOREIGN KEY RELATIONSHIPS:
{json.dumps([
    {
        "table": t.get("table_name").upper(),
        "foreign_keys": [
            {
                "columns": fk.get("constrained_columns"),
                "references": f"{fk.get('referred_table', '').upper()}({', '.join(fk.get('referred_columns', []))})"
            }
            for fk in t.get("foreign_keys", [])
        ]
    }
    for t in tables if t.get("foreign_keys")
], indent=2)}

STEPS:
1. Connect to Snowflake
2. For each table with foreign keys:
   ALTER TABLE {target_schema}.<TABLE> ADD CONSTRAINT fk_<name>
   FOREIGN KEY (<columns>) REFERENCES {target_schema}.<ref_table>(<ref_columns>)
3. Note: Snowflake FKs are not enforced but serve as documentation

Use UPPERCASE for all identifiers.""",
            },
            {
                "task_id": "4_validate_migration",
                "description": f"""MEGA-TASK 4: Validate the Migration

Verify that all data was migrated correctly.

SOURCE SCHEMA: {source_schema} (PostgreSQL)
TARGET SCHEMA: {target_schema} (Snowflake)

EXPECTED ROW COUNTS:
{json.dumps({t.get('table_name'): t.get('row_count', 0) for t in tables}, indent=2)}

VALIDATION STEPS:
1. Connect to both PostgreSQL and Snowflake
2. For each table:
   a. Get row count from PostgreSQL {source_schema}.<table>
   b. Get row count from Snowflake {target_schema}.<TABLE>
   c. Compare and report any mismatches
3. Report overall validation status

Print a summary table showing:
- Table name
- Source rows
- Target rows  
- Status (OK/MISMATCH)

The task succeeds if ALL row counts match.""",
            },
        ]
        
        return tasks

    def execute_migration(
        self,
        migration_plan: str,
        catalog: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute the migration using consolidated mega-tasks."""
        print("=" * 80)
        print("PHASE 3: Migration Execution (Optimized)")
        print("=" * 80)
        print(f"Source: {self.source_db.get('host')}/{self.source_db.get('database')}.{self.source_db.get('schema')}")
        print(f"Target: {self.target_db.get('account')}/{self.target_db.get('database')}.{self.target_db.get('schema')}")
        print("-" * 80)

        start_time = datetime.datetime.utcnow()

        try:
            self.worker.start_kernel()

            # Build mega-tasks from catalog
            tasks = self._build_mega_tasks(catalog)
            print(f"\nCreated {len(tasks)} mega-tasks")
            for t in tasks:
                print(f"  - {t['task_id']}")

            context = {
                "source_db": self.source_db,
                "target_db": self.target_db,
                "catalog": catalog,
            }

            completed_tasks: List[str] = []
            failed_tasks: List[str] = []

            for i, task in enumerate(tasks):
                task_id = task.get("task_id")
                description = task.get("description")

                print(f"\n{'='*70}")
                print(f"MEGA-TASK {i+1}/{len(tasks)}: {task_id}")
                print("=" * 70)

                result = self.worker.execute_task(
                    task_id=task_id,
                    task_description=description,
                    context=context,
                )

                self.execution_log.append({
                    "task_id": task_id,
                    "status": "success" if result.get("success") else "failed",
                    "attempts": result.get("attempts"),
                    "result": result.get("result"),
                })

                if result.get("success"):
                    completed_tasks.append(task_id)
                    print(f"\n[SUCCESS] {task_id} completed in {result.get('attempts')} attempt(s)")
                else:
                    failed_tasks.append(task_id)
                    print(f"\n[FAILED] {task_id} failed after {result.get('attempts')} attempts")
                    print(f"Error: {result.get('last_error', 'Unknown')[:300]}")
                    # Continue with next task even if this one failed

        finally:
            self.worker.shutdown_kernel()

        end_time = datetime.datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        report = {
            "success": len(failed_tasks) == 0,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_tasks": len(tasks),
            "completed_tasks": len(completed_tasks),
            "failed_tasks": len(failed_tasks),
            "completed_task_ids": completed_tasks,
            "failed_task_ids": failed_tasks,
            "execution_log": self.execution_log,
        }

        report_path = os.path.join(self.output_dir, "execution_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print("\n" + "=" * 80)
        print("Migration Execution Complete")
        print("=" * 80)
        print(f"Duration: {duration:.1f} seconds")
        print(f"Tasks: {len(completed_tasks)}/{len(tasks)} completed")
        if failed_tasks:
            print(f"Failed: {failed_tasks}")
        print(f"Report: {report_path}")

        return report
