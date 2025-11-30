"""Schema Analyzer agent that uses GPT-5.1-codex to dynamically generate all inspection code."""

from __future__ import annotations

import datetime
import json
import os
import queue
from typing import Any, Dict, List, Optional, Tuple

from jupyter_client import KernelManager

from azure_openai_client import AzureOpenAIChatCompletionClient, SimpleLLMMessage


class SchemaAnalyzerAgent:
    """Schema analyzer that uses LLM to generate ALL inspection code dynamically."""

    RESULT_START = "SCHEMA_ANALYSIS_RESULT_START"
    RESULT_END = "SCHEMA_ANALYSIS_RESULT_END"
    CONFIG_START = "SOURCE_CONFIG_START"
    CONFIG_END = "SOURCE_CONFIG_END"

    def __init__(self, config: Dict[str, Any] = None, **kwargs):
        # Support both dict config and keyword args
        if config is None:
            config = kwargs
        else:
            config = {**config, **kwargs}
            
        self.llm_config = config.get("llm_config", {})
        self.output_dir = config.get("output_dir")
        self.run_folder = config.get("run_folder")  # For saving source_config.json
        self.instructions = config.get("instructions", "")  # User instructions with connection details
        
        if not self.output_dir:
            raise ValueError("output_dir must be provided to SchemaAnalyzerAgent")
        os.makedirs(self.output_dir, exist_ok=True)

        self.max_iterations = int(config.get("max_iterations", 7))
        self.stable_rounds_required = int(config.get("stable_rounds_required", 2))
        self.kernel_timeout = int(config.get("kernel_timeout", 120))

        # Build LLM client
        self.llm_client = AzureOpenAIChatCompletionClient(
            deployment=self.llm_config.get("deployment", "enmapper-gpt-5.1-codex"),
            api_key=self.llm_config.get("api_key"),
            base_url=self.llm_config.get("base_url"),
            api_version=self.llm_config.get("api_version", "2024-12-01-preview"),
            reasoning_effort=self.llm_config.get("reasoning_effort", "medium"),
        )

    def analyze(self) -> Dict[str, Any]:
        """Main entry point - extracts connection from instructions and runs analysis."""
        if not self.instructions:
            return {"success": False, "error": "No instructions provided"}
        
        # Step 1: Extract connection details from instructions using LLM
        print("[SchemaAnalyzer] Extracting connection details from instructions...")
        db_config = self._extract_connection_from_instructions()
        
        if not db_config:
            return {"success": False, "error": "Could not extract connection details from instructions"}
        
        print(f"[SchemaAnalyzer] Extracted config: {db_config.get('type')} @ {db_config.get('host')}/{db_config.get('database')}.{db_config.get('schema')}")
        
        # Step 2: Save config for other phases
        if self.run_folder:
            config_path = os.path.join(self.run_folder, "source_config.json")
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(db_config, f, indent=2)
            print(f"[SchemaAnalyzer] Saved source config to {config_path}")
        
        # Step 3: Run schema analysis
        return self.analyze_schema(db_config)

    def _extract_connection_from_instructions(self) -> Optional[Dict[str, Any]]:
        """Use LLM to extract database connection details from user instructions."""
        
        system_prompt = f"""You are a database connection parser. Extract connection details from user instructions.

The user will provide instructions that contain database connection information. Extract the following fields:
- type: Database type. Supported types and their identifiers:
  * postgresql (or postgres, psql, pg)
  * mysql (or mariadb)
  * mongodb (or mongo)
  * sqlserver (or mssql, sql server)
  * teradata (or clearscape, teradata vantage)
  * oracle (or oracle db)
  * db2 (or ibm db2)
  * sqlite
  * redshift (or amazon redshift)
  * bigquery (or google bigquery)
  * snowflake
  * any other database type
  
- host: Database hostname or IP (look for "host:", "hostname:", "server:", "endpoint:", URLs)
- port: Port number (defaults: postgresql=5432, mysql=3306, mongodb=27017, sqlserver=1433, teradata=1025, oracle=1521)
- database: Database name (look for "database:", "db:", "dbname:")
- schema: Schema/namespace name (look for "schema:", or infer from context like "Airline database")
- user: Username (look for "user:", "username:", "login:")
- password: Password (look for "password:", "pwd:", "pass:")
- sslmode: SSL mode if specified (default: prefer for postgresql, require for cloud databases)

OUTPUT FORMAT:
Print exactly this:
{self.CONFIG_START}
{{
  "type": "...",
  "host": "...",
  "port": ...,
  "database": "...",
  "schema": "...",
  "user": "...",
  "password": "...",
  "sslmode": "..."
}}
{self.CONFIG_END}

If you cannot find a required field (host, user, password), use "MISSING" as the value.
For Teradata ClearScape URLs like "xxx.env.clearscape.teradata.com", extract the full hostname.
"""

        messages = [
            SimpleLLMMessage(role="system", content=system_prompt),
            SimpleLLMMessage(role="user", content=f"Extract connection details from these instructions:\n\n{self.instructions}")
        ]
        
        result = self.llm_client.create(messages, max_tokens=4000)
        response_text = result.content if hasattr(result, 'content') else ""
        
        print(f"[SchemaAnalyzer] Config extraction response: {response_text[:500]}...")
        
        # Parse the config from response
        if self.CONFIG_START in response_text and self.CONFIG_END in response_text:
            start_idx = response_text.index(self.CONFIG_START) + len(self.CONFIG_START)
            end_idx = response_text.index(self.CONFIG_END)
            json_str = response_text[start_idx:end_idx].strip()
            try:
                config = json.loads(json_str)
                # Validate required fields
                if config.get("host") == "MISSING" or config.get("user") == "MISSING":
                    print("[SchemaAnalyzer] WARNING: Missing required connection fields")
                return config
            except json.JSONDecodeError as e:
                print(f"[SchemaAnalyzer] Failed to parse config JSON: {e}")
                return None
        
        return None

    def analyze_schema(self, db_config: Dict[str, Any]) -> Dict[str, Any]:
        """Drive the schema analysis loop using LLM-generated code in a Jupyter kernel."""
        session = JupyterKernelSession(timeout=self.kernel_timeout)
        metadata: List[Dict[str, Any]] = []
        relationships: List[Dict[str, Any]] = []
        iteration = 0
        satisfied = False
        prev_fingerprint: Optional[Tuple] = None
        stable_rounds = 0
        session_started = False
        conversation: List[Dict[str, str]] = []
        last_error: Optional[str] = None
        last_output: Optional[str] = None

        try:
            session.start()
            session_started = True

            while iteration < self.max_iterations:
                iteration += 1
                print(f"[SchemaAnalyzer] Iteration {iteration} - asking LLM for inspection code")

                # Ask LLM to generate inspection code (NO predefined code)
                code = self._ask_llm_for_code(
                    db_config, iteration, last_error, last_output, conversation
                )

                if not code or len(code.strip()) < 50:
                    print(f"[SchemaAnalyzer] LLM returned insufficient code ({len(code) if code else 0} chars)")
                    last_error = "LLM returned empty or insufficient code. Please generate complete Python code."
                    conversation.append({
                        "role": "user",
                        "content": last_error
                    })
                    continue

                print(f"[SchemaAnalyzer] LLM generated code ({len(code)} chars)")
                print(f"[SchemaAnalyzer] Code preview:\n{code[:500]}...")

                # Execute code in kernel
                try:
                    raw_output = session.execute(code)
                    last_output = raw_output
                    print(f"[SchemaAnalyzer] Kernel output ({len(raw_output)} chars)")
                    print(f"[SchemaAnalyzer] Output preview: {raw_output[:300]}...")
                    payload = self._parse_kernel_output(raw_output)
                    last_error = None
                except Exception as exec_err:
                    last_error = str(exec_err)
                    last_output = None
                    print(f"[SchemaAnalyzer] Kernel error: {last_error[:500]}")
                    self._log_kernel_execution(iteration, code, f"ERROR: {last_error}")
                    conversation.append({
                        "role": "assistant",
                        "content": f"I generated this code:\n```python\n{code}\n```"
                    })
                    conversation.append({
                        "role": "user",
                        "content": f"The code failed with this error:\n{last_error}\n\nPlease analyze the error and generate fixed code."
                    })
                    continue

                metadata = payload.get("tables", [])
                relationships = payload.get("relationships", [])
                self._log_kernel_execution(iteration, code, raw_output)

                if not metadata:
                    last_error = "Code executed but returned no tables in the payload."
                    conversation.append({
                        "role": "assistant",
                        "content": f"I generated this code:\n```python\n{code}\n```"
                    })
                    conversation.append({
                        "role": "user",
                        "content": f"The code ran successfully but the payload contained no tables. Output was:\n{raw_output[:1000]}\n\nPlease investigate and fix the code."
                    })
                    continue

                # Check fingerprint for stability
                fingerprint = self._build_fingerprint(metadata)
                if fingerprint == prev_fingerprint:
                    stable_rounds += 1
                else:
                    stable_rounds = 1
                prev_fingerprint = fingerprint
                satisfied = stable_rounds >= self.stable_rounds_required

                print(
                    f"[SchemaAnalyzer] Iteration {iteration} complete "
                    f"(tables={len(metadata)}, stable_rounds={stable_rounds}, satisfied={satisfied})"
                )

                # Record successful iteration in conversation
                conversation.append({
                    "role": "assistant",
                    "content": f"I generated this code:\n```python\n{code}\n```"
                })
                conversation.append({
                    "role": "user",
                    "content": f"Code executed successfully! Found {len(metadata)} tables with {len(relationships)} relationships. "
                               f"Stable rounds: {stable_rounds}/{self.stable_rounds_required}. "
                               f"{'Analysis complete!' if satisfied else 'Please regenerate to verify stability.'}"
                })

                if satisfied:
                    break

        except Exception as exc:
            print(f"[SchemaAnalyzer] Schema inspection encountered an error: {exc}")
            import traceback
            traceback.print_exc()
            metadata = []
            relationships = []
            satisfied = False

        finally:
            if session_started:
                session.shutdown()

        analysis_file = (
            self._dump_markdown(metadata, relationships, db_config, iteration, satisfied, conversation)
            if metadata
            else None
        )
        schema_file = (
            self._dump_json(metadata, relationships, db_config, iteration, satisfied)
            if metadata
            else None
        )

        return {
            "success": bool(metadata),
            "analysis_file": analysis_file,
            "schema_file": schema_file,
            "output_dir": self.output_dir,
            "iterations": iteration,
            "satisfied": satisfied,
        }

    def _ask_llm_for_code(
        self,
        db_config: Dict[str, Any],
        iteration: int,
        last_error: Optional[str],
        last_output: Optional[str],
        conversation: List[Dict[str, str]],
    ) -> str:
        """Ask the LLM to generate Python code for schema inspection. NO predefined templates."""

        # Build a dynamic system prompt based on database type
        db_type = db_config.get("type", "unknown")
        
        system_prompt = f"""You are an expert database schema analyzer agent. Your task is to write Python code that will be executed in a Jupyter kernel to analyze a database schema.

DATABASE CONNECTION DETAILS:
- Type: {db_type}
- Host: {db_config.get('host', 'unknown')}
- Port: {db_config.get('port', 'unknown')}
- Database: {db_config.get('database', 'unknown')}
- Schema/Namespace: {db_config.get('schema', 'default')}
- Username: {db_config.get('user', 'unknown')}
- Password: {db_config.get('password', 'unknown')}
- SSL Mode: {db_config.get('sslmode', 'prefer')}
- Additional Config: {json.dumps({k: v for k, v in db_config.items() if k not in ['user', 'password', 'host', 'port', 'database', 'schema', 'type', 'sslmode']})}

YOUR TASK:
1. Write Python code that connects to this {db_type} database
2. Discover ALL tables/collections in the specified schema/namespace
3. For each table/collection, extract:
   - Table/collection name
   - All columns/fields with their data types
   - Primary key(s)
   - Foreign key relationships (if applicable)
   - Row/document count
   - 2 sample values per column/field (handle NULL, special types like Decimal, datetime, bytes, ObjectId, etc.)
4. Build a relationships list showing how tables/collections reference each other

OUTPUT REQUIREMENTS:
Your code MUST print the results in this EXACT format:
1. First print the marker: {self.RESULT_START}
2. Then print a JSON object with this structure:
   {{
     "schema": "<schema_name>",
     "database": "<database_name>",
     "host": "<host>",
     "tables": [
       {{
         "table_name": "<name>",
         "columns": [
           {{"name": "<col>", "type": "<type>", "nullable": true/false, "default": <value_or_null>}}
         ],
         "primary_key": ["<col1>", ...],
         "foreign_keys": [
           {{"constrained_columns": [...], "referred_table": "...", "referred_columns": [...], "options": {{"ondelete": "..."}}}}
         ],
         "row_count": <number>,
         "column_samples": [
           {{"column": "<col>", "samples": [<val1>, <val2>]}}
         ]
       }}
     ],
     "relationships": [
       {{"source_table": "...", "source_columns": [...], "target_table": "...", "target_columns": [...], "on_delete": "..."}}
     ]
   }}
3. Finally print the marker: {self.RESULT_END}

IMPORTANT GUIDELINES:

=== PACKAGE INSTALLATION (DO THIS FIRST!) ===
You have FULL POWER to install ANY Python package you need. Start your code with:
```
import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', package])

# Install required packages for your database type
# Examples:
# install_package('psycopg2-binary')     # PostgreSQL
# install_package('pymysql')             # MySQL
# install_package('pymongo')             # MongoDB
# install_package('pyodbc')              # SQL Server
# install_package('teradatasql')         # Teradata
# install_package('cx_Oracle')           # Oracle
# install_package('ibm_db')              # IBM DB2
# install_package('sqlalchemy')          # ORM (works with many DBs)
```

=== DATABASE-SPECIFIC GUIDANCE ===
- PostgreSQL: use psycopg2-binary or SQLAlchemy, set search_path, use sslmode
- MySQL: use pymysql or mysql-connector-python
- MongoDB: use pymongo, handle ObjectId conversion
- SQL Server: use pyodbc with appropriate ODBC driver
- Teradata/ClearScape: use teradatasql with SIMPLE connection params ONLY:
  ```
  teradatasql.connect(host=HOST, user=USER, password=PASSWORD, connect_timeout=30)
  ```
  DO NOT use encryptdata, logmech, or other advanced params for ClearScape environments.
  To list tables, query: SELECT TableName FROM DBC.Tables WHERE TableKind = 'T' AND DatabaseName = USER
  The schema name in ClearScape is often the username (demo_user), not a separate database name.
- Oracle: use cx_Oracle or oracledb
- Other databases: figure out the best approach - YOU CAN INSTALL ANY PACKAGE

=== OTHER GUIDELINES ===
- Handle connection errors gracefully
- Convert non-JSON-serializable types (Decimal, datetime, bytes, ObjectId, etc.) to JSON-compatible formats
- Use proper escaping for schema/table names with special characters

Generate ONLY executable Python code. No explanations, no markdown formatting around the code itself.
The code will be executed directly in a Jupyter kernel."""

        messages = [SimpleLLMMessage(role="system", content=system_prompt)]

        if iteration == 1:
            messages.append(SimpleLLMMessage(
                role="user",
                content=f"Generate Python code to analyze the {db_type} database schema. Remember to print the JSON result between the markers {self.RESULT_START} and {self.RESULT_END}."
            ))
        else:
            # Add recent conversation history (last 4 exchanges max to stay within context)
            for msg in conversation[-8:]:
                messages.append(SimpleLLMMessage(role=msg["role"], content=msg["content"]))

            if last_error:
                messages.append(SimpleLLMMessage(
                    role="user",
                    content=f"The previous code failed. Please fix it and generate new code.\n\nError: {last_error}"
                ))
            else:
                messages.append(SimpleLLMMessage(
                    role="user",
                    content="Please regenerate the inspection code to verify the schema analysis is stable and complete."
                ))

        # Log the request
        print(f"[SchemaAnalyzer] Sending {len(messages)} messages to LLM")
        for i, msg in enumerate(messages):
            preview = msg.content[:200] if msg.content else "(empty)"
            print(f"[SchemaAnalyzer]   Message {i+1} ({msg.role}): {preview}...")

        result = self.llm_client.create(messages, max_tokens=32000)
        response_text = result.content if hasattr(result, 'content') else ""

        # Log response preview
        print(f"[SchemaAnalyzer] LLM response ({len(response_text)} chars)")
        if response_text:
            print(f"[SchemaAnalyzer] Response preview: {response_text[:300]}...")

        # Extract code from response (handle markdown code blocks)
        code = self._extract_code(response_text)
        return code

    def _extract_code(self, response: str) -> str:
        """Extract Python code from LLM response, handling markdown code blocks."""
        if not response:
            return ""

        response = response.strip()

        # Try to extract from markdown code block
        if "```python" in response:
            start = response.find("```python") + len("```python")
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()

        if "```" in response:
            start = response.find("```") + 3
            # Skip language identifier if present
            newline_pos = response.find("\n", start)
            if newline_pos > start and newline_pos - start < 20:
                start = newline_pos + 1
            end = response.find("```", start)
            if end > start:
                return response[start:end].strip()

        # If no code block markers, assume entire response is code
        # But strip any leading/trailing non-code text
        lines = response.split('\n')
        code_lines = []
        in_code = False
        for line in lines:
            # Heuristic: code usually starts with import, from, def, class, #, or variable assignment
            stripped = line.strip()
            if not in_code:
                if stripped.startswith(('import ', 'from ', 'def ', 'class ', '#', 'db_', 'DB_', 'config', 'CONFIG')):
                    in_code = True
                elif '=' in stripped and not stripped.startswith(('Note:', 'Warning:', 'Error:')):
                    in_code = True
            if in_code:
                code_lines.append(line)

        return '\n'.join(code_lines).strip() if code_lines else response

    def _parse_kernel_output(self, raw_output: str) -> Dict[str, Any]:
        """Extract the JSON payload emitted by the kernel script."""
        start_idx = raw_output.find(self.RESULT_START)
        end_idx = raw_output.find(self.RESULT_END)
        if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
            raise RuntimeError(
                f"Unable to parse schema payload from kernel output. "
                f"Expected markers '{self.RESULT_START}' and '{self.RESULT_END}' not found or in wrong order. "
                f"Output preview: {raw_output[:500]}"
            )
        json_text = raw_output[start_idx + len(self.RESULT_START):end_idx].strip()
        try:
            return json.loads(json_text)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON from kernel output: {e}\nJSON text: {json_text[:500]}")

    def _log_kernel_execution(self, iteration: int, code: str, raw_output: str) -> None:
        """Persist the generated code and kernel output for traceability."""
        run_dir = os.path.join(self.output_dir, f"iteration_{iteration}")
        os.makedirs(run_dir, exist_ok=True)
        code_path = os.path.join(run_dir, "kernel_inspection.py")
        output_path = os.path.join(run_dir, "kernel_output.log")

        with open(code_path, "w", encoding="utf-8") as code_file:
            code_file.write(code)

        with open(output_path, "w", encoding="utf-8") as out_file:
            out_file.write(raw_output)

    def _build_fingerprint(self, metadata: List[Dict[str, Any]]) -> Tuple:
        """Build a fingerprint of the latest schema snapshot for convergence checks."""
        fingerprint = []
        for table in sorted(metadata, key=lambda t: t.get("table_name", "")):
            fk_signature = tuple(
                (
                    fk.get("referred_table"),
                    tuple(fk.get("constrained_columns", [])),
                    tuple(fk.get("referred_columns", [])),
                )
                for fk in table.get("foreign_keys", [])
            )
            fingerprint.append(
                (
                    table.get("table_name", ""),
                    table.get("row_count"),
                    tuple(col.get("name", "") for col in table.get("columns", [])),
                    tuple(table.get("primary_key", [])),
                    fk_signature,
                )
            )
        return tuple(fingerprint)

    def _dump_markdown(
        self,
        metadata: List[Dict[str, Any]],
        relationships: List[Dict[str, Any]],
        db_config: Dict[str, Any],
        iterations: int,
        satisfied: bool,
        conversation: List[Dict[str, str]],
    ) -> str:
        """Write a markdown report summarizing the schema and LLM conversation."""
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"schema_analysis_{timestamp}.md"
        path = os.path.join(self.output_dir, filename)

        db_type = db_config.get("type", "unknown")

        lines = [
            f"# Schema Analysis - {db_config.get('schema', 'default')}",
            "",
            f"- Database Type: {db_type}",
            f"- Host: {db_config.get('host')}:{db_config.get('port')}",
            f"- Database: {db_config.get('database')}",
            f"- Schema/Namespace: {db_config.get('schema', 'default')}",
            f"- Tables/Collections Detected: {len(metadata)}",
            f"- LLM Iterations: {iterations}",
            f"- Converged (satisfied): {satisfied}",
            f"- Model: {self.llm_config.get('deployment', 'enmapper-gpt-5.1-codex')}",
            f"- Reasoning Effort: {self.llm_config.get('reasoning_effort', 'medium')}",
            f"- Generated at UTC: {datetime.datetime.utcnow().isoformat()}",
            "",
            "## Tables/Collections",
        ]

        for table_info in metadata:
            table_name = table_info.get("table_name", "unknown")
            lines.append(f"### {table_name}")
            lines.append(f"- Row/Document count: {table_info.get('row_count', 'unknown')}")
            pk = table_info.get("primary_key") or []
            lines.append(f"- Primary key: {', '.join(pk) if pk else 'None'}")
            lines.append("- Columns/Fields:")
            samples_map = {
                entry.get("column", ""): entry.get("samples", [])
                for entry in table_info.get("column_samples", [])
            }
            for column in table_info.get("columns", []):
                col_name = column.get("name", "unknown")
                col_type = column.get("type", "unknown")
                col_line = f"  - `{col_name}` ({col_type})"
                if not column.get("nullable", True):
                    col_line += " [NOT NULL]"
                if column.get("default") is not None:
                    col_line += f" default {column['default']}"
                lines.append(col_line)
                sample_values = samples_map.get(col_name, [])
                if sample_values:
                    formatted_samples = ", ".join(repr(value) for value in sample_values[:2])
                    lines.append(f"    - Samples: {formatted_samples}")
            lines.append("")

        if relationships:
            lines.append("## Relationships")
            for rel in relationships:
                source = rel.get("source_table", "unknown")
                target = rel.get("target_table", "unknown")
                source_cols = rel.get("source_columns", [])
                target_cols = rel.get("target_columns", [])
                on_delete = rel.get("on_delete") or "default"
                lines.append(
                    f"- `{source}`.{','.join(source_cols)} -> "
                    f"`{target}`.{','.join(target_cols)} (ON DELETE {on_delete})"
                )
            lines.append("")

        lines.append("## LLM Conversation Log")
        lines.append("")
        for i, msg in enumerate(conversation):
            role = msg.get("role", "unknown").upper()
            content = msg.get("content", "")
            # Truncate very long messages
            if len(content) > 1000:
                content = content[:1000] + "\n... (truncated)"
            lines.append(f"### Turn {i+1} ({role})")
            lines.append("```")
            lines.append(content)
            lines.append("```")
            lines.append("")

        lines.append("## Notes")
        lines.append(
            f"This schema analysis was performed dynamically by {self.llm_config.get('deployment', 'GPT-5.1-codex')} "
            f"with {self.llm_config.get('reasoning_effort', 'medium')} reasoning effort. "
            "The LLM generated ALL inspection code from scratch based on the database type and connection details. "
            "No predefined templates were used."
        )

        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        return filename

    def _dump_json(
        self,
        metadata: List[Dict[str, Any]],
        relationships: List[Dict[str, Any]],
        db_config: Dict[str, Any],
        iterations: int,
        satisfied: bool,
    ) -> str:
        """Serialize the schema metadata for downstream agents."""
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"schema_catalog_{timestamp}.json"
        path = os.path.join(self.output_dir, filename)

        payload = {
            "database_type": db_config.get("type", "unknown"),
            "schema": db_config.get("schema", "default"),
            "database": db_config.get("database"),
            "host": db_config.get("host"),
            "tables": metadata,
            "relationships": relationships,
            "generated_at": datetime.datetime.utcnow().isoformat(),
            "iterations": iterations,
            "satisfied": satisfied,
            "llm_model": self.llm_config.get("deployment"),
            "llm_reasoning": self.llm_config.get("reasoning_effort"),
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        return filename


class JupyterKernelSession:
    """Light wrapper around a local IPython kernel for executing code blocks."""

    def __init__(self, kernel_name: str = "python3", timeout: int = 120):
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
