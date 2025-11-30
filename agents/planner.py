"""Planner agents and debate orchestration for Phase 2."""

from __future__ import annotations

import datetime
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from azure_openai_client import AzureOpenAIChatCompletionClient, SimpleLLMMessage


class PlannerAgent:
    """LLM-powered planner agent that generates migration plans."""

    def __init__(
        self,
        name: str,
        client_config: Dict[str, Any],
        system_prompt: str,
        max_tokens: int = 16000,
        max_retries: int = 3,
    ):
        self.name = name
        self.client = AzureOpenAIChatCompletionClient(
            deployment=client_config["deployment"],
            api_key=client_config["api_key"],
            base_url=client_config["base_url"],
            api_version=client_config["api_version"],
            reasoning_effort=client_config.get("reasoning_effort"),
        )
        self.system_prompt = system_prompt
        self.history: List[SimpleLLMMessage] = []
        self.max_tokens = max_tokens
        self.max_retries = max_retries

    def send_instruction(self, instruction: str) -> str:
        """Send a user instruction and get the assistant response."""
        # Build messages list fresh each time (system + history + new instruction)
        messages = [SimpleLLMMessage(role="system", content=self.system_prompt)]
        messages.extend(self.history)
        messages.append(SimpleLLMMessage(role="user", content=instruction))

        print(f"[{self.name}] Sending {len(messages)} messages to LLM...")
        if os.environ.get("AUTOGEN_DEBUG"):
            for i, msg in enumerate(messages):
                preview = msg.content[:150] if msg.content else "(empty)"
                print(f"[{self.name}]   Message {i+1} ({msg.role}): {preview}...")

        # Retry loop for empty responses
        response_content = ""
        for attempt in range(1, self.max_retries + 1):
            result = self.client.create(messages, max_tokens=self.max_tokens)
            response_content = result.content if isinstance(result.content, str) else str(result.content or "")

            if response_content.strip():
                print(f"[{self.name}] Got response ({len(response_content)} chars) on attempt {attempt}")
                break
            else:
                print(f"[{self.name}] Empty response on attempt {attempt}, retrying...")
                # Add a nudge to the messages for retry
                if attempt < self.max_retries:
                    messages.append(SimpleLLMMessage(
                        role="user",
                        content="Please provide your response. Do not return empty content."
                    ))

        if not response_content.strip():
            response_content = f"[{self.name} returned empty response after {self.max_retries} attempts]"
            print(f"[{self.name}] WARNING: All retry attempts returned empty content")

        # Log response preview
        print(f"[{self.name}] Response preview: {response_content[:300]}...")

        # Update history
        self.history.append(SimpleLLMMessage(role="user", content=instruction))
        self.history.append(SimpleLLMMessage(role="assistant", content=response_content))

        return response_content


class DebateRunner:
    """Coordinate the planner debate loop and produce a migration plan."""

    def __init__(
        self,
        planner_alpha: PlannerAgent,
        planner_beta: PlannerAgent,
        output_dir: str,
        max_rounds: int = 2,
    ):
        self.planner_alpha = planner_alpha
        self.planner_beta = planner_beta
        self.output_dir = output_dir
        self.max_rounds = max_rounds
        os.makedirs(output_dir, exist_ok=True)

    def run_debate(self, catalog: Dict[str, Any]) -> Dict[str, Any]:
        """Run the debate and return the final plan and supporting logs."""
        summary = self._summarize_catalog(catalog)
        conversation: List[Dict[str, str]] = []

        print("=" * 80)
        print("PHASE 2: Migration Planning Debate")
        print("=" * 80)
        print(f"Schema: {catalog.get('schema', 'unknown')}")
        print(f"Tables: {len(catalog.get('tables', []))}")
        print(f"Relationships: {len(catalog.get('relationships', []))}")
        print("-" * 80)

        # Initial prompt for Planner Alpha - be very explicit about what we want
        alpha_initial_prompt = f"""You are tasked with creating a migration plan for moving this database schema to Snowflake.

Here is the source schema catalog:

{summary}

Please provide a detailed migration plan that includes:

1. **Schema Creation Order**: List the tables in the order they should be created in Snowflake (considering foreign key dependencies)

2. **DDL Statements**: Provide the Snowflake CREATE TABLE statements for each table, including:
   - Column definitions with appropriate Snowflake data types
   - Primary key constraints
   - Foreign key constraints (if applicable)

3. **Data Type Mappings**: Document any data type conversions needed from the source database to Snowflake

4. **Data Loading Strategy**: Describe how data should be loaded:
   - Recommended load order (respecting FK constraints)
   - Batch sizes if applicable
   - Any transformations needed during load

5. **Validation Checks**: List specific validation queries to run after migration:
   - Row count comparisons
   - Data integrity checks
   - Referential integrity validation

Please be thorough and provide actual code/SQL where appropriate."""

        print("\n[Debate] Asking Planner Alpha for initial migration plan...")
        alpha_response = self.planner_alpha.send_instruction(alpha_initial_prompt)
        
        print(f"\n[Debate] Planner Alpha initial plan ({len(alpha_response)} chars):")
        print("-" * 40)
        print(alpha_response[:500] + "..." if len(alpha_response) > 500 else alpha_response)
        print("-" * 40)
        
        conversation.append({
            "speaker": self.planner_alpha.name,
            "message": alpha_response
        })

        # Log iteration
        self._log_iteration(0, "alpha_initial", alpha_initial_prompt, alpha_response)

        for round_number in range(1, self.max_rounds + 1):
            print(f"\n[Debate] Round {round_number} - Asking Planner Beta to critique...")
            
            beta_prompt = f"""Please review the following migration plan from Planner Alpha:

{alpha_response}

Provide a detailed critique focusing on:

1. **Completeness**: Are all tables and relationships covered? Any missing DDL?

2. **Safety**: Are there any risky operations? What could go wrong?

3. **Data Integrity**: Are the validation checks sufficient? What additional checks would you recommend?

4. **Performance**: Any concerns about the load strategy? Suggestions for optimization?

5. **Rollback Strategy**: What happens if something fails? How do we recover?

6. **Missing Elements**: What important aspects were not addressed?

Be specific and provide concrete suggestions for improvement."""

            beta_response = self.planner_beta.send_instruction(beta_prompt)
            
            print(f"\n[Debate] Planner Beta critique ({len(beta_response)} chars):")
            print("-" * 40)
            print(beta_response[:500] + "..." if len(beta_response) > 500 else beta_response)
            print("-" * 40)
            
            conversation.append({
                "speaker": self.planner_beta.name,
                "message": beta_response
            })

            self._log_iteration(round_number, "beta_critique", beta_prompt, beta_response)

            print(f"\n[Debate] Round {round_number} - Asking Planner Alpha to revise...")
            
            alpha_revision_prompt = f"""Planner Beta has provided the following critique of your migration plan:

{beta_response}

Please revise your migration plan to address these concerns. Specifically:

1. Address each critique point raised
2. Add any missing elements
3. Improve safety and validation where suggested
4. Clearly mark what changes you made

Provide the updated, complete migration plan."""

            alpha_response = self.planner_alpha.send_instruction(alpha_revision_prompt)
            
            print(f"\n[Debate] Planner Alpha revision ({len(alpha_response)} chars):")
            print("-" * 40)
            print(alpha_response[:500] + "..." if len(alpha_response) > 500 else alpha_response)
            print("-" * 40)
            
            conversation.append({
                "speaker": self.planner_alpha.name,
                "message": alpha_response
            })

            self._log_iteration(round_number, "alpha_revision", alpha_revision_prompt, alpha_response)

        print("\n" + "=" * 80)
        print("Debate complete!")
        print("=" * 80)

        return {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "schema": catalog.get("schema"),
            "database_type": catalog.get("database_type", "unknown"),
            "catalog_summary": summary,
            "final_plan": alpha_response,
            "final_critique": beta_response,
            "conversation": conversation,
            "run_rounds": self.max_rounds,
        }

    def _log_iteration(self, round_num: int, step: str, prompt: str, response: str) -> None:
        """Log each debate iteration for traceability."""
        iteration_dir = os.path.join(self.output_dir, f"round_{round_num}")
        os.makedirs(iteration_dir, exist_ok=True)

        prompt_path = os.path.join(iteration_dir, f"{step}_prompt.txt")
        response_path = os.path.join(iteration_dir, f"{step}_response.txt")

        with open(prompt_path, "w", encoding="utf-8") as f:
            f.write(prompt)
        with open(response_path, "w", encoding="utf-8") as f:
            f.write(response)

    def _summarize_catalog(self, catalog: Dict[str, Any]) -> str:
        """Build a detailed textual summary of the schema catalog."""
        lines: List[str] = [
            f"Database Type: {catalog.get('database_type', 'unknown')}",
            f"Schema: {catalog.get('schema', 'unknown')}",
            f"Host: {catalog.get('host', 'unknown')}",
            f"Database: {catalog.get('database', 'unknown')}",
            f"Generated at: {catalog.get('generated_at', 'unknown')}",
            "",
            "=" * 60,
            "TABLES",
            "=" * 60,
        ]

        for table in catalog.get("tables", []):
            table_name = table.get("table_name", "unknown")
            row_count = table.get("row_count", 0)
            pk = table.get("primary_key", [])
            pk_str = ", ".join(pk) if pk else "None"

            lines.append("")
            lines.append(f"TABLE: {table_name}")
            lines.append(f"  Row Count: {row_count}")
            lines.append(f"  Primary Key: {pk_str}")
            lines.append("  Columns:")

            for col in table.get("columns", []):
                col_name = col.get("name", "unknown")
                col_type = col.get("type", "unknown")
                nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
                default = col.get("default")
                default_str = f" DEFAULT {default}" if default else ""
                lines.append(f"    - {col_name}: {col_type} {nullable}{default_str}")

            # Add sample values
            samples = table.get("column_samples", [])
            if samples:
                lines.append("  Sample Values:")
                for sample in samples[:5]:  # Limit to first 5 columns
                    col = sample.get("column", "unknown")
                    vals = sample.get("samples", [])
                    if vals:
                        val_str = ", ".join(repr(v) for v in vals[:2])
                        lines.append(f"    - {col}: [{val_str}]")

            # Add foreign keys
            fks = table.get("foreign_keys", [])
            if fks:
                lines.append("  Foreign Keys:")
                for fk in fks:
                    src_cols = fk.get("constrained_columns", [])
                    ref_table = fk.get("referred_table", "unknown")
                    ref_cols = fk.get("referred_columns", [])
                    on_delete = fk.get("options", {}).get("ondelete", "NO ACTION") if isinstance(fk.get("options"), dict) else "NO ACTION"
                    lines.append(f"    - {','.join(src_cols)} -> {ref_table}({','.join(ref_cols)}) ON DELETE {on_delete}")

        # Add relationships summary
        relationships = catalog.get("relationships", [])
        if relationships:
            lines.append("")
            lines.append("=" * 60)
            lines.append("RELATIONSHIPS")
            lines.append("=" * 60)
            for rel in relationships:
                src = rel.get("source_table", "unknown")
                src_cols = rel.get("source_columns", [])
                tgt = rel.get("target_table", "unknown")
                tgt_cols = rel.get("target_columns", [])
                on_delete = rel.get("on_delete", "NO ACTION")
                lines.append(f"  {src}.{','.join(src_cols)} -> {tgt}.{','.join(tgt_cols)} (ON DELETE {on_delete})")

        return "\n".join(lines)
