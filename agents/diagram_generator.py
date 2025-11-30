"""Diagram Generator Agent - Creates Mermaid ER diagrams from schema catalog."""

import json
from typing import Any, Dict

from azure_openai_client import AzureOpenAIChatCompletionClient, SimpleLLMMessage


class DiagramGeneratorAgent:
    """Agent that generates Mermaid ER diagrams from schema catalogs."""

    def __init__(
        self,
        llm_config: Dict[str, Any],
    ):
        self.llm_client = AzureOpenAIChatCompletionClient(
            deployment=llm_config.get("deployment", "enmapper-gpt-5.1-codex"),
            api_key=llm_config.get("api_key"),
            base_url=llm_config.get("base_url"),
            api_version=llm_config.get("api_version", "2024-12-01-preview"),
            reasoning_effort=llm_config.get("reasoning_effort", "low"),
        )

    def generate_mermaid(self, catalog: Dict[str, Any]) -> str:
        """Generate Mermaid ER diagram code from schema catalog."""
        
        # Build a summary of the schema for the LLM
        tables = catalog.get("tables", [])
        relationships = catalog.get("relationships", [])
        schema_name = catalog.get("schema", "unknown")
        
        table_summaries = []
        for t in tables:
            cols = []
            for c in t.get("columns", []):
                col_type = c.get("type", "unknown")
                nullable = "nullable" if c.get("nullable", True) else "not null"
                cols.append(f"    {c.get('name')} {col_type} {nullable}")
            
            pk = t.get("primary_key", [])
            fks = t.get("foreign_keys", [])
            
            table_summaries.append({
                "name": t.get("table_name"),
                "columns": cols,
                "primary_key": pk,
                "foreign_keys": fks,
                "row_count": t.get("row_count", 0),
            })

        system_prompt = """You are an expert at creating Mermaid ER diagrams. Generate a clean, readable Mermaid erDiagram from the provided schema information.

RULES:
1. Output ONLY the Mermaid code, no explanations
2. Use erDiagram syntax
3. Include all tables with their key columns (PK, FK, and important columns)
4. Show relationships with proper cardinality (||--o{, }o--||, etc.)
5. Use clear, readable formatting
6. For large schemas, focus on the most important columns (PK, FK, and a few key fields)

Example format:
```mermaid
erDiagram
    CUSTOMERS {
        int customer_id PK
        string name
        string email
    }
    ORDERS {
        int order_id PK
        int customer_id FK
        date order_date
    }
    CUSTOMERS ||--o{ ORDERS : places
```

Output ONLY the mermaid code block."""

        user_content = f"""Generate a Mermaid ER diagram for this database schema:

SCHEMA: {schema_name}

TABLES:
{json.dumps(table_summaries, indent=2)}

RELATIONSHIPS:
{json.dumps(relationships, indent=2)}

Generate clean Mermaid erDiagram code."""

        messages = [
            SimpleLLMMessage(role="system", content=system_prompt),
            SimpleLLMMessage(role="user", content=user_content),
        ]

        result = self.llm_client.create(messages, max_tokens=4000)
        response_text = result.content if isinstance(result.content, str) else str(result.content or "")

        # Extract mermaid code
        mermaid_code = self._extract_mermaid(response_text)
        return mermaid_code

    def _extract_mermaid(self, response: str) -> str:
        """Extract Mermaid code from LLM response."""
        if not response:
            return ""

        response = response.strip()

        # Try to find mermaid code block
        if "```mermaid" in response:
            start = response.find("```mermaid") + len("```mermaid")
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

        # Check if response starts with erDiagram
        if response.startswith("erDiagram"):
            return response

        return response

