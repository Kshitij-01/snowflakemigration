"""
Configuration file for the migration pipeline
Loads credentials and sets up Azure OpenAI endpoints
"""

import os
import re
from typing import Dict, Any

def load_credentials() -> Dict[str, str]:
    """Load credentials from environment variables or credentials.txt file."""
    creds = {}
    
    # First, check environment variables (for production/Docker)
    env_keys = [
        'AZURE_OPENAI_API_KEY', 'AZURE_OPENAI_ENDPOINT', 'AZURE_OPENAI_API_VERSION',
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
    ]
    
    for key in env_keys:
        value = os.environ.get(key)
        if value:
            creds[key] = value
    
    # If we have the essential keys from env, return
    if creds.get('AZURE_OPENAI_API_KEY') and creds.get('SNOWFLAKE_ACCOUNT'):
        return creds
    
    # Fall back to credentials.txt file (for local development)
    cred_file = os.path.join(os.path.dirname(__file__), 'credentials.txt')
    
    if os.path.exists(cred_file):
        with open(cred_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Don't override env vars
                    if key.strip() not in creds:
                        creds[key.strip()] = value.strip()
    
    if not creds.get('AZURE_OPENAI_API_KEY'):
        raise ValueError("AZURE_OPENAI_API_KEY not found in environment or credentials.txt")
    
    return creds

def get_azure_openai_config(
    reasoning_effort: str = "medium",
    deployment: str | None = None,
) -> Dict[str, Any]:
    """
    Get Azure OpenAI configuration.
    
    Args:
        reasoning_effort: Reasoning effort level for GPT-5.1-codex ('low', 'medium', 'high').
        deployment: Specific deployment name (defaults to GPT-5.1-codex).
    """
    creds = load_credentials()
    endpoint = creds.get("AZURE_OPENAI_ENDPOINT")
    api_key = creds.get("AZURE_OPENAI_API_KEY")
    api_version = creds.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    deployment = deployment or "enmapper-gpt-5.1-codex"
    base_url = endpoint.rstrip("/")
    return {
        "deployment": deployment,
        "api_key": api_key,
        "base_url": base_url,
        "api_version": api_version,
        "reasoning_effort": reasoning_effort,
    }

def get_snowflake_config() -> Dict[str, str]:
    """Get Snowflake configuration."""
    creds = load_credentials()
    return {
        "account": creds.get('SNOWFLAKE_ACCOUNT'),
        "user": creds.get('SNOWFLAKE_USER'),
        "password": creds.get('SNOWFLAKE_PASSWORD'),
        "warehouse": creds.get('SNOWFLAKE_WAREHOUSE'),
        "database": creds.get('SNOWFLAKE_DATABASE'),
        "schema": creds.get('SNOWFLAKE_SCHEMA'),
    }

def get_output_dirs() -> Dict[str, str]:
    """Get output directory paths."""
    base_dir = os.path.dirname(__file__)
    return {
        "base": os.path.join(base_dir, "output"),
        "schema_analysis": os.path.join(base_dir, "output", "schema_analysis"),
        "migration_plan": os.path.join(base_dir, "output", "migration_plan"),
        "migration_execution": os.path.join(base_dir, "output", "migration_execution"),
    }


def sanitize_run_id(run_id: str) -> str:
    """Normalize a run ID so it's safe to use in file paths."""
    if not run_id:
        return "run"
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "-", run_id)
    normalized = normalized.strip("-")[:64]
    return normalized or "run"


def get_agent_output_dir(run_id: str, agent_name: str) -> str:
    """Return a per-run, per-agent output directory and make sure it exists."""
    sanitized_id = sanitize_run_id(run_id)
    base = os.path.join(os.path.dirname(__file__), "output", sanitized_id)
    agent_dir = os.path.join(base, agent_name)
    os.makedirs(agent_dir, exist_ok=True)
    return agent_dir

