#!/usr/bin/env python
"""
Phase 3: Migration Execution Runner (Optimized)

Usage:
    python run_migration.py --run-folder <run-folder-name>

This script:
1. Loads the schema catalog from Phase 1
2. Executes the migration using 4 consolidated mega-tasks
"""

import argparse
import datetime
import json
import os
import sys

from config import get_azure_openai_config, load_credentials


def find_latest_catalog(run_folder: str) -> tuple:
    """Find the latest schema catalog in the run folder."""
    schema_agent_dir = os.path.join(run_folder, "schema_agent")
    if not os.path.isdir(schema_agent_dir):
        return None, None

    catalog_files = [
        f for f in os.listdir(schema_agent_dir)
        if f.startswith("schema_catalog_") and f.endswith(".json")
    ]
    if not catalog_files:
        return None, None

    catalog_files.sort(reverse=True)
    catalog_path = os.path.join(schema_agent_dir, catalog_files[0])

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    return catalog, catalog_path


def main():
    parser = argparse.ArgumentParser(description="Run Phase 3: Migration Execution")
    parser.add_argument(
        "--run-folder",
        required=True,
        help="Name of the run folder (e.g., 'ecommerce-run-1_20251129_123456')"
    )
    parser.add_argument(
        "--worker-deployment",
        default="enmapper-gpt-5.1-codex",
        help="Azure deployment for Worker agent"
    )
    parser.add_argument(
        "--worker-effort",
        default="medium",
        choices=["low", "medium", "high"],
        help="Reasoning effort for Worker agent"
    )
    parser.add_argument(
        "--source-host",
        default="sqltosnowflake.postgres.database.azure.com",
        help="Source PostgreSQL host"
    )
    parser.add_argument(
        "--source-db",
        default="postgres",
        help="Source database name"
    )
    parser.add_argument(
        "--source-schema",
        default="ecommerce",
        help="Source schema name"
    )
    parser.add_argument(
        "--source-user",
        default="postgresadmin",
        help="Source database user"
    )
    parser.add_argument(
        "--source-password",
        default="Postgres@123456",
        help="Source database password"
    )
    parser.add_argument(
        "--target-schema",
        default=None,
        help="Target Snowflake schema (defaults to source schema uppercased)"
    )

    args = parser.parse_args()

    # Load credentials
    creds = load_credentials()

    # Determine run folder path
    output_base = os.path.join(os.path.dirname(__file__), "output")
    run_folder = os.path.join(output_base, args.run_folder)

    if not os.path.isdir(run_folder):
        print(f"ERROR: Run folder not found: {run_folder}")
        sys.exit(1)

    print("=" * 80)
    print("PHASE 3: Migration Execution (Optimized - 4 Mega-Tasks)")
    print("=" * 80)
    print(f"Run folder: {run_folder}")

    # Load catalog from Phase 1
    catalog, catalog_path = find_latest_catalog(run_folder)
    if not catalog:
        print("ERROR: No schema catalog found in run folder")
        print("Please run Phase 1 (Schema Analysis) first")
        sys.exit(1)

    print(f"Loaded catalog: {catalog_path}")
    print(f"  Tables: {len(catalog.get('tables', []))}")
    for t in catalog.get("tables", []):
        print(f"    - {t.get('table_name')}: {t.get('row_count', 0)} rows")

    # Create execution output folder
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    execution_dir = os.path.join(run_folder, f"execution_{timestamp}")
    os.makedirs(execution_dir, exist_ok=True)
    print(f"Execution output: {execution_dir}")

    # Build configurations
    azure_config = get_azure_openai_config()

    worker_config = {
        "deployment": args.worker_deployment,
        "api_key": azure_config["api_key"],
        "base_url": azure_config["base_url"],
        "api_version": azure_config["api_version"],
        "reasoning_effort": args.worker_effort,
    }

    # Source database config
    source_db = {
        "host": args.source_host,
        "port": 5432,
        "database": args.source_db,
        "schema": args.source_schema,
        "user": args.source_user,
        "password": args.source_password,
    }

    # Target Snowflake schema - default to source schema name uppercased
    target_schema = args.target_schema or args.source_schema.upper()

    # Target database config (Snowflake)
    target_db = {
        "account": creds.get("SNOWFLAKE_ACCOUNT", ""),
        "user": creds.get("SNOWFLAKE_USER", ""),
        "password": creds.get("SNOWFLAKE_PASSWORD", ""),
        "warehouse": creds.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": creds.get("SNOWFLAKE_DATABASE", "MIGRATION_DB"),
        "schema": target_schema,
    }

    print("\nSource Database (PostgreSQL):")
    print(f"  Host: {source_db['host']}")
    print(f"  Database: {source_db['database']}")
    print(f"  Schema: {source_db['schema']}")

    print("\nTarget Database (Snowflake):")
    print(f"  Account: {target_db['account']}")
    print(f"  Database: {target_db['database']}")
    print(f"  Schema: {target_db['schema']}")

    print("\nWorker Agent:")
    print(f"  Model: {args.worker_deployment}")
    print(f"  Effort: {args.worker_effort}")
    print(f"  Max attempts per task: 7")

    print("-" * 80)
    print("Starting migration execution with 4 mega-tasks...")
    print("-" * 80)

    # Import and run executor
    from agents.executor import MigrationExecutor

    executor = MigrationExecutor(
        worker_config=worker_config,
        output_dir=execution_dir,
        source_db=source_db,
        target_db=target_db,
    )

    result = executor.execute_migration(
        migration_plan="",  # Not used in optimized version
        catalog=catalog,
    )

    # Print summary
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Success: {result.get('success')}")
    print(f"Duration: {result.get('duration_seconds', 0):.1f} seconds")
    print(f"Total tasks: {result.get('total_tasks', 0)}")
    print(f"Completed: {result.get('completed_tasks', 0)}")
    print(f"Failed: {result.get('failed_tasks', 0)}")

    if result.get("failed_task_ids"):
        print(f"\nFailed tasks: {result['failed_task_ids']}")

    print(f"\nReport saved to: {execution_dir}/execution_report.json")

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())

