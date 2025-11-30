#!/usr/bin/env python
"""
End-to-End Migration Pipeline

Runs all 3 phases automatically:
  Phase 1: Schema Analysis
  Phase 2: Migration Planning (Debate)
  Phase 3: Migration Execution

Usage:
    python run_e2e_migration.py --run-id my-migration --source-schema ecommerce
"""

import argparse
import datetime
import json
import os
import sys

from config import get_azure_openai_config, load_credentials


def run_phase1_schema_analysis(run_folder: str, source_schema: str, db_config: dict) -> dict:
    """Run Phase 1: Schema Analysis."""
    print("\n" + "=" * 80)
    print("PHASE 1: Schema Analysis")
    print("=" * 80)

    from agents.schema_analyzer import SchemaAnalyzerAgent

    azure_config = get_azure_openai_config(reasoning_effort="medium")

    agent = SchemaAnalyzerAgent(
        db_type="postgresql",
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"],
        schema=source_schema,
        user=db_config["user"],
        password=db_config["password"],
        output_dir=os.path.join(run_folder, "schema_agent"),
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
        return {"success": False, "error": "Phase 1 failed", "details": result}

    # Find the generated catalog
    schema_agent_dir = os.path.join(run_folder, "schema_agent")
    catalog_files = [
        f for f in os.listdir(schema_agent_dir)
        if f.startswith("schema_catalog_") and f.endswith(".json")
    ]
    if not catalog_files:
        return {"success": False, "error": "No catalog file generated"}

    catalog_files.sort(reverse=True)
    catalog_path = os.path.join(schema_agent_dir, catalog_files[0])

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    print(f"\n[Phase 1] Complete - Found {len(catalog.get('tables', []))} tables")
    print(f"[Phase 1] Catalog: {catalog_path}")

    return {
        "success": True,
        "catalog": catalog,
        "catalog_path": catalog_path,
        "iterations": result.get("iterations"),
    }


def run_phase2_migration_planning(run_folder: str, catalog: dict, source_schema: str) -> dict:
    """Run Phase 2: Migration Planning (Debate)."""
    print("\n" + "=" * 80)
    print("PHASE 2: Migration Planning (Debate)")
    print("=" * 80)

    from agents.planner import PlannerAgent, DebateRunner

    azure_config = get_azure_openai_config()

    # Create planner agents
    planner_alpha = PlannerAgent(
        name="Planner Alpha",
        deployment=azure_config["deployment"],  # gpt-5.1-codex
        api_key=azure_config["api_key"],
        base_url=azure_config["base_url"],
        api_version=azure_config["api_version"],
        reasoning_effort="medium",
        max_tokens=16000,
    )

    planner_beta = PlannerAgent(
        name="Planner Beta",
        deployment="enmapper-gpt-5.1",  # gpt-5.1 for critique
        api_key=azure_config["api_key"],
        base_url=azure_config["base_url"],
        api_version=azure_config["api_version"],
        reasoning_effort=None,  # gpt-5.1 doesn't use reasoning_effort
        max_tokens=16000,
    )

    # Run debate
    debate_runner = DebateRunner(
        planner_alpha=planner_alpha,
        planner_beta=planner_beta,
        output_dir=os.path.join(run_folder, "migration_plan"),
        debate_rounds=2,
    )

    result = debate_runner.run_debate(catalog, source_schema)

    if not result:
        return {"success": False, "error": "Phase 2 debate failed"}

    # Find the generated plan
    plan_dir = os.path.join(run_folder, "migration_plan")
    plan_files = [
        f for f in os.listdir(plan_dir)
        if f.startswith("migration_plan_") and f.endswith(".json")
    ]
    if not plan_files:
        return {"success": False, "error": "No migration plan generated"}

    plan_files.sort(reverse=True)
    plan_path = os.path.join(plan_dir, plan_files[0])

    with open(plan_path, "r", encoding="utf-8") as f:
        plan_data = json.load(f)

    # Load markdown plan text
    plan_md_path = plan_path.replace(".json", ".md")
    plan_text = ""
    if os.path.exists(plan_md_path):
        with open(plan_md_path, "r", encoding="utf-8") as f:
            plan_text = f.read()

    print(f"\n[Phase 2] Complete - Migration plan generated")
    print(f"[Phase 2] Plan: {plan_path}")

    return {
        "success": True,
        "plan_data": plan_data,
        "plan_text": plan_text,
        "plan_path": plan_path,
    }


def run_phase3_migration_execution(
    run_folder: str,
    catalog: dict,
    source_db: dict,
    target_db: dict,
) -> dict:
    """Run Phase 3: Migration Execution."""
    print("\n" + "=" * 80)
    print("PHASE 3: Migration Execution")
    print("=" * 80)

    from agents.executor import MigrationExecutor

    azure_config = get_azure_openai_config()

    worker_config = {
        "deployment": azure_config["deployment"],
        "api_key": azure_config["api_key"],
        "base_url": azure_config["base_url"],
        "api_version": azure_config["api_version"],
        "reasoning_effort": "medium",
    }

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    execution_dir = os.path.join(run_folder, f"execution_{timestamp}")
    os.makedirs(execution_dir, exist_ok=True)

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

    print(f"\n[Phase 3] Complete - {result.get('completed_tasks')}/{result.get('total_tasks')} tasks")
    print(f"[Phase 3] Duration: {result.get('duration_seconds', 0):.1f} seconds")

    return {
        "success": result.get("success", False),
        "execution_dir": execution_dir,
        "duration": result.get("duration_seconds"),
        "completed_tasks": result.get("completed_tasks"),
        "failed_tasks": result.get("failed_tasks"),
        "failed_task_ids": result.get("failed_task_ids", []),
    }


def main():
    parser = argparse.ArgumentParser(description="Run E2E Migration Pipeline")
    parser.add_argument(
        "--run-id",
        required=True,
        help="Unique identifier for this migration run"
    )
    parser.add_argument(
        "--source-schema",
        required=True,
        help="Source PostgreSQL schema to migrate"
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
    parser.add_argument(
        "--skip-phase1",
        action="store_true",
        help="Skip Phase 1 if catalog already exists"
    )
    parser.add_argument(
        "--skip-phase2",
        action="store_true",
        help="Skip Phase 2 if plan already exists"
    )

    args = parser.parse_args()

    # Load credentials
    creds = load_credentials()

    # Create run folder with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_folder_name = f"{args.run_id}_{timestamp}"
    output_base = os.path.join(os.path.dirname(__file__), "output")
    run_folder = os.path.join(output_base, run_folder_name)
    os.makedirs(run_folder, exist_ok=True)

    print("=" * 80)
    print("E2E MIGRATION PIPELINE")
    print("=" * 80)
    print(f"Run ID: {args.run_id}")
    print(f"Run Folder: {run_folder_name}")
    print(f"Source Schema: {args.source_schema}")
    print(f"Target Schema: {args.target_schema or args.source_schema.upper()}")

    # Source database config
    source_db = {
        "host": args.source_host,
        "port": 5432,
        "database": args.source_db,
        "schema": args.source_schema,
        "user": args.source_user,
        "password": args.source_password,
    }

    # Target Snowflake config
    target_schema = args.target_schema or args.source_schema.upper()
    target_db = {
        "account": creds.get("SNOWFLAKE_ACCOUNT", ""),
        "user": creds.get("SNOWFLAKE_USER", ""),
        "password": creds.get("SNOWFLAKE_PASSWORD", ""),
        "warehouse": creds.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": creds.get("SNOWFLAKE_DATABASE", "MIGRATION_DB"),
        "schema": target_schema,
    }

    start_time = datetime.datetime.now()
    results = {"run_folder": run_folder, "phases": {}}

    # =========================================================================
    # PHASE 1: Schema Analysis
    # =========================================================================
    if args.skip_phase1:
        print("\n[Skipping Phase 1 - using existing catalog]")
        # Would need to load existing catalog here
        results["phases"]["phase1"] = {"skipped": True}
    else:
        phase1_result = run_phase1_schema_analysis(
            run_folder=run_folder,
            source_schema=args.source_schema,
            db_config=source_db,
        )
        results["phases"]["phase1"] = phase1_result

        if not phase1_result.get("success"):
            print(f"\n[ERROR] Phase 1 failed: {phase1_result.get('error')}")
            return 1

        catalog = phase1_result["catalog"]

    # =========================================================================
    # PHASE 2: Migration Planning
    # =========================================================================
    if args.skip_phase2:
        print("\n[Skipping Phase 2 - using existing plan]")
        results["phases"]["phase2"] = {"skipped": True}
    else:
        phase2_result = run_phase2_migration_planning(
            run_folder=run_folder,
            catalog=catalog,
            source_schema=args.source_schema,
        )
        results["phases"]["phase2"] = phase2_result

        if not phase2_result.get("success"):
            print(f"\n[ERROR] Phase 2 failed: {phase2_result.get('error')}")
            return 1

    # =========================================================================
    # PHASE 3: Migration Execution
    # =========================================================================
    phase3_result = run_phase3_migration_execution(
        run_folder=run_folder,
        catalog=catalog,
        source_db=source_db,
        target_db=target_db,
    )
    results["phases"]["phase3"] = phase3_result

    end_time = datetime.datetime.now()
    total_duration = (end_time - start_time).total_seconds()

    # =========================================================================
    # FINAL SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("E2E MIGRATION COMPLETE")
    print("=" * 80)
    print(f"Run Folder: {run_folder}")
    print(f"Total Duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
    print()
    print("Phase Results:")
    print(f"  Phase 1 (Schema Analysis): {'Skipped' if args.skip_phase1 else 'Success' if results['phases'].get('phase1', {}).get('success') else 'Failed'}")
    print(f"  Phase 2 (Migration Plan):  {'Skipped' if args.skip_phase2 else 'Success' if results['phases'].get('phase2', {}).get('success') else 'Failed'}")
    print(f"  Phase 3 (Execution):       {'Success' if phase3_result.get('success') else 'Failed'}")

    if phase3_result.get("success"):
        print()
        print(f"Migration completed successfully!")
        print(f"  Tables migrated to Snowflake schema: {target_schema}")
        print(f"  Tasks completed: {phase3_result.get('completed_tasks')}/{phase3_result.get('completed_tasks', 0) + phase3_result.get('failed_tasks', 0)}")
    else:
        print()
        print(f"Migration failed!")
        if phase3_result.get("failed_task_ids"):
            print(f"  Failed tasks: {phase3_result['failed_task_ids']}")

    # Save summary
    results["total_duration_seconds"] = total_duration
    results["success"] = phase3_result.get("success", False)
    results["target_schema"] = target_schema

    summary_path = os.path.join(run_folder, "e2e_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nSummary saved to: {summary_path}")

    return 0 if phase3_result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())

