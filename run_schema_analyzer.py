"""
Main script to run the Schema Analyzer Agent
"""

import argparse
import datetime
import sys
from config import (
    get_agent_output_dir,
    get_azure_openai_config,
    sanitize_run_id,
)
from agents.schema_analyzer import SchemaAnalyzerAgent


def parse_args():
    parser = argparse.ArgumentParser(description="Run the schema analyzer agent.")
    parser.add_argument("--run-id", help="Unique identifier for this analysis run.")
    parser.add_argument("--schema", default="ecommerce", help="PostgreSQL schema to profile.")
    return parser.parse_args()


def main():
    """Run the schema analyzer agent."""
    args = parse_args()
    run_id = args.run_id or f"run_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    sanitized_run_id = sanitize_run_id(run_id)
    unique_run_folder = f"{sanitized_run_id}_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    # Get configuration
    llm_config = get_azure_openai_config(
        reasoning_effort="medium",
        deployment="enmapper-gpt-5.1-codex",
    )
    
    # Database configuration - PostgreSQL Azure database
    database_config = {
        "type": "postgresql",
        "host": "sqltosnowflake.postgres.database.azure.com",
        "port": 5432,
        "database": "postgres",
        "schema": args.schema,
        "user": "postgresadmin",
        "password": "Postgres@123456",
    }

    agent_output_dir = get_agent_output_dir(unique_run_folder, "schema_agent")
    
    # Create agent configuration
    agent_config = {
        "llm_config": llm_config,
        "output_dir": agent_output_dir,
        "run_id": sanitized_run_id,
        "run_folder": unique_run_folder,
        "max_iterations": 6,
        "stable_rounds_required": 2,
        "kernel_timeout": 45,
    }
    
    print("="*80)
    print("Schema Analyzer Agent - Starting Analysis")
    print("="*80)
    print(f"\nRun ID: {sanitized_run_id}")
    print(f"Run Folder: {unique_run_folder}")
    print(f"Database: {database_config['type']}://{database_config['host']}/{database_config['database']}")
    print(f"Schema: {database_config['schema']}")
    print(f"Output Directory: {agent_config['output_dir']}")
    print(f"Reasoning Effort: medium")
    print("\n" + "-"*80)
    print("Initializing agent with code execution capability...")
    print("-"*80 + "\n")
    
    # Create and run the schema analyzer agent
    try:
        agent = SchemaAnalyzerAgent(agent_config)
        results = agent.analyze_schema(database_config)
        
        print("\n" + "="*80)
        print("Schema Analysis Complete")
        print("="*80)
        print(f"\nAnalysis File: {results.get('analysis_file', 'Not generated')}")
        print(f"Schema JSON: {results.get('schema_file', 'Not generated')}")
        print(f"Iterations: {results.get('iterations')}")
        print(f"Satisfied: {results.get('satisfied')}")
        print(f"Success: {results.get('success', False)}")
        
        if results.get('success'):
            print("\n[SUCCESS] Schema analysis completed successfully!")
            print(f"Check the output directory: {results.get('output_dir')}")
        else:
            print("\n[WARNING] Analysis may not have completed fully.")
            print("Check the agent output above for details.")
            
    except Exception as e:
        print(f"\n[ERROR] Failed to run schema analyzer: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

