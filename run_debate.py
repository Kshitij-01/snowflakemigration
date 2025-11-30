"""Run the planner debate (Phase 2) against a completed schema catalog."""

from __future__ import annotations

import argparse
import datetime
import glob
import json
import os
import sys
from typing import Any, Dict

from config import get_agent_output_dir, get_azure_openai_config
from agents.planner import DebateRunner, PlannerAgent


def parse_args():
    parser = argparse.ArgumentParser(description="Run planner debate for migration planning.")
    parser.add_argument(
        "--run-folder",
        required=True,
        help="Name of the output run folder containing schema_agent artifacts.",
    )
    parser.add_argument(
        "--alpha-deployment",
        default="enmapper-gpt-5.1-codex",
        help="Azure deployment to use for Planner Alpha.",
    )
    parser.add_argument(
        "--beta-deployment",
        default="enmapper-gpt-5.1",
        help="Azure deployment to use for Planner Beta.",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=2,
        help="Number of debate cycles to execute.",
    )
    return parser.parse_args()


def find_latest_catalog(run_folder: str) -> str:
    schema_dir = os.path.join(os.path.dirname(__file__), "output", run_folder, "schema_agent")
    pattern = os.path.join(schema_dir, "schema_catalog_*.json")
    candidates = sorted(glob.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No schema catalog found for run folder {run_folder}")
    return candidates[-1]


def load_catalog(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_plan_markdown(run_folder: str, plan_data: Dict[str, Any]) -> str:
    lines = [
        f"# Migration Plan for {plan_data.get('schema', 'unknown')}",
        "",
        f"- **Run folder**: {run_folder}",
        f"- **Database Type**: {plan_data.get('database_type', 'unknown')}",
        f"- **Generated**: {plan_data.get('timestamp', 'unknown')}",
        f"- **Debate Rounds**: {plan_data.get('run_rounds', 0)}",
        "",
        "---",
        "",
        "## Final Migration Plan (Planner Alpha)",
        "",
        plan_data.get("final_plan", "(No plan generated)"),
        "",
        "---",
        "",
        "## Final Critique (Planner Beta)",
        "",
        plan_data.get("final_critique", "(No critique generated)"),
        "",
        "---",
        "",
        "## Full Debate Conversation",
        "",
    ]
    for i, entry in enumerate(plan_data.get("conversation", [])):
        speaker = entry.get("speaker", "Unknown")
        message = entry.get("message", "")
        lines.append(f"### Turn {i+1}: {speaker}")
        lines.append("")
        lines.append(message)
        lines.append("")
        lines.append("---")
        lines.append("")
    return "\n".join(lines)


def main():
    args = parse_args()
    run_root = os.path.join(os.path.dirname(__file__), "output", args.run_folder)
    if not os.path.isdir(run_root):
        print("Specified run folder does not exist.", args.run_folder, file=sys.stderr)
        sys.exit(1)

    catalog_path = find_latest_catalog(args.run_folder)
    catalog = load_catalog(catalog_path)
    print("Loaded catalog keys:", ", ".join(catalog.keys()))
    tables = catalog.get("tables", [])
    print(f"Tables in catalog: {len(tables)}")

    alpha_config = get_azure_openai_config(
        reasoning_effort="high",
        deployment=args.alpha_deployment,
    )
    beta_config = get_azure_openai_config(
        reasoning_effort="medium",
        deployment=args.beta_deployment,
    )

    alpha_system = (
        "You are Planner Alpha (GPT-5.1-codex). Produce structured migration steps "
        "with code, transformations, and validation checkpoints."
    )
    beta_system = (
        "You are Planner Beta (GPT-5.1). Critique the migration plan "
        "from operational, data-quality, and reliability perspectives."
    )

    planner_alpha = PlannerAgent(
        name="Planner Alpha",
        client_config=alpha_config,
        system_prompt=alpha_system,
        max_tokens=16000,
    )
    planner_beta = PlannerAgent(
        name="Planner Beta",
        client_config=beta_config,
        system_prompt=beta_system,
        max_tokens=16000,
    )

    plan_dir = get_agent_output_dir(args.run_folder, "migration_plan")

    runner = DebateRunner(
        planner_alpha=planner_alpha,
        planner_beta=planner_beta,
        output_dir=plan_dir,
        max_rounds=args.max_rounds,
    )

    plan_data = runner.run_debate(catalog)
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    plan_json_path = os.path.join(plan_dir, f"migration_plan_{timestamp}.json")
    plan_md_path = os.path.join(plan_dir, f"migration_plan_{timestamp}.md")

    with open(plan_json_path, "w", encoding="utf-8") as handle:
        json.dump(plan_data, handle, indent=2)
    with open(plan_md_path, "w", encoding="utf-8") as handle:
        handle.write(build_plan_markdown(args.run_folder, plan_data))

    print("=" * 80)
    print("Planner debate complete")
    print(f"Schema catalog: {catalog_path}")
    print(f"Migration plan JSON: {plan_json_path}")
    print(f"Migration plan outline: {plan_md_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()

