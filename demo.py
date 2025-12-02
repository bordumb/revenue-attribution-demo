import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Path hacking to find datadr
sys.path.append(str(Path(__file__).parent.parent.parent))

from datadr import DataDr, AnomalyAlert
from datadr.core.config import DataDrConfig

# Import setup script
import setup_cloud_env

load_dotenv()

async def main():
    base_dir = Path(__file__).parent.resolve()
    
    # 1. Initialize Environment (Docker DB + Data)
    print("\n=== INITIALIZING CLOUD ENVIRONMENT ===")
    try:
        await setup_cloud_env.wait_for_db()
        await setup_cloud_env.setup_database()
        setup_cloud_env.setup_local_files()
    except Exception as e:
        print(f"Environment setup failed: {e}")
        print("Did you run 'docker-compose up -d'?")
        return

    # 2. Load Config
    config_path = base_dir / "config" / "datadr_config.yaml"
    config = DataDrConfig.from_yaml(str(config_path))
    
    # Configure absolute paths
    config.job_repo.root_path = str(base_dir / "jobs")
    config.context.git.repo_path = str(base_dir)
    
    # Configure MCP Server to use current Python venv
    mcp_config = config.context.mcp_servers[0]
    mcp_config.command = sys.executable
    mcp_config.args = [str(base_dir / "postgres_mcp.py")]
    
    # Ensure env vars are passed (if not set in yaml)
    if not mcp_config.env:
        mcp_config.env = {}
    mcp_config.env["PYTHONUNBUFFERED"] = "1"
    
    print("\n=== STARTING CONTEXT-AWARE INVESTIGATION (CLOUD) ===")
    print(f"Connecting to MCP Server: {mcp_config.command} {mcp_config.args[0]}")
    
    dr = DataDr(config)
    
    # 3. The Alert
    # 4 transactions total. 2 have NULL user_id.
    # The 'transactions' table in Postgres allows NULLs.
    # The Code uses 'inner join' which drops them.
    # Expected: 4. Actual: 2.
    alert = AnomalyAlert(
        metric_name="total_revenue_count",
        job_name="revenue_attribution",
        job_path=str(base_dir / "jobs" / "revenue_attribution"),
        date="2025-01-01",
        expected_value=4.0,
        actual_value=2.0,
        deviation_pct=-50.0
    )
    
    # 4. Investigate
    investigation = await dr.investigate_async(alert)
    
    # 5. Report
    print("\n=== DIAGNOSIS ===")
    print(investigation.diagnosis.summary)
    print(f"Confidence: {investigation.diagnosis.confidence:.2%}")
    
    print("\n=== CONTEXT USED ===")
    # Print context highlights
    if investigation.alert.metadata:
        print("Metadata:", investigation.alert.metadata)
        
    print("\n=== EVIDENCE ===")
    for item in investigation.diagnosis.evidence:
        print(f"- {item}")

    # Generate report file
    report_path = dr.generate_report(investigation)
    print(f"\nReport generated at: {report_path}")

if __name__ == "__main__":
    asyncio.run(main())