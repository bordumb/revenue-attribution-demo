"""
Setup script for the Cloud/MCP Demo.
1. Waits for Docker Postgres to be healthy.
2. Creates tables and seeds data.
3. Generates the PySpark job files locally.
"""

import asyncio
import os
import shutil
import subprocess
import time
import asyncpg
from pathlib import Path
from datetime import date

BASE_DIR = Path(__file__).parent.resolve()
JOB_DIR = BASE_DIR / "jobs" / "revenue_attribution"

# Database Config (Matches docker-compose.yml)
DB_CONFIG = {
    "user": "admin",
    "password": "password",
    "database": "analytics",
    "host": "localhost",
    "port": 5433
}

async def wait_for_db():
    """Polls until the database is ready."""
    print("Waiting for PostgreSQL to be ready...")
    for i in range(10):
        try:
            conn = await asyncpg.connect(**DB_CONFIG)
            await conn.close()
            print("✓ Database is ready.")
            return
        except Exception:
            print(f"  Attempt {i+1}/10: Database not ready yet...")
            time.sleep(2)
    raise RuntimeError("Database failed to become ready.")

async def setup_database():
    """Creates schema and inserts seed data."""
    print("Seeding database...")
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # 1. Transactions Table
        await conn.execute("DROP TABLE IF EXISTS transactions")
        await conn.execute("""
            CREATE TABLE transactions (
                txn_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50), 
                amount DECIMAL(10, 2),
                txn_date DATE
            )
        """)
        
        # Insert Data: Use datetime.date objects, NOT strings
        await conn.executemany("""
            INSERT INTO transactions (txn_id, user_id, amount, txn_date)
            VALUES ($1, $2, $3, $4)
        """, [
            ("t1", "u1", 100.00, date(2025, 1, 1)),
            ("t2", "u2", 200.00, date(2025, 1, 1)),
            ("t3", None, 50.00,  date(2025, 1, 1)), # Guest checkout
            ("t4", None, 75.00,  date(2025, 1, 1)), # Guest checkout
        ])
        print("  - Created 'transactions' table (4 rows)")

        # 2. Users Table
        await conn.execute("DROP TABLE IF EXISTS users")
        await conn.execute("""
            CREATE TABLE users (
                user_id VARCHAR(50) PRIMARY KEY,
                tier VARCHAR(20),
                country VARCHAR(2)
            )
        """)
        
        await conn.executemany("""
            INSERT INTO users (user_id, tier, country)
            VALUES ($1, $2, $3)
        """, [
            ("u1", "gold", "US"),
            ("u2", "silver", "UK")
        ])
        print("  - Created 'users' table (2 rows)")
        
    finally:
        await conn.close()

def setup_local_files():
    """Creates the PySpark job files."""
    print("Generating local job files...")
    if JOB_DIR.exists(): shutil.rmtree(JOB_DIR)
    JOB_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate local data mirrors for Spark execution
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("DataGen").getOrCreate()
    
    data_dir = BASE_DIR / "data"
    if data_dir.exists(): shutil.rmtree(data_dir)
    
    # Mirror Postgres data to Parquet (using strings here is fine for Spark)
    txns = [
        ("t1", "u1", 100.0, "2025-01-01"),
        ("t2", "u2", 200.0, "2025-01-01"),
        ("t3", None, 50.0,  "2025-01-01"),
        ("t4", None, 75.0,  "2025-01-01"),
    ]
    spark.createDataFrame(txns, ["txn_id", "user_id", "amount", "txn_date"])\
        .write.parquet(str(data_dir / "transactions"))

    users = [("u1", "gold", "US"), ("u2", "silver", "UK")]
    spark.createDataFrame(users, ["user_id", "tier", "country"])\
        .write.parquet(str(data_dir / "users"))
        
    spark.stop()
    print("  - Generated local Parquet mirrors for Spark execution")

    # The Job Script
    code = """
from pyspark.sql import SparkSession
import os
from pathlib import Path

def run(spark):
    cwd = Path(os.getcwd())
    # Robust path finding for demo
    data_dir = cwd / "data"
    if not data_dir.exists():
        if (cwd / "examples/end_to_end_mcp_cloud/data").exists():
            data_dir = cwd / "examples/end_to_end_mcp_cloud/data"
        elif (cwd / ".." / "data").exists():
            data_dir = cwd / ".." / "data"
            
    txns = spark.read.parquet(str(data_dir / "transactions"))
    users = spark.read.parquet(str(data_dir / "users"))
    
    # BUG: Inner join drops NULL user_ids (Guest checkouts)
    return txns.join(users, on="user_id", how="inner").count()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RevenueAttribution").getOrCreate()
    print(f"Result: {run(spark)}")
"""
    (JOB_DIR / "job.py").write_text(code)
    
    # Initialize Git
    if (BASE_DIR / ".git").exists(): shutil.rmtree(BASE_DIR / ".git")
    subprocess.run("git init", shell=True, cwd=BASE_DIR, stdout=subprocess.DEVNULL)
    subprocess.run("git add .", shell=True, cwd=BASE_DIR, stdout=subprocess.DEVNULL)
    subprocess.run("git commit -m 'Initial commit'", shell=True, cwd=BASE_DIR, stdout=subprocess.DEVNULL)
    print("  - Initialized local git repo")

async def main():
    await wait_for_db()
    await setup_database()
    setup_local_files()
    print("\n✅ Environment Setup Complete.")

if __name__ == "__main__":
    asyncio.run(main())