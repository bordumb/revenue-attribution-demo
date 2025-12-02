
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
