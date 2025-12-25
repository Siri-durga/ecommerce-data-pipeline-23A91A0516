import psycopg2
import pandas as pd
import json
import time
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("data/processed/analytics")

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="ecommerce",
        user="postgres",
        password="postgres"
    )

def execute_query(conn, query_name, sql):
    start = time.time()
    df = pd.read_sql(sql, conn)
    execution_time = (time.time() - start) * 1000
    return df, execution_time

def export_to_csv(df, filename):
    df.to_csv(OUTPUT_DIR / filename, index=False)

def generate_summary(results, total_time):
    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": len(results),
        "query_results": {},
        "total_execution_time_seconds": round(total_time, 2)
    }

    for name, info in results.items():
        summary["query_results"][name] = {
            "rows": info["rows"],
            "columns": info["columns"],
            "execution_time_ms": round(info["time"], 2)
        }

    with open(OUTPUT_DIR / "analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()

    with open("sql/queries/analytical_queries.sql") as f:
        queries = f.read().split(";")

    results = {}
    total_start = time.time()

    for i, query in enumerate(queries):
        if query.strip():
            name = f"query{i+1}"
            df, exec_time = execute_query(conn, name, query)
            export_to_csv(df, f"{name}.csv")
            results[name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "time": exec_time
            }

    total_time = time.time() - total_start
    generate_summary(results, total_time)

    conn.close()
    print("Analytics generation completed")

if __name__ == "__main__":
    main()
