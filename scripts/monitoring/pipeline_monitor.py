import os
import psycopg2
import json
import time
from datetime import datetime, timedelta
import statistics
import logging

OUTPUT_FILE = "data/processed/monitoring_report.json"
LOG_FILE = "logs/scheduler_activity.log"

DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce",
    "user": "postgres",
    "password": "postgres"
}

def db_connect():
    start = time.time()
    conn = psycopg2.connect(**DB_CONFIG)
    response_time = (time.time() - start) * 1000
    return conn, response_time

def read_last_pipeline_run():
    if not os.path.exists(LOG_FILE):
        return None
    with open(LOG_FILE) as f:
        lines = f.readlines()
    for line in reversed(lines):
        if "Pipeline execution SUCCESS" in line:
            return datetime.fromisoformat(line.split(" - ")[0])
    return None

def fetch_one(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    cur.close()
    return result

def fetch_all(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    return result

def main():
    report = {
        "monitoring_timestamp": datetime.utcnow().isoformat(),
        "checks": {},
        "alerts": []
    }

    overall_score = 100

    # DATABASE CONNECTIVITY
    try:
        conn, response_time = db_connect()
        report["checks"]["database_connectivity"] = {
            "status": "ok",
            "response_time_ms": round(response_time, 2)
        }
    except Exception as e:
        report["checks"]["database_connectivity"] = {
            "status": "error",
            "message": str(e)
        }
        report["alerts"].append({
            "severity": "critical",
            "check": "database_connectivity",
            "message": "Database unreachable",
            "timestamp": datetime.utcnow().isoformat()
        })
        overall_score -= 30
        conn = None

    # DATA FRESHNESS
    if conn:
        warehouse_latest = fetch_one(conn,
            "SELECT MAX(created_at) FROM warehouse.fact_sales")[0]

        hours_lag = (datetime.utcnow() - warehouse_latest).total_seconds() / 3600

        status = "ok" if hours_lag < 24 else "critical"

        report["checks"]["data_freshness"] = {
            "status": status,
            "warehouse_latest_record": str(warehouse_latest),
            "max_lag_hours": round(hours_lag, 2)
        }

        if status == "critical":
            report["alerts"].append({
                "severity": "critical",
                "check": "data_freshness",
                "message": "Warehouse data older than 24 hours",
                "timestamp": datetime.utcnow().isoformat()
            })
            overall_score -= 20

    # DATA VOLUME ANOMALY
    # DATA VOLUME ANOMALY
if conn:
    rows = fetch_all(conn, """
        SELECT d.full_date, COUNT(f.sales_key)
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_date d ON f.date_key = d.date_key
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY d.full_date
        ORDER BY d.full_date
    """)

    if not rows:
        report["checks"]["data_volume_anomalies"] = {
            "status": "critical",
            "expected_range": "N/A",
            "actual_count": 0,
            "anomaly_detected": True,
            "anomaly_type": "no_data"
        }

        report["alerts"].append({
            "severity": "critical",
            "check": "data_volume_anomalies",
            "message": "No transaction data found for last 30 days",
            "timestamp": datetime.utcnow().isoformat()
        })

        overall_score -= 25

    else:
        historical_counts = [r[1] for r in rows[:-1]]
        today_count = rows[-1][1]

        mean = statistics.mean(historical_counts) if historical_counts else today_count
        std = statistics.stdev(historical_counts) if len(historical_counts) > 1 else 0

        upper = mean + (3 * std)
        lower = max(0, mean - (3 * std))

        anomaly = today_count > upper or today_count < lower

        report["checks"]["data_volume_anomalies"] = {
            "status": "anomaly_detected" if anomaly else "ok",
            "expected_range": f"{int(lower)}-{int(upper)}",
            "actual_count": today_count,
            "anomaly_detected": anomaly,
            "anomaly_type": (
                "spike" if today_count > upper
                else "drop" if today_count < lower
                else None
            )
        }

        if anomaly:
            report["alerts"].append({
                "severity": "warning",
                "check": "data_volume_anomalies",
                "message": "Transaction volume anomaly detected",
                "timestamp": datetime.utcnow().isoformat()
            })
            overall_score -= 15


        mean = statistics.mean(counts)
        std = statistics.stdev(counts) if len(counts) > 1 else 0

        upper = mean + (3 * std)
        lower = mean - (3 * std)

        anomaly = today_count > upper or today_count < lower

        report["checks"]["data_volume_anomalies"] = {
            "status": "anomaly_detected" if anomaly else "ok",
            "expected_range": f"{int(lower)}-{int(upper)}",
            "actual_count": today_count,
            "anomaly_detected": anomaly,
            "anomaly_type": "spike" if today_count > upper else "drop" if today_count < lower else None
        }

        if anomaly:
            report["alerts"].append({
                "severity": "warning",
                "check": "data_volume_anomalies",
                "message": "Transaction volume anomaly detected",
                "timestamp": datetime.utcnow().isoformat()
            })
            overall_score -= 15

    # DATA QUALITY
    if conn:
        nulls = fetch_one(conn, """
            SELECT COUNT(*) FILTER (WHERE customer_key IS NULL),
                   COUNT(*) FILTER (WHERE product_key IS NULL)
            FROM warehouse.fact_sales
        """)

        null_count = sum(nulls)
        quality_score = max(0, 100 - null_count)

        report["checks"]["data_quality"] = {
            "status": "ok" if quality_score >= 95 else "degraded",
            "quality_score": quality_score,
            "null_violations": null_count
        }

        if quality_score < 95:
            report["alerts"].append({
                "severity": "warning",
                "check": "data_quality",
                "message": "Data quality score below threshold",
                "timestamp": datetime.utcnow().isoformat()
            })
            overall_score -= 10

    if conn:
        conn.close()

    report["overall_health_score"] = max(0, overall_score)
    report["pipeline_health"] = (
        "healthy" if overall_score >= 85 else
        "degraded" if overall_score >= 60 else
        "critical"
    )

    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=4)

    print("Monitoring report generated")

if __name__ == "__main__":
    main()
