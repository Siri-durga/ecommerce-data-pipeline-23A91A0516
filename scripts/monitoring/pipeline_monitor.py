import psycopg2
import json
import time
import os
import statistics
from datetime import datetime, timedelta

OUTPUT_FILE = "data/processed/monitoring_report.json"

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

def fetch_all(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows

def fetch_one(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    cur.close()
    return row

def main():
    report = {
        "monitoring_timestamp": datetime.utcnow().isoformat(),
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }

    conn = None  # ✅ defined ONCE, always exists

    # ---------------- DATABASE CONNECTIVITY ----------------
    try:
        conn, response_time = get_connection()
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
        report["pipeline_health"] = "critical"
        report["overall_health_score"] -= 40

    # ---------------- DATA FRESHNESS ----------------
    if conn:
        latest = fetch_one(
            conn,
            "SELECT MAX(created_at) FROM warehouse.fact_sales"
        )[0]

        if latest is None:
            hours_lag = 999
        else:
            hours_lag = (datetime.utcnow() - latest).total_seconds() / 3600

        status = "ok" if hours_lag < 24 else "critical"

        report["checks"]["data_freshness"] = {
            "status": status,
            "warehouse_latest_record": str(latest),
            "max_lag_hours": round(hours_lag, 2)
        }

        if status == "critical":
            report["alerts"].append({
                "severity": "critical",
                "check": "data_freshness",
                "message": "Warehouse data is stale (>24 hours)",
                "timestamp": datetime.utcnow().isoformat()
            })
            report["pipeline_health"] = "critical"
            report["overall_health_score"] -= 20

    # ---------------- DATA VOLUME ANOMALY ----------------
    if conn:
        rows = fetch_all(
            conn,
            """
            SELECT d.full_date, COUNT(f.sales_key)
            FROM warehouse.fact_sales f
            JOIN warehouse.dim_date d ON f.date_key = d.date_key
            WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY d.full_date
            ORDER BY d.full_date
            """
        )

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
                "message": "No transactions found in last 30 days",
                "timestamp": datetime.utcnow().isoformat()
            })

            report["pipeline_health"] = "critical"
            report["overall_health_score"] -= 25

        else:
            historical = [r[1] for r in rows[:-1]]
            today_count = rows[-1][1]

            mean = statistics.mean(historical) if historical else today_count
            std = statistics.stdev(historical) if len(historical) > 1 else 0

            upper = mean + (3 * std)
            lower = max(0, mean - (3 * std))

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
                report["overall_health_score"] -= 10

    # ---------------- DATA QUALITY ----------------
    if conn:
        row = fetch_one(
            conn,
            """
            SELECT
                COUNT(*) FILTER (WHERE customer_key IS NULL),
                COUNT(*) FILTER (WHERE product_key IS NULL)
            FROM warehouse.fact_sales
            """
        )

        nulls = sum(row)
        quality_score = max(0, 100 - nulls)

        report["checks"]["data_quality"] = {
            "status": "ok" if quality_score >= 95 else "degraded",
            "quality_score": quality_score,
            "null_violations": nulls
        }

        if quality_score < 95:
            report["alerts"].append({
                "severity": "warning",
                "check": "data_quality",
                "message": "Data quality score below threshold",
                "timestamp": datetime.utcnow().isoformat()
            })
            report["overall_health_score"] -= 10

    if conn:
        conn.close()

    # ---------------- FINAL STATUS ----------------
    score = report["overall_health_score"]
    report["pipeline_health"] = (
        "healthy" if score >= 85 else
        "degraded" if score >= 60 else
        "critical"
    )

    os.makedirs("data/processed", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=4)

    print("Monitoring report generated successfully")

if __name__ == "__main__":
    main()
