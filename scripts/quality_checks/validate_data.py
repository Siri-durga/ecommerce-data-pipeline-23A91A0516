import psycopg2
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

OUTPUT_PATH = "data/quality/data_quality_report.json"

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def run_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

def validate():
    conn = get_connection()
    cursor = conn.cursor()

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": {},
        "overall_quality_score": 100,
        "quality_grade": "A"
    }

    # ---------- NULL CHECK ----------
    cursor.execute("""
        SELECT COUNT(*) FROM production.customers
        WHERE email IS NULL OR email = '';
    """)
    null_violations = cursor.fetchone()[0]

    report["checks_performed"]["null_checks"] = {
        "status": "passed" if null_violations == 0 else "failed",
        "null_violations": null_violations
    }

    # ---------- DUPLICATES ----------
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT email FROM production.customers
            GROUP BY email HAVING COUNT(*) > 1
        ) d;
    """)
    duplicates = cursor.fetchone()[0]

    report["checks_performed"]["duplicate_checks"] = {
        "status": "passed" if duplicates == 0 else "failed",
        "duplicates_found": duplicates
    }

    # ---------- REFERENTIAL ----------
    cursor.execute("""
        SELECT COUNT(*) FROM production.transactions t
        LEFT JOIN production.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL;
    """)
    orphan_txn = cursor.fetchone()[0]

    report["checks_performed"]["referential_integrity"] = {
        "status": "passed" if orphan_txn == 0 else "failed",
        "orphan_records": orphan_txn
    }

    # ---------- CONSISTENCY ----------
    cursor.execute("""
        SELECT COUNT(*) FROM production.transaction_items
        WHERE ABS(
            line_total -
            (quantity * unit_price * (1 - discount_percentage / 100.0))
        ) > 0.01;
    """)
    mismatches = cursor.fetchone()[0]

    report["checks_performed"]["data_consistency"] = {
        "status": "passed" if mismatches == 0 else "failed",
        "mismatches": mismatches
    }

    # ---------- SCORING ----------
    penalty = (
        null_violations * 2 +
        duplicates * 2 +
        orphan_txn * 5 +
        mismatches * 2
    )

    score = max(0, 100 - penalty)
    report["overall_quality_score"] = score

    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    elif score >= 60:
        grade = "D"
    else:
        grade = "F"

    report["quality_grade"] = grade

    with open(OUTPUT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    conn.close()

if __name__ == "__main__":
    validate()
