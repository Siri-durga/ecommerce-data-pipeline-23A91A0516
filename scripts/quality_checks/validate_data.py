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


# ---------------- CONNECTION ----------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def run_scalar(cursor, query):
    cursor.execute(query)
    return cursor.fetchone()[0]


# ---------------- QUALITY CHECKS ----------------
def completeness_checks(cursor):
    details = {
        "staging.customers.email": run_scalar(
            cursor,
            "SELECT COUNT(*) FROM staging.customers WHERE email IS NULL OR email = ''"
        ),
        "staging.transactions.total_amount": run_scalar(
            cursor,
            "SELECT COUNT(*) FROM staging.transactions WHERE total_amount IS NULL"
        )
    }

    violations = sum(details.values())
    status = "passed" if violations == 0 else "failed"

    return {
        "status": status,
        "tables_checked": ["staging.customers", "staging.transactions"],
        "null_violations": violations,
        "details": details
    }


def uniqueness_checks(cursor):
    customer_dup = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM (
            SELECT customer_id
            FROM staging.customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ) d
        """
    )

    email_dup = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM (
            SELECT email
            FROM staging.customers
            GROUP BY email
            HAVING COUNT(*) > 1
        ) d
        """
    )

    total = customer_dup + email_dup
    status = "passed" if total == 0 else "failed"

    return {
        "status": status,
        "duplicates_found": total,
        "details": {
            "duplicate_customer_ids": customer_dup,
            "duplicate_emails": email_dup
        }
    }


def referential_integrity_checks(cursor):
    orphan_tx = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transactions t
        LEFT JOIN staging.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """
    )

    orphan_items_tx = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transaction_items ti
        LEFT JOIN staging.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
        """
    )

    orphan_items_prod = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transaction_items ti
        LEFT JOIN staging.products p
        ON ti.product_id = p.product_id
        WHERE p.product_id IS NULL
        """
    )

    total = orphan_tx + orphan_items_tx + orphan_items_prod
    status = "passed" if total == 0 else "failed"

    return {
        "status": status,
        "orphan_records": total,
        "details": {
            "transactions_without_customers": orphan_tx,
            "items_without_transactions": orphan_items_tx,
            "items_without_products": orphan_items_prod
        }
    }


def validity_range_checks(cursor):
    price_violations = run_scalar(
        cursor,
        "SELECT COUNT(*) FROM staging.products WHERE price <= 0 OR cost < 0"
    )

    discount_violations = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transaction_items
        WHERE discount_percentage < 0 OR discount_percentage > 100
        """
    )

    quantity_violations = run_scalar(
        cursor,
        "SELECT COUNT(*) FROM staging.transaction_items WHERE quantity <= 0"
    )

    total = price_violations + discount_violations + quantity_violations
    status = "passed" if total == 0 else "failed"

    return {
        "status": status,
        "violations": total,
        "details": {
            "invalid_prices_or_costs": price_violations,
            "invalid_discounts": discount_violations,
            "invalid_quantities": quantity_violations
        }
    }


def consistency_checks(cursor):
    mismatches = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transaction_items
        WHERE ABS(
            line_total -
            (quantity * unit_price * (1 - discount_percentage / 100.0))
        ) > 0.01
        """
    )

    status = "passed" if mismatches == 0 else "failed"

    return {
        "status": status,
        "mismatches": mismatches,
        "details": {
            "line_total_formula_mismatch": mismatches
        }
    }


def accuracy_business_rules(cursor):
    future_tx = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transactions
        WHERE transaction_date > CURRENT_DATE
        """
    )

    reg_after_tx = run_scalar(
        cursor,
        """
        SELECT COUNT(*) FROM staging.transactions t
        JOIN staging.customers c
        ON t.customer_id = c.customer_id
        WHERE t.transaction_date < c.registration_date
        """
    )

    total = future_tx + reg_after_tx
    status = "passed" if total == 0 else "failed"

    return {
        "status": status,
        "violations": total,
        "details": {
            "future_transactions": future_tx,
            "registration_after_transaction": reg_after_tx
        }
    }


# ---------------- SCORING ----------------
def calculate_weighted_score(results):
    weights = {
        "completeness": 0.20,
        "uniqueness": 0.15,
        "referential": 0.30,
        "validity": 0.15,
        "consistency": 0.10,
        "accuracy": 0.10
    }

    score = 100
    for key, weight in weights.items():
        if results[key]["status"] == "failed":
            score -= weight * 100

    return max(0, round(score, 2))


# ---------------- MAIN ----------------
def validate():
    conn = get_connection()
    cursor = conn.cursor()

    completeness = completeness_checks(cursor)
    uniqueness = uniqueness_checks(cursor)
    referential = referential_integrity_checks(cursor)
    validity = validity_range_checks(cursor)
    consistency = consistency_checks(cursor)
    accuracy = accuracy_business_rules(cursor)

    results = {
        "completeness": completeness,
        "uniqueness": uniqueness,
        "referential": referential,
        "validity": validity,
        "consistency": consistency,
        "accuracy": accuracy
    }

    overall_score = calculate_weighted_score(results)

    grade = (
        "A" if overall_score >= 90 else
        "B" if overall_score >= 80 else
        "C" if overall_score >= 70 else
        "D" if overall_score >= 60 else
        "F"
    )

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": {
            "null_checks": completeness,
            "duplicate_checks": uniqueness,
            "referential_integrity": referential,
            "range_checks": validity,
            "data_consistency": consistency,
            "accuracy_business_rules": accuracy
        },
        "overall_quality_score": overall_score,
        "quality_grade": grade
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    conn.close()


if __name__ == "__main__":
    validate()
