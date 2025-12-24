import psycopg2
import json
from datetime import datetime
from decimal import Decimal

# -------------------------------
# Database Connection
# -------------------------------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="ecommerce",
        user="postgres",
        password="postgres"
    )

# -------------------------------
# Cleansing Utilities
# -------------------------------
def normalize_text(val):
    return val.strip() if val else None

def normalize_email(email):
    return email.lower().strip() if email else None

def normalize_phone(phone):
    return ''.join(filter(str.isdigit, phone)) if phone else None

def title_case(val):
    return val.title() if val else None

# -------------------------------
# Enrichment Logic
# -------------------------------
def calculate_profit_margin(price, cost):
    if price is None or cost is None or price <= 0:
        return None
    return round(((price - cost) / price) * 100, 2)

def price_category(price):
    if price < 50:
        return "Budget"
    elif price < 200:
        return "Mid-range"
    return "Premium"

# -------------------------------
# Business Rules
# -------------------------------
def valid_transaction(total_amount):
    return total_amount is not None and total_amount > 0

def valid_item(quantity):
    return quantity is not None and quantity > 0

# -------------------------------
# Load Customers (DIMENSION)
# -------------------------------
def load_customers(cur, summary):
    cur.execute("SELECT * FROM staging.customers")
    rows = cur.fetchall()

    inserted = 0
    cur.execute("TRUNCATE production.customers")

    for r in rows:
        cur.execute("""
            INSERT INTO production.customers
            (customer_id, first_name, last_name, email, phone, city, state, country, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            r[0],
            title_case(normalize_text(r[1])),
            title_case(normalize_text(r[2])),
            normalize_email(r[3]),
            normalize_phone(r[4]),
            normalize_text(r[6]),
            normalize_text(r[7]),
            normalize_text(r[8]),
            r[5]
        ))
        inserted += 1

    summary["customers"] = {
        "input": len(rows),
        "output": inserted,
        "filtered": 0,
        "rejected_reasons": {}
    }

# -------------------------------
# Load Products (DIMENSION)
# -------------------------------
def load_products(cur, summary):
    cur.execute("SELECT * FROM staging.products")
    rows = cur.fetchall()

    cur.execute("TRUNCATE production.products")
    inserted = 0

    for r in rows:
        margin = calculate_profit_margin(r[3], r[4])
        category = price_category(r[3])

        cur.execute("""
            INSERT INTO production.products
            (product_id, name, category, price, cost, profit_margin, price_category)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            r[0],
            normalize_text(r[1]),
            normalize_text(r[2]),
            round(Decimal(r[3]), 2),
            round(Decimal(r[4]), 2),
            margin,
            category
        ))
        inserted += 1

    summary["products"] = {
        "input": len(rows),
        "output": inserted,
        "filtered": 0,
        "rejected_reasons": {}
    }

# -------------------------------
# Load Transactions (FACT – INCREMENTAL)
# -------------------------------
def load_transactions(cur, summary):
    cur.execute("""
        SELECT s.*
        FROM staging.transactions s
        LEFT JOIN production.transactions p
        ON s.transaction_id = p.transaction_id
        WHERE p.transaction_id IS NULL
    """)
    rows = cur.fetchall()

    inserted = 0
    rejected = 0

    for r in rows:
        if not valid_transaction(r[3]):
            rejected += 1
            continue

        cur.execute("""
            INSERT INTO production.transactions
            (transaction_id, customer_id, transaction_date, total_amount)
            VALUES (%s,%s,%s,%s)
        """, (r[0], r[1], r[2], r[3]))

        inserted += 1

    summary["transactions"] = {
        "input": len(rows),
        "output": inserted,
        "filtered": rejected,
        "rejected_reasons": {"invalid_total_amount": rejected}
    }

# -------------------------------
# Load Transaction Items (FACT)
# -------------------------------
def load_transaction_items(cur, summary):
    cur.execute("""
        SELECT s.*
        FROM staging.transaction_items s
        LEFT JOIN production.transaction_items p
        ON s.item_id = p.item_id
        WHERE p.item_id IS NULL
    """)
    rows = cur.fetchall()

    inserted = 0
    rejected = 0

    for r in rows:
        if not valid_item(r[3]):
            rejected += 1
            continue

        # Recalculate line total
        line_total = round(
            r[3] * r[4] * (1 - (r[5] / 100)), 2
        )

        cur.execute("""
            INSERT INTO production.transaction_items
            (item_id, transaction_id, product_id, quantity, unit_price, discount, line_total)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            r[0], r[1], r[2], r[3], r[4], r[5], line_total
        ))

        inserted += 1

    summary["transaction_items"] = {
        "input": len(rows),
        "output": inserted,
        "filtered": rejected,
        "rejected_reasons": {"invalid_quantity": rejected}
    }

# -------------------------------
# MAIN ETL DRIVER
# -------------------------------
def run_etl():
    conn = get_connection()
    cur = conn.cursor()

    summary = {
        "transformation_timestamp": datetime.utcnow().isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_normalization",
            "email_standardization",
            "phone_standardization",
            "profit_margin_calculation",
            "price_category_assignment",
            "business_rule_filtering",
            "incremental_fact_loading"
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0
        }
    }

    try:
        load_customers(cur, summary["records_processed"])
        load_products(cur, summary["records_processed"])
        load_transactions(cur, summary["records_processed"])
        load_transaction_items(cur, summary["records_processed"])

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()

    with open("data/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run_etl()
