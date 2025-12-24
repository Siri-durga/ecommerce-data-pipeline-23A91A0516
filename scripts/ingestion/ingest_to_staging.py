import os
import json
import time
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "ecommerce_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

RAW_DATA_PATH = "data/raw"
STAGING_SCHEMA = "staging"

TABLE_FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "transactions": "transactions.csv",
    "transaction_items": "transaction_items.csv"
}

SUMMARY_PATH = "data/staging/ingestion_summary.json"
LOG_FILE = f"logs/ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

os.makedirs("logs", exist_ok=True)
os.makedirs("data/staging", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
def copy_csv_to_table(cursor, table_name, csv_file):
    columns = {
        "customers": """
            customer_id, first_name, last_name, email, phone,
            registration_date, city, state, country, age_group
        """,
        "products": """
            product_id, product_name, category, sub_category,
            price, cost, brand, stock_quantity, supplier_id
        """,
        "transactions": """
            transaction_id, customer_id, transaction_date,
            transaction_time, payment_method, shipping_address, total_amount
        """,
        "transaction_items": """
            item_id, transaction_id, product_id, quantity,
            unit_price, discount_percentage, line_total
        """
    }

    copy_sql = f"""
        COPY {STAGING_SCHEMA}.{table_name} ({columns[table_name]})
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE,
            DELIMITER ',',
            QUOTE '\"'
        )
    """

    with open(csv_file, "r", encoding="utf-8") as f:
        cursor.copy_expert(copy_sql, f)

# --------------------------------------------------
def ingest():
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        conn.autocommit = False

        # Truncate tables
        for table in TABLE_FILES:
            cursor.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.{table}")
            logging.info(f"Truncated {table}")

        # Load each table
        for table, file in TABLE_FILES.items():
            path = os.path.join(RAW_DATA_PATH, file)

            copy_csv_to_table(cursor, table, path)

            cursor.execute(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{table}")
            rows = cursor.fetchone()[0]

            summary["tables_loaded"][f"{STAGING_SCHEMA}.{table}"] = {
                "rows_loaded": rows,
                "status": "success",
                "error_message": None
            }

        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()

        for table in TABLE_FILES:
            summary["tables_loaded"].setdefault(
                f"{STAGING_SCHEMA}.{table}",
                {
                    "rows_loaded": 0,
                    "status": "failed",
                    "error_message": str(e)
                }
            )

    finally:
        if conn:
            conn.close()

        summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)
        with open(SUMMARY_PATH, "w") as f:
            json.dump(summary, f, indent=4)

# --------------------------------------------------
if __name__ == "__main__":
    ingest()
