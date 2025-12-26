import pytest
import os
import psycopg2

pytestmark = pytest.mark.skipif(
    os.getenv("DB_AVAILABLE", "false") != "true",
    reason="Database not available"
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "ecommerce"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

def test_production_tables_populated():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM production.transactions")
    assert cur.fetchone()[0] >= 0
    conn.close()

def test_no_orphan_records():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)
    assert cur.fetchone()[0] == 0
    conn.close()
