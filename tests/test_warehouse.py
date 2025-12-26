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

def test_dimension_tables_exist():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'warehouse'
    """)
    tables = [r[0] for r in cur.fetchall()]
    assert "dim_customers" in tables
    assert "fact_sales" in tables
    conn.close()

def test_fact_table_has_surrogate_keys():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='warehouse'
          AND table_name='fact_sales'
    """)
    cols = [r[0] for r in cur.fetchall()]
    assert "customer_key" in cols
    conn.close()
