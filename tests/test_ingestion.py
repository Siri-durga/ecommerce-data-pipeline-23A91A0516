import psycopg2
import os

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "ecommerce"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

def test_database_connection(db_available):
    conn = get_connection()
    assert conn is not None
    conn.close()

def test_warehouse_tables_exist(db_available):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'warehouse'
    """)
    tables = [r[0] for r in cur.fetchall()]
    assert "fact_sales" in tables
    assert "dim_customers" in tables
    conn.close()
