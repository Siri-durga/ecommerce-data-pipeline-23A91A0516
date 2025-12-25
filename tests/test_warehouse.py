import psycopg2

def test_dimension_tables_exist():
    conn = psycopg2.connect(
        host="localhost", database="ecommerce",
        user="postgres", password="postgres"
    )
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
    conn = psycopg2.connect(
        host="localhost", database="ecommerce",
        user="postgres", password="postgres"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'fact_sales'
    """)
    cols = [r[0] for r in cur.fetchall()]
    assert "customer_key" in cols
    conn.close()
