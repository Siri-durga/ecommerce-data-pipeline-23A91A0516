import psycopg2

def test_production_tables_populated():
    conn = psycopg2.connect(
        host="localhost", database="ecommerce",
        user="postgres", password="postgres"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM production.transactions")
    count = cur.fetchone()[0]
    assert count > 0
    conn.close()

def test_no_orphan_records():
    conn = psycopg2.connect(
        host="localhost", database="ecommerce",
        user="postgres", password="postgres"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)
    assert cur.fetchone()[0] == 0
    conn.close()
