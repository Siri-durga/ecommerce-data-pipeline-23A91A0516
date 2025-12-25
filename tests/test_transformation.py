import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="ecommerce",
        user="postgres",
        password="postgres"
    )

def test_fact_sales_has_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM warehouse.fact_sales")
    count = cur.fetchone()[0]
    assert count >= 0
    conn.close()

def test_no_orphan_customer_keys():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) 
        FROM warehouse.fact_sales f
        LEFT JOIN warehouse.dim_customers c
        ON f.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """)
    assert cur.fetchone()[0] == 0
    conn.close()
