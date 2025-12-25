import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="ecommerce",
        user="postgres",
        password="postgres"
    )

def test_db_connection():
    conn = get_connection()
    assert conn is not None
    conn.close()

def test_staging_tables_exist():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
    """)
    tables = [r[0] for r in cur.fetchall()]
    assert "customers" in tables
    conn.close()
