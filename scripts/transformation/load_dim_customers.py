import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "ecommerce",
    "user": "postgres",
    "password": "postgres"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_dim_customers():
    conn = get_connection()
    cur = conn.cursor()

    print("Loading warehouse.dim_customers...")

    # SAFE delete (not truncate)
    cur.execute("DELETE FROM warehouse.dim_customers")

    insert_sql = """
        INSERT INTO warehouse.dim_customers (
            customer_id,
            full_name,
            email,
            city,
            state,
            country,
            age_group,
            customer_segment,
            registration_date,
            effective_date,
            end_date,
            is_current
        )
        SELECT
            customer_id,
            first_name || ' ' || last_name,
            email,
            city,
            state,
            country,
            age_group,
            'New',
            registration_date,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM public.customers;
    """

    cur.execute(insert_sql)
    conn.commit()

    cur.close()
    conn.close()

    print("✅ dim_customers loaded")

if __name__ == "__main__":
    load_dim_customers()
