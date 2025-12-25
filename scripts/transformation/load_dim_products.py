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

def load_dim_products():
    conn = get_connection()
    cur = conn.cursor()

    print("Loading warehouse.dim_products...")

    cur.execute("DELETE FROM warehouse.dim_products")

    insert_sql = """
        INSERT INTO warehouse.dim_products (
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            price_range,
            effective_date,
            end_date,
            is_current
        )
        SELECT
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            CASE
                WHEN price < 500 THEN 'Budget'
                WHEN price < 2000 THEN 'Mid-range'
                ELSE 'Premium'
            END AS price_range,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM public.products;
    """

    cur.execute(insert_sql)
    conn.commit()

    cur.close()
    conn.close()
    print("✅ dim_products loaded")

if __name__ == "__main__":
    load_dim_products()
