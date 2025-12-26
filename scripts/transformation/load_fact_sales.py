import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "ecommerce",
    "user": "postgres",
    "password": "postgres"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_fact_sales():
    conn = get_connection()
    cur = conn.cursor()

    print("Loading warehouse.fact_sales...")

    # Clean existing data
    cur.execute("TRUNCATE TABLE warehouse.fact_sales")

    insert_sql = """
        INSERT INTO warehouse.fact_sales (
            date_key,
            customer_key,
            product_key,
            payment_method_key,
            transaction_id,
            quantity,
            unit_price,
            discount_amount,
            line_total,
            profit,
            created_at
        )
        SELECT
            d.date_key,
            dc.customer_key,
            dp.product_key,
            pm.payment_method_key,
            t.transaction_id,
            ti.quantity,
            ti.unit_price,
            (ti.unit_price * ti.quantity * ti.discount_percentage / 100),
            ti.line_total,
            (ti.line_total - (ti.cost * ti.quantity)),
            t.transaction_date
        FROM public.transaction_items ti
        JOIN public.transactions t
            ON ti.transaction_id = t.transaction_id
        JOIN warehouse.dim_customers dc
            ON t.customer_id = dc.customer_id AND dc.is_current = TRUE
        JOIN warehouse.dim_products dp
            ON ti.product_id = dp.product_id AND dp.is_current = TRUE
        JOIN warehouse.dim_payment_method pm
            ON t.payment_method = pm.payment_method_name
        JOIN warehouse.dim_date d
            ON d.full_date = t.transaction_date;
    """

    cur.execute(insert_sql)
    conn.commit()

    cur.close()
    conn.close()

    print("✅ warehouse.fact_sales loaded successfully")

if __name__ == "__main__":
    load_fact_sales()
