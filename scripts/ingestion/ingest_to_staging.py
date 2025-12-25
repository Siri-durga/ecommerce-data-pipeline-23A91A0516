import os
import psycopg2
import csv

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "ecommerce",
    "user": "postgres",
    "password": "postgres"
}

DATA_PATH = "data/raw"

TABLES = {
    "customers": {
        "file": "customers.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.customers (
                customer_id VARCHAR PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                registration_date DATE,
                city VARCHAR,
                state VARCHAR,
                country VARCHAR,
                age_group VARCHAR
            );
        """
    },
    "products": {
        "file": "products.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.products (
                product_id VARCHAR PRIMARY KEY,
                product_name VARCHAR,
                category VARCHAR,
                sub_category VARCHAR,
                price NUMERIC,
                cost NUMERIC,
                brand VARCHAR,
                stock_quantity INTEGER,
                supplier_id VARCHAR
            );
        """
    },
    "transactions": {
        "file": "transactions.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.transactions (
                transaction_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR,
                transaction_date DATE,
                transaction_time TIME,
                payment_method VARCHAR,
                shipping_address VARCHAR,
                total_amount NUMERIC
            );
        """
    },
    "transaction_items": {
        "file": "transaction_items.csv",
        "ddl": """
            CREATE TABLE IF NOT EXISTS public.transaction_items (
                item_id VARCHAR PRIMARY KEY,
                transaction_id VARCHAR,
                product_id VARCHAR,
                quantity INTEGER,
                unit_price NUMERIC,
                discount_percentage NUMERIC,
                line_total NUMERIC,
                cost NUMERIC
            );
        """
    }
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_table(cursor, table_name, config):
    print(f"Loading {table_name}...")

    cursor.execute(config["ddl"])
    cursor.execute(f"TRUNCATE TABLE public.{table_name}")

    file_path = os.path.join(DATA_PATH, config["file"])

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        insert_sql = f"""
            INSERT INTO public.{table_name} ({",".join(headers)})
            VALUES ({",".join(["%s"] * len(headers))})
        """

        for row in reader:
            cursor.execute(insert_sql, row)

def main():
    conn = get_connection()
    cur = conn.cursor()

    for table, config in TABLES.items():
        load_table(cur, table, config)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Ingestion completed successfully")

if __name__ == "__main__":
    main()
