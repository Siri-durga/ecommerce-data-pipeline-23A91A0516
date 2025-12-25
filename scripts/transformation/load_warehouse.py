import psycopg2
from datetime import date, timedelta
import calendar

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="ecommerce",
        user="postgres",
        password="postgres"
    )

# =========================
# DATE DIMENSION
# =========================
def build_dim_date(start_date, end_date, conn):
    cur = conn.cursor()
    current = start_date

    while current <= end_date:
        date_key = int(current.strftime("%Y%m%d"))
        cur.execute("""
            INSERT INTO warehouse.dim_date
            (date_key, full_date, year, quarter, month, day,
             month_name, day_name, week_of_year, is_weekend, is_holiday)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_key) DO NOTHING
        """, (
            date_key,
            current,
            current.year,
            (current.month - 1)//3 + 1,
            current.month,
            current.day,
            calendar.month_name[current.month],
            calendar.day_name[current.weekday()],
            current.isocalendar()[1],
            current.weekday() >= 5,
            False
        ))
        current += timedelta(days=1)

    conn.commit()
    cur.close()

# =========================
# PAYMENT METHOD DIM
# =========================
def load_payment_methods(conn):
    cur = conn.cursor()
    data = [
        ("Credit Card", "Online"),
        ("Debit Card", "Online"),
        ("UPI", "Online"),
        ("Net Banking", "Online"),
        ("Cash on Delivery", "Offline")
    ]
    for row in data:
        cur.execute("""
            INSERT INTO warehouse.dim_payment_method
            (payment_method_name, payment_type)
            VALUES (%s,%s)
            ON CONFLICT DO NOTHING
        """, row)

    conn.commit()
    cur.close()

# =========================
# AGGREGATES
# =========================
def build_aggregates(conn):
    cur = conn.cursor()

    cur.execute("TRUNCATE warehouse.agg_daily_sales")
    cur.execute("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT date_key,
               COUNT(DISTINCT transaction_id),
               SUM(line_total),
               SUM(profit),
               COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key
    """)

    cur.execute("TRUNCATE warehouse.agg_product_performance")
    cur.execute("""
        INSERT INTO warehouse.agg_product_performance
        SELECT product_key,
               SUM(quantity),
               SUM(line_total),
               SUM(profit),
               AVG(discount_amount)
        FROM warehouse.fact_sales
        GROUP BY product_key
    """)

    cur.execute("TRUNCATE warehouse.agg_customer_metrics")
    cur.execute("""
        INSERT INTO warehouse.agg_customer_metrics
        SELECT customer_key,
               COUNT(DISTINCT transaction_id),
               SUM(line_total),
               AVG(line_total),
               MAX(created_at::DATE)
        FROM warehouse.fact_sales
        GROUP BY customer_key
    """)

    conn.commit()
    cur.close()

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    conn = get_connection()

    build_dim_date(date(2024,1,1), date(2024,12,31), conn)
    load_payment_methods(conn)
    build_aggregates(conn)

    conn.close()
    print("Warehouse loading completed")
