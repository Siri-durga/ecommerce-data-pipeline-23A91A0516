import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime
import yaml
from pathlib import Path

fake = Faker()

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"


# ---------------- CONFIG LOADER ----------------
def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


# ---------------- DATA GENERATION ----------------
def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []
    used_emails = set()

    for i in range(1, num_customers + 1):
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        })

    return pd.DataFrame(customers)


def generate_products(num_products: int) -> pd.DataFrame:
    categories = {
        "Electronics": ["Mobile", "Laptop", "Accessories"],
        "Clothing": ["Men", "Women", "Kids"],
        "Home & Kitchen": ["Appliances", "Decor"],
        "Books": ["Fiction", "Education"],
        "Sports": ["Outdoor", "Indoor"],
        "Beauty": ["Skincare", "Makeup"]
    }

    products = []

    for i in range(1, num_products + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(0, 500),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })

    return pd.DataFrame(products)


def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    transactions = []
    customer_ids = customers_df["customer_id"].tolist()
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]

    for i in range(1, num_transactions + 1):
        txn_datetime = fake.date_time_between(start_date="-1y", end_date="now")

        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": txn_datetime.date(),
            "transaction_time": txn_datetime.time(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", ")
        })

    return pd.DataFrame(transactions)


def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    items = []
    item_counter = 1

    product_price_map = products_df.set_index("product_id")["price"].to_dict()

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)
        chosen_products = random.sample(list(product_price_map.keys()), num_items)

        for prod_id in chosen_products:
            quantity = random.randint(1, 4)
            unit_price = product_price_map[prod_id]
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                quantity * unit_price * (1 - discount / 100),
                2
            )

            items.append({
                "item_id": f"ITEM{item_counter:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": prod_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percentage": discount,
                "line_total": line_total
            })
            item_counter += 1

    return pd.DataFrame(items)


# ---------------- VALIDATION ----------------
def validate_referential_integrity(customers, products, transactions, items) -> dict:
    issues = 0

    if not set(transactions["customer_id"]).issubset(customers["customer_id"]):
        issues += 1

    if not set(items["transaction_id"]).issubset(transactions["transaction_id"]):
        issues += 1

    if not set(items["product_id"]).issubset(products["product_id"]):
        issues += 1

    return {
        "orphan_records": issues,
        "constraint_violations": issues,
        "data_quality_score": 100 if issues == 0 else max(0, 100 - issues * 10)
    }


# ---------------- MAIN ----------------
def main():
    config = load_config()
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    customers = generate_customers(config["data_generation"]["customers"])
    products = generate_products(config["data_generation"]["products"])
    transactions = generate_transactions(config["data_generation"]["transactions"], customers)
    items = generate_transaction_items(transactions, products)

    # ---- Calculate total_amount correctly ----
    totals = (
        items
        .groupby("transaction_id", as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "total_amount"})
    )

    transactions = transactions.merge(totals, on="transaction_id", how="left")
    transactions["total_amount"] = transactions["total_amount"].round(2)

    # ---- Save CSVs ----
    customers.to_csv(RAW_DATA_DIR / "customers.csv", index=False)
    products.to_csv(RAW_DATA_DIR / "products.csv", index=False)
    transactions.to_csv(RAW_DATA_DIR / "transactions.csv", index=False)
    items.to_csv(RAW_DATA_DIR / "transaction_items.csv", index=False)

    validation = validate_referential_integrity(customers, products, transactions, items)

    metadata = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items)
        },
        "date_range": {
            "start": str(transactions["transaction_date"].min()),
            "end": str(transactions["transaction_date"].max())
        },
        "data_quality": validation
    }

    with open(RAW_DATA_DIR / "generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("✅ Data generation completed successfully.")


if __name__ == "__main__":
    main()
