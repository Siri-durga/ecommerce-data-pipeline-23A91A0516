import pandas as pd
import os
import re
import pytest

CI = os.getenv("CI") == "true"


DATA_DIR = "data/raw"

@pytest.mark.skipif(CI, reason="Skip data generation tests in CI")
def test_csv_files_exist():

    files = ["customers.csv", "products.csv", "transactions.csv", "transaction_items.csv"]
    for f in files:
        assert os.path.exists(os.path.join(DATA_DIR, f)), f"{f} not generated"

def test_required_columns_exist():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    assert "customer_id" in customers.columns
    assert "email" in customers.columns

def test_no_null_customer_id():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    assert customers["customer_id"].isnull().sum() == 0

def test_email_format():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    for email in customers["email"].head(50):
        assert re.match(r"[^@]+@[^@]+\.[^@]+", email)

def test_referential_integrity_transactions():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    transactions = pd.read_csv(f"{DATA_DIR}/transactions.csv")
    assert set(transactions["customer_id"]).issubset(set(customers["customer_id"]))
