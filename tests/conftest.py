import os
import psycopg2
import pytest

def is_db_available():
    try:
        psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "ecommerce"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            connect_timeout=3
        ).close()
        return True
    except Exception:
        return False

@pytest.fixture(scope="session")
def db_available():
    if not is_db_available():
        pytest.skip("Database not available, skipping DB-dependent tests")
