"""Shared pytest fixtures for the Olist ETL test suite.

Fixture scopes:
- ``engine``  → session: one DB connection for the whole test run
- DataFrames  → function: fresh copy per test (no cross-test mutation)
"""


import pandas as pd
import pytest
from sqlalchemy import text

from src.utils.db_connection import get_engine

# ------------------------------------------------------------------
# Database engine (integration tests only — skipped without a DB)
# ------------------------------------------------------------------


@pytest.fixture(scope="session")
def engine():
    """Return a live SQLAlchemy engine, or skip if PostgreSQL is unavailable.

    The fixture is session-scoped so the connection pool is reused across
    all integration tests.  It is safe to use in unit test runs — any test
    that depends on it will simply be skipped if no database is reachable.
    """
    try:
        eng = get_engine()
        # Verify connectivity and bootstrap schemas that the init.sql would
        # normally create via Docker Compose — required when running in CI
        # against a bare PostgreSQL service container.
        with eng.begin() as conn:
            conn.execute(text("SELECT 1"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        yield eng
        eng.dispose()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available — skipping integration tests: {exc}")


# ------------------------------------------------------------------
# Orders
# ------------------------------------------------------------------


@pytest.fixture
def sample_orders_df() -> pd.DataFrame:
    """10 orders: 7 delivered (including 2 late), 2 shipped, 1 canceled."""
    return pd.DataFrame(
        {
            "order_id": [
                "ord_001", "ord_002", "ord_003", "ord_004", "ord_005",
                "ord_006", "ord_007", "ord_008", "ord_009", "ord_010",
            ],
            "customer_id": [
                "cust_001", "cust_002", "cust_003", "cust_004", "cust_005",
                "cust_006", "cust_007", "cust_008", "cust_009", "cust_010",
            ],
            "order_status": [
                "delivered", "delivered", "delivered", "delivered", "delivered",
                "delivered", "delivered", "shipped",  "shipped",  "canceled",
            ],
            "order_purchase_timestamp": pd.to_datetime([
                "2021-01-05 08:30:00", "2021-02-10 14:00:00", "2021-03-15 09:15:00",
                "2021-04-20 11:00:00", "2021-05-25 16:45:00", "2021-06-30 10:30:00",
                "2021-07-04 13:00:00", "2021-08-08 08:00:00", "2021-09-12 17:30:00",
                "2021-10-01 12:00:00",
            ]),
            "order_approved_at": pd.to_datetime([
                "2021-01-05 09:00:00", "2021-02-10 15:00:00", "2021-03-15 10:00:00",
                "2021-04-20 12:00:00", "2021-05-25 17:00:00", "2021-06-30 11:00:00",
                "2021-07-04 14:00:00", "2021-08-08 09:00:00", None, None,
            ], errors="coerce"),
            "order_delivered_carrier_date": pd.to_datetime([
                "2021-01-07 10:00:00", "2021-02-12 09:00:00", "2021-03-17 11:00:00",
                "2021-04-22 08:00:00", "2021-05-28 14:00:00", "2021-07-03 10:00:00",
                "2021-07-07 09:00:00", None, None, None,
            ], errors="coerce"),
            "order_delivered_customer_date": pd.to_datetime([
                "2021-01-12 14:00:00", "2021-02-16 18:00:00", "2021-03-22 10:00:00",
                "2021-04-28 16:00:00", "2021-06-03 09:00:00", "2021-07-10 11:00:00",
                "2021-07-15 13:00:00", None, None, None,
            ], errors="coerce"),
            "order_estimated_delivery_date": pd.to_datetime([
                "2021-01-20 00:00:00", "2021-02-14 00:00:00", "2021-03-30 00:00:00",
                "2021-04-30 00:00:00", "2021-06-10 00:00:00", "2021-07-05 00:00:00",
                "2021-07-20 00:00:00", "2021-08-20 00:00:00", "2021-09-25 00:00:00",
                "2021-10-15 00:00:00",
            ]),
        }
    )


# ------------------------------------------------------------------
# Order items
# ------------------------------------------------------------------


@pytest.fixture
def sample_order_items_df() -> pd.DataFrame:
    """12 items across the first 7 delivered orders."""
    return pd.DataFrame(
        {
            "order_id": [
                "ord_001", "ord_001", "ord_002", "ord_003", "ord_003",
                "ord_004", "ord_005", "ord_005", "ord_005", "ord_006",
                "ord_007", "ord_007",
            ],
            "order_item_id": [1, 2, 1, 1, 2, 1, 1, 2, 3, 1, 1, 2],
            "product_id": [
                "prod_001", "prod_002", "prod_003", "prod_001", "prod_004",
                "prod_002", "prod_003", "prod_005", "prod_001", "prod_004",
                "prod_005", "prod_002",
            ],
            "seller_id": [
                "sell_001", "sell_001", "sell_002", "sell_001", "sell_003",
                "sell_002", "sell_003", "sell_001", "sell_002", "sell_003",
                "sell_001", "sell_002",
            ],
            "shipping_limit_date": pd.to_datetime([
                "2021-01-08", "2021-01-08", "2021-02-13", "2021-03-18",
                "2021-03-18", "2021-04-23", "2021-05-27", "2021-05-27",
                "2021-05-27", "2021-07-02", "2021-07-06", "2021-07-06",
            ]),
            "price": [
                120.00, 80.00, 250.00, 95.00, 45.00,
                180.00, 320.00, 60.00, 40.00, 150.00,
                200.00, 110.00,
            ],
            "freight_value": [
                12.50, 8.00, 18.00, 9.50, 5.00,
                15.00, 22.00, 6.50, 4.00, 13.50,
                17.00, 11.00,
            ],
        }
    )


# ------------------------------------------------------------------
# Order payments
# ------------------------------------------------------------------


@pytest.fixture
def sample_order_payments_df() -> pd.DataFrame:
    """One payment row per delivered order (7 rows)."""
    return pd.DataFrame(
        {
            "order_id": [
                "ord_001", "ord_002", "ord_003", "ord_004",
                "ord_005", "ord_006", "ord_007",
            ],
            "payment_sequential": [1, 1, 1, 1, 1, 1, 1],
            "payment_type": [
                "credit_card", "boleto", "credit_card", "voucher",
                "credit_card", "boleto", "credit_card",
            ],
            "payment_installments": [3, 1, 6, 1, 12, 1, 3],
            "payment_value": [
                200.00, 250.00, 140.00, 180.00,
                420.00, 150.00, 310.00,
            ],
        }
    )


# ------------------------------------------------------------------
# Customers
# ------------------------------------------------------------------


@pytest.fixture
def sample_customers_df() -> pd.DataFrame:
    """10 rows covering 8 unique customers (cust_001 and cust_004 repeat)."""
    return pd.DataFrame(
        {
            "customer_id": [
                "cust_001", "cust_002", "cust_003", "cust_004", "cust_005",
                "cust_006", "cust_007", "cust_008", "cust_001", "cust_004",
            ],
            "customer_unique_id": [
                "uniq_001", "uniq_002", "uniq_003", "uniq_004", "uniq_005",
                "uniq_006", "uniq_007", "uniq_008", "uniq_001", "uniq_004",
            ],
            "customer_zip_code_prefix": [
                "01310", "20040", "30130", "40020", "60150",
                "70040", "80010", "90035", "01310", "40020",
            ],
            "customer_city": [
                "são paulo", "rio de janeiro", "belo horizonte", "salvador",
                "fortaleza",  "brasilia",       "curitiba",       "porto alegre",
                "são paulo", "salvador",
            ],
            "customer_state": [
                "sp", "rj", "mg", "ba", "ce",
                "df", "pr", "rs", "sp", "ba",
            ],
        }
    )


# ------------------------------------------------------------------
# Products
# ------------------------------------------------------------------


@pytest.fixture
def sample_products_df() -> pd.DataFrame:
    """5 products across 3 categories."""
    return pd.DataFrame(
        {
            "product_id": [
                "prod_001", "prod_002", "prod_003", "prod_004", "prod_005",
            ],
            "product_category_name": [
                "cama_mesa_banho", "informatica_acessorios", "cama_mesa_banho",
                "esporte_lazer", "informatica_acessorios",
            ],
            "product_name_length": [50, 60, 45, 55, 70],
            "product_description_length": [250, 300, 200, 280, 350],
            "product_photos_qty": [3, 4, 2, 5, 3],
            "product_weight_g": [300, 500, 200, 800, 400],
            "product_length_cm": [20, 30, 15, 40, 25],
            "product_height_cm": [10, 15, 8, 20, 12],
            "product_width_cm": [15, 25, 12, 30, 20],
        }
    )


# ------------------------------------------------------------------
# Category translation
# ------------------------------------------------------------------


@pytest.fixture
def sample_category_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_category_name": [
                "cama_mesa_banho",
                "informatica_acessorios",
                "esporte_lazer",
            ],
            "product_category_name_english": [
                "bed_bath_table",
                "computers_accessories",
                "sports_leisure",
            ],
        }
    )
