"""Shared pytest fixtures for the Olist ETL test suite."""

import pandas as pd
import pytest


@pytest.fixture
def sample_orders_df() -> pd.DataFrame:
    """One delivered, one shipped, one canceled order."""
    return pd.DataFrame(
        {
            "order_id": ["ord_001", "ord_002", "ord_003"],
            "customer_id": ["cust_001", "cust_002", "cust_003"],
            "order_status": ["delivered", "shipped", "canceled"],
            "order_purchase_timestamp": pd.to_datetime(
                ["2021-01-01 10:00:00", "2021-01-02 11:00:00", "2021-01-03 12:00:00"]
            ),
            "order_approved_at": pd.to_datetime(
                ["2021-01-01 11:00:00", "2021-01-02 12:00:00", None], errors="coerce"
            ),
            "order_delivered_carrier_date": pd.to_datetime(
                ["2021-01-03 10:00:00", None, None], errors="coerce"
            ),
            "order_delivered_customer_date": pd.to_datetime(
                ["2021-01-07 14:00:00", None, None], errors="coerce"
            ),
            "order_estimated_delivery_date": pd.to_datetime(
                ["2021-01-10 00:00:00", "2021-01-15 00:00:00", "2021-01-12 00:00:00"]
            ),
        }
    )


@pytest.fixture
def sample_order_items_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["ord_001", "ord_001", "ord_002"],
            "order_item_id": [1, 2, 1],
            "product_id": ["prod_001", "prod_002", "prod_001"],
            "seller_id": ["sell_001", "sell_001", "sell_002"],
            "shipping_limit_date": pd.to_datetime(
                ["2021-01-05", "2021-01-05", "2021-01-06"]
            ),
            "price": [100.0, 50.0, 200.0],
            "freight_value": [10.0, 5.0, 15.0],
        }
    )


@pytest.fixture
def sample_order_payments_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["ord_001", "ord_002"],
            "payment_sequential": [1, 1],
            "payment_type": ["credit_card", "boleto"],
            "payment_installments": [3, 1],
            "payment_value": [160.0, 215.0],
        }
    )


@pytest.fixture
def sample_customers_df() -> pd.DataFrame:
    """Three rows: two unique customers (cust_001 appears twice)."""
    return pd.DataFrame(
        {
            "customer_id": ["cust_001", "cust_002", "cust_001"],
            "customer_unique_id": ["uniq_001", "uniq_002", "uniq_001"],
            "customer_zip_code_prefix": ["01310", "20040", "01310"],
            "customer_city": ["são paulo", "rio de janeiro", "são paulo"],
            "customer_state": ["sp", "rj", "sp"],
        }
    )


@pytest.fixture
def sample_products_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_id": ["prod_001", "prod_002"],
            "product_category_name": ["cama_mesa_banho", "informatica_acessorios"],
            "product_weight_g": [300, 500],
            "product_length_cm": [20, 30],
            "product_height_cm": [10, 15],
            "product_width_cm": [15, 25],
        }
    )


@pytest.fixture
def sample_category_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_category_name": [
                "cama_mesa_banho",
                "informatica_acessorios",
            ],
            "product_category_name_english": [
                "bed_bath_table",
                "computers_accessories",
            ],
        }
    )
