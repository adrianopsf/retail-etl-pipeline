import pytest
import pandas as pd


@pytest.fixture
def sample_orders_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["ord_001", "ord_002", "ord_003"],
            "customer_id": ["cust_001", "cust_002", "cust_003"],
            "order_status": ["delivered", "shipped", "canceled"],
            "order_purchase_timestamp": [
                "2021-01-01 10:00:00",
                "2021-01-02 11:00:00",
                "2021-01-03 12:00:00",
            ],
            "order_approved_at": [
                "2021-01-01 11:00:00",
                "2021-01-02 12:00:00",
                None,
            ],
            "order_delivered_carrier_date": [
                "2021-01-03 10:00:00",
                None,
                None,
            ],
            "order_delivered_customer_date": [
                "2021-01-07 14:00:00",
                None,
                None,
            ],
            "order_estimated_delivery_date": [
                "2021-01-10 00:00:00",
                "2021-01-15 00:00:00",
                "2021-01-12 00:00:00",
            ],
        }
    )


@pytest.fixture
def sample_customers_df() -> pd.DataFrame:
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
        }
    )


@pytest.fixture
def sample_category_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "product_category_name": ["cama_mesa_banho", "informatica_acessorios"],
            "product_category_name_english": ["bed_bath_table", "computers_accessories"],
        }
    )
