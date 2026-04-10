import pandas as pd
from src.transform.orders_transformer import transform_orders
from src.transform.customers_transformer import transform_customers
from src.transform.products_transformer import transform_products


def test_transform_orders_parses_datetimes(sample_orders_df):
    result = transform_orders(sample_orders_df)
    assert pd.api.types.is_datetime64_any_dtype(result["order_purchase_timestamp"])


def test_transform_orders_computes_delivery_days(sample_orders_df):
    result = transform_orders(sample_orders_df)
    delivered = result[result["order_id"] == "ord_001"]
    assert delivered["delivery_days"].iloc[0] == 6


def test_transform_orders_flags_late_deliveries(sample_orders_df):
    result = transform_orders(sample_orders_df)
    delivered = result[result["order_id"] == "ord_001"]
    assert delivered["is_late"].iloc[0] is False


def test_transform_orders_drops_rows_without_required_fields():
    df = pd.DataFrame({"order_id": [None], "customer_id": ["c1"], "order_status": ["delivered"]})
    result = transform_orders(df)
    assert len(result) == 0


def test_transform_customers_deduplicates(sample_customers_df):
    result = transform_customers(sample_customers_df)
    assert len(result) == 2


def test_transform_customers_normalizes_state(sample_customers_df):
    result = transform_customers(sample_customers_df)
    assert all(result["customer_state"].str.isupper())


def test_transform_customers_title_cases_city(sample_customers_df):
    result = transform_customers(sample_customers_df)
    assert "São Paulo" in result["customer_city"].values


def test_transform_products_merges_translation(sample_products_df, sample_category_df):
    result = transform_products(sample_products_df, sample_category_df)
    assert "product_category_name_english" in result.columns
    assert "bed_bath_table" in result["product_category_name_english"].values


def test_transform_products_fills_unknown_category(sample_products_df):
    empty_translation = pd.DataFrame(
        columns=["product_category_name", "product_category_name_english"]
    )
    result = transform_products(sample_products_df, empty_translation)
    assert (result["product_category_name_english"] == "unknown").all()
