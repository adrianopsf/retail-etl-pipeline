"""Tests for the OlistExtractor."""

import pandas as pd
import pytest

from src.extract.olist_extractor import OlistExtractor


def test_raises_for_missing_directory():
    with pytest.raises(NotADirectoryError):
        OlistExtractor("/nonexistent/path")


def test_extract_single_raises_for_unknown_dataset(tmp_path):
    extractor = OlistExtractor(str(tmp_path))
    with pytest.raises(ValueError, match="Unknown dataset"):
        extractor.extract_single("nonexistent_table")


def test_extract_single_raises_when_file_missing(tmp_path):
    extractor = OlistExtractor(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        extractor.extract_single("orders")


def test_extract_single_returns_dataframe(tmp_path):
    csv_content = "order_id,customer_id,order_status\nord_001,cust_001,delivered\n"
    (tmp_path / "olist_orders_dataset.csv").write_text(csv_content)

    extractor = OlistExtractor(str(tmp_path))
    df = extractor.extract_single("orders")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "order_id" in df.columns


def test_extract_single_parses_date_columns(tmp_path):
    csv_content = (
        "order_id,customer_id,order_status,order_purchase_timestamp\n"
        "ord_001,cust_001,delivered,2021-01-01 10:00:00\n"
    )
    (tmp_path / "olist_orders_dataset.csv").write_text(csv_content)

    extractor = OlistExtractor(str(tmp_path))
    df = extractor.extract_single("orders")

    assert pd.api.types.is_datetime64_any_dtype(df["order_purchase_timestamp"])


def test_extract_all_returns_all_datasets(tmp_path):
    """extract_all should return a dict with 9 keys."""
    # Create dummy CSV files for every expected dataset
    files = [
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv",
        "olist_geolocation_dataset.csv",
    ]
    for fname in files:
        (tmp_path / fname).write_text("col_a,col_b\nval_1,val_2\n")

    extractor = OlistExtractor(str(tmp_path))
    result = extractor.extract_all()

    assert len(result) == 9
    assert all(isinstance(df, pd.DataFrame) for df in result.values())
