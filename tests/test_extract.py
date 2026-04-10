"""Tests for OlistExtractor."""

from unittest.mock import patch

import pandas as pd
import pytest

from src.extract.olist_extractor import OlistExtractor

# Required columns per dataset (non-exhaustive — validates key business fields)
_REQUIRED_COLUMNS = {
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "customers": ["customer_id", "customer_unique_id", "customer_state"],
    "products": ["product_id", "product_category_name"],
    "order_items": ["order_id", "order_item_id", "product_id", "price"],
    "order_payments": ["order_id", "payment_value"],
    "order_reviews": ["review_id", "order_id", "review_score"],
    "sellers": ["seller_id", "seller_state"],
}


# ------------------------------------------------------------------
# Constructor
# ------------------------------------------------------------------


def test_constructor_raises_for_missing_directory():
    with pytest.raises(NotADirectoryError, match="Data directory not found"):
        OlistExtractor("/nonexistent/path/to/data")


# ------------------------------------------------------------------
# extract_single — success path
# ------------------------------------------------------------------


def test_extract_single_file_success(tmp_path):
    """Happy path: CSV on disk → DataFrame returned with correct shape."""
    csv_content = (
        "order_id,customer_id,order_status,order_purchase_timestamp\n"
        "ord_001,cust_001,delivered,2021-01-01 10:00:00\n"
        "ord_002,cust_002,shipped,2021-01-02 11:00:00\n"
    )
    (tmp_path / "olist_orders_dataset.csv").write_text(csv_content)

    extractor = OlistExtractor(str(tmp_path))
    df = extractor.extract_single("orders")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "order_id" in df.columns


def test_extract_single_uses_read_csv(tmp_path):
    """Verify that pandas.read_csv is called with the correct file path."""
    (tmp_path / "olist_customers_dataset.csv").write_text("customer_id\nc1\n")

    extractor = OlistExtractor(str(tmp_path))
    with patch("pandas.read_csv", return_value=pd.DataFrame({"customer_id": ["c1"]})) as mock_csv:
        extractor.extract_single("customers")
        assert mock_csv.call_count == 1
        call_path = str(mock_csv.call_args[0][0])
        assert "olist_customers_dataset.csv" in call_path


def test_extract_single_parses_order_date_columns(tmp_path):
    """Date columns declared in _DATASETS must be parsed as datetime64."""
    csv_content = (
        "order_id,customer_id,order_status,order_purchase_timestamp,"
        "order_approved_at,order_delivered_carrier_date,"
        "order_delivered_customer_date,order_estimated_delivery_date\n"
        "ord_001,cust_001,delivered,2021-01-01 10:00:00,"
        "2021-01-01 11:00:00,2021-01-03 10:00:00,"
        "2021-01-07 14:00:00,2021-01-10 00:00:00\n"
    )
    (tmp_path / "olist_orders_dataset.csv").write_text(csv_content)

    extractor = OlistExtractor(str(tmp_path))
    df = extractor.extract_single("orders")

    assert pd.api.types.is_datetime64_any_dtype(df["order_purchase_timestamp"])


# ------------------------------------------------------------------
# extract_single — error paths
# ------------------------------------------------------------------


def test_extract_missing_file_raises(tmp_path):
    """FileNotFoundError with an informative message when the CSV is absent."""
    extractor = OlistExtractor(str(tmp_path))
    with pytest.raises(FileNotFoundError, match="olist_orders_dataset.csv"):
        extractor.extract_single("orders")


def test_extract_unknown_dataset_raises(tmp_path):
    extractor = OlistExtractor(str(tmp_path))
    with pytest.raises(ValueError, match="Unknown dataset"):
        extractor.extract_single("sales_summary")


# ------------------------------------------------------------------
# extract_all
# ------------------------------------------------------------------


def test_extract_all_returns_all_datasets(tmp_path):
    """extract_all must return a dict with exactly 9 keys."""
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

    result = OlistExtractor(str(tmp_path)).extract_all()

    assert len(result) == 9
    assert all(isinstance(df, pd.DataFrame) for df in result.values())


# ------------------------------------------------------------------
# Column validation
# ------------------------------------------------------------------


@pytest.mark.parametrize("dataset,required_cols", _REQUIRED_COLUMNS.items())
def test_extract_validates_columns(tmp_path, dataset, required_cols):
    """Key columns must be present in each extracted dataset."""
    header = ",".join(required_cols)
    row = ",".join(["val"] * len(required_cols))
    fname_map = {
        "orders":         "olist_orders_dataset.csv",
        "customers":      "olist_customers_dataset.csv",
        "products":       "olist_products_dataset.csv",
        "order_items":    "olist_order_items_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews":  "olist_order_reviews_dataset.csv",
        "sellers":        "olist_sellers_dataset.csv",
    }
    (tmp_path / fname_map[dataset]).write_text(f"{header}\n{row}\n")

    df = OlistExtractor(str(tmp_path)).extract_single(dataset)
    for col in required_cols:
        assert col in df.columns, f"Column '{col}' missing from dataset '{dataset}'"
