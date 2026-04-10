"""Tests for all domain transformer classes.

Naming mirrors the spec so CI output is easy to read:
  test_orders_transform_*   → OrdersTransformer
  test_customers_*          → CustomersTransformer
  test_products_*           → ProductsTransformer
"""

import pandas as pd
import pytest

from src.transform.customers_transformer import CustomersTransformer
from src.transform.orders_transformer import OrdersTransformer
from src.transform.products_transformer import ProductsTransformer, _normalize_category

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _transform_orders(orders, items, payments):
    return OrdersTransformer().transform(orders, items, payments)


def _make_orders_for_customers(
    customer_ids: list[str] | None = None,
    values: list[float] | None = None,
) -> pd.DataFrame:
    """Return a minimal orders DataFrame for CustomersTransformer."""
    cids = customer_ids or ["cust_001", "cust_002"]
    vals = values or [300.0, 700.0]
    return pd.DataFrame(
        {
            "order_id": [f"ord_{i:03d}" for i in range(1, len(cids) + 1)],
            "customer_id": cids,
            "order_purchase_timestamp": pd.to_datetime(
                ["2021-01-01"] * len(cids)
            ),
            "total_order_value": vals,
        }
    )


# =============================================================
# OrdersTransformer
# =============================================================


def test_orders_transform_filters_delivered_only(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    assert set(result["order_status"]) == {"delivered"}
    # 7 delivered in fixture
    assert len(result) == 7


def test_orders_transform_calculates_delivery_days(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    """delivery_days = customer_delivery - purchase, in whole days."""
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    row = result[result["order_id"] == "ord_001"].iloc[0]
    # Jan 5 → Jan 12 = 7 days
    assert row["delivery_days"] == 7


def test_orders_transform_no_nulls_in_key_columns(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    """order_id and customer_id must be fully populated after transformation."""
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    assert result["order_id"].notna().all()
    assert result["customer_id"].notna().all()


def test_orders_transform_creates_order_month(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    """order_month must exist and match YYYY-MM format for every row."""
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    assert "order_month" in result.columns
    assert result["order_month"].str.match(r"^\d{4}-\d{2}$").all()
    assert result[result["order_id"] == "ord_001"]["order_month"].iloc[0] == "2021-01"


def test_orders_is_late_delivery(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    """Orders delivered after their estimated date must be flagged is_late_delivery=True."""
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    # ord_002: delivered Feb 16, estimated Feb 14 → LATE
    late = result[result["order_id"] == "ord_002"].iloc[0]
    assert bool(late["is_late_delivery"]) is True

    # ord_001: delivered Jan 12, estimated Jan 20 → on time
    on_time = result[result["order_id"] == "ord_001"].iloc[0]
    assert bool(on_time["is_late_delivery"]) is False


def test_orders_delivery_days_sentinel_for_missing_date(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    """delivery_days must be -1 when the customer delivery timestamp is null."""
    # Add a delivered order without a customer delivery date
    extra = pd.concat(
        [
            sample_orders_df,
            pd.DataFrame(
                {
                    "order_id": ["ord_no_delivery"],
                    "customer_id": ["cust_x"],
                    "order_status": ["delivered"],
                    "order_purchase_timestamp": pd.to_datetime(["2021-11-01"]),
                    "order_approved_at": [None],
                    "order_delivered_carrier_date": [None],
                    "order_delivered_customer_date": [None],
                    "order_estimated_delivery_date": pd.to_datetime(["2021-11-15"]),
                }
            ),
        ],
        ignore_index=True,
    )
    result = _transform_orders(
        extra, sample_order_items_df, sample_order_payments_df
    )
    row = result[result["order_id"] == "ord_no_delivery"].iloc[0]
    assert row["delivery_days"] == -1


def test_orders_no_duplicate_order_ids(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    assert result["order_id"].is_unique


def test_orders_aggregates_total_items(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    row = result[result["order_id"] == "ord_001"].iloc[0]
    assert row["total_items"] == 2  # two items in fixture for ord_001


def test_orders_aggregates_total_order_value(
    sample_orders_df, sample_order_items_df, sample_order_payments_df
):
    result = _transform_orders(
        sample_orders_df, sample_order_items_df, sample_order_payments_df
    )
    row = result[result["order_id"] == "ord_001"].iloc[0]
    assert row["total_order_value"] == pytest.approx(200.0)  # payment fixture value


# =============================================================
# CustomersTransformer
# =============================================================


def test_customers_no_duplicate_customer_ids(sample_customers_df):
    """customer_unique_id must be unique in the output."""
    orders = _make_orders_for_customers(
        ["cust_001", "cust_002", "cust_003", "cust_004",
         "cust_005", "cust_006", "cust_007", "cust_008"],
        [300.0, 700.0, 150.0, 600.0, 50.0, 220.0, 480.0, 100.0],
    )
    result = CustomersTransformer().transform(sample_customers_df, orders)
    assert result["customer_unique_id"].is_unique


def test_customers_creates_value_segments(sample_customers_df):
    """Every customer must have a non-null customer_segment."""
    orders = _make_orders_for_customers(
        ["cust_001", "cust_002", "cust_003", "cust_004",
         "cust_005", "cust_006", "cust_007", "cust_008"],
        [300.0, 700.0, 150.0, 600.0, 50.0, 220.0, 480.0, 100.0],
    )
    result = CustomersTransformer().transform(sample_customers_df, orders)
    assert result["customer_segment"].notna().all()
    assert set(result["customer_segment"]).issubset(
        {"high_value", "medium_value", "low_value"}
    )


def test_customers_high_value_segment_threshold(sample_customers_df):
    """Customers with total_spent ≥ 500 must be labelled 'high_value'."""
    orders = _make_orders_for_customers(["cust_002"], [700.0])
    result = CustomersTransformer().transform(sample_customers_df, orders)
    row = result[result["customer_unique_id"] == "uniq_002"].iloc[0]
    assert row["customer_segment"] == "high_value"


def test_customers_low_value_segment_threshold(sample_customers_df):
    """Customers with total_spent < 200 must be labelled 'low_value'."""
    orders = _make_orders_for_customers(["cust_005"], [50.0])
    result = CustomersTransformer().transform(sample_customers_df, orders)
    row = result[result["customer_unique_id"] == "uniq_005"].iloc[0]
    assert row["customer_segment"] == "low_value"


def test_customers_normalises_state(sample_customers_df):
    orders = _make_orders_for_customers()
    result = CustomersTransformer().transform(sample_customers_df, orders)
    assert all(result["customer_state"].str.isupper())


def test_customers_title_cases_city(sample_customers_df):
    orders = _make_orders_for_customers()
    result = CustomersTransformer().transform(sample_customers_df, orders)
    assert "São Paulo" in result["customer_city"].values


def test_customers_zero_totals_when_no_orders(sample_customers_df):
    """Customers with no matching orders should have total_orders=0 and total_spent=0."""
    empty_orders = pd.DataFrame(
        columns=["order_id", "customer_id", "order_purchase_timestamp", "total_order_value"]
    )
    result = CustomersTransformer().transform(sample_customers_df, empty_orders)
    assert (result["total_orders"] == 0).all()
    assert (result["total_spent"] == 0.0).all()
    assert (result["customer_segment"] == "low_value").all()


# =============================================================
# ProductsTransformer
# =============================================================


def test_products_translates_categories(
    sample_products_df, sample_order_items_df, sample_category_df
):
    result = ProductsTransformer().transform(
        sample_products_df, sample_order_items_df, sample_category_df
    )
    assert "product_category_name_english" in result.columns
    assert "bed_bath_table" in result["product_category_name_english"].values
    assert "computers_accessories" in result["product_category_name_english"].values


def test_products_handles_null_category(
    sample_products_df, sample_order_items_df
):
    """Products whose category has no English translation should get 'unknown'."""
    empty_translation = pd.DataFrame(
        columns=["product_category_name", "product_category_name_english"]
    )
    result = ProductsTransformer().transform(
        sample_products_df, sample_order_items_df, empty_translation
    )
    assert (result["product_category_name_english"] == "unknown").all()


def test_products_null_source_category_mapped_to_unknown(sample_order_items_df, sample_category_df):
    """A row where product_category_name is NaN should become 'unknown'."""
    products_with_null = pd.DataFrame(
        {
            "product_id": ["prod_null"],
            "product_category_name": [None],
            "product_weight_g": [100],
        }
    )
    result = ProductsTransformer().transform(
        products_with_null, sample_order_items_df, sample_category_df
    )
    assert result.iloc[0]["product_category_name_english"] == "unknown"


def test_products_no_duplicate_product_ids(
    sample_products_df, sample_order_items_df, sample_category_df
):
    result = ProductsTransformer().transform(
        sample_products_df, sample_order_items_df, sample_category_df
    )
    assert result["product_id"].is_unique


def test_products_computes_total_sold(
    sample_products_df, sample_order_items_df, sample_category_df
):
    result = ProductsTransformer().transform(
        sample_products_df, sample_order_items_df, sample_category_df
    )
    # prod_001 appears in ord_001 item 1, ord_003 item 1, ord_005 item 3 → 3 times
    p1 = result[result["product_id"] == "prod_001"].iloc[0]
    assert p1["total_sold"] == 3


def test_products_computes_avg_price(
    sample_products_df, sample_order_items_df, sample_category_df
):
    result = ProductsTransformer().transform(
        sample_products_df, sample_order_items_df, sample_category_df
    )
    p1 = result[result["product_id"] == "prod_001"].iloc[0]
    # prod_001 prices: 120.00, 95.00, 40.00 → avg ≈ 85.00
    assert p1["avg_price"] == pytest.approx((120.00 + 95.00 + 40.00) / 3, rel=1e-2)


def test_products_creates_category_slug(
    sample_products_df, sample_order_items_df, sample_category_df
):
    result = ProductsTransformer().transform(
        sample_products_df, sample_order_items_df, sample_category_df
    )
    assert "category_slug" in result.columns
    assert result["category_slug"].str.match(r"^[a-z0-9_]+$").all()


# =============================================================
# _normalize_category (pure function)
# =============================================================


class TestNormalizeCategory:
    def test_lowercases(self):
        assert _normalize_category("BED BATH") == "bed_bath"

    def test_spaces_become_underscores(self):
        assert _normalize_category("bed bath table") == "bed_bath_table"

    def test_removes_special_characters(self):
        result = _normalize_category("cama & mesa!")
        assert "&" not in result
        assert "!" not in result

    def test_handles_already_normalised_input(self):
        assert _normalize_category("sports_leisure") == "sports_leisure"

    def test_strips_leading_trailing_whitespace(self):
        assert _normalize_category("  electronics  ") == "electronics"
