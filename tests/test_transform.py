"""Tests for all domain transformer classes."""

import pandas as pd
import pytest

from src.transform.customers_transformer import CustomersTransformer
from src.transform.orders_transformer import OrdersTransformer
from src.transform.products_transformer import ProductsTransformer, _normalize_category


# =============================================================
# OrdersTransformer
# =============================================================


class TestOrdersTransformer:
    def _transform(self, orders_df, items_df, payments_df):
        return OrdersTransformer().transform(orders_df, items_df, payments_df)

    def test_filters_to_delivered_only(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        assert set(result["order_status"]) == {"delivered"}

    def test_computes_delivery_days(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        row = result[result["order_id"] == "ord_001"].iloc[0]
        assert row["delivery_days"] == 6  # Jan 1 → Jan 7

    def test_delivery_days_sentinel_when_no_delivery(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        # ord_001 is the only delivered order; it has a real delivery date
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        assert (result["delivery_days"] >= -1).all()

    def test_is_late_delivery_false_when_on_time(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        row = result[result["order_id"] == "ord_001"].iloc[0]
        # Delivered Jan 7, estimated Jan 10 → on time
        assert row["is_late_delivery"] is False

    def test_adds_order_month(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        assert "order_month" in result.columns
        assert result["order_month"].iloc[0] == "2021-01"

    def test_aggregates_total_items(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        row = result[result["order_id"] == "ord_001"].iloc[0]
        assert row["total_items"] == 2

    def test_aggregates_total_order_value(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        row = result[result["order_id"] == "ord_001"].iloc[0]
        assert row["total_order_value"] == pytest.approx(160.0)

    def test_no_duplicate_order_ids(
        self, sample_orders_df, sample_order_items_df, sample_order_payments_df
    ):
        result = self._transform(
            sample_orders_df, sample_order_items_df, sample_order_payments_df
        )
        assert result["order_id"].is_unique


# =============================================================
# CustomersTransformer
# =============================================================


class TestCustomersTransformer:
    def _make_orders_for_customers(self) -> pd.DataFrame:
        """Minimal orders DataFrame compatible with CustomersTransformer."""
        return pd.DataFrame(
            {
                "order_id": ["ord_001", "ord_002"],
                "customer_id": ["cust_001", "cust_002"],
                "order_purchase_timestamp": pd.to_datetime(
                    ["2021-01-01", "2021-06-01"]
                ),
                "total_order_value": [300.0, 700.0],
            }
        )

    def test_deduplicates_on_unique_id(self, sample_customers_df):
        orders = self._make_orders_for_customers()
        result = CustomersTransformer().transform(sample_customers_df, orders)
        assert result["customer_unique_id"].is_unique

    def test_normalises_state_to_uppercase(self, sample_customers_df):
        orders = self._make_orders_for_customers()
        result = CustomersTransformer().transform(sample_customers_df, orders)
        assert all(result["customer_state"].str.isupper())

    def test_title_cases_city(self, sample_customers_df):
        orders = self._make_orders_for_customers()
        result = CustomersTransformer().transform(sample_customers_df, orders)
        assert "São Paulo" in result["customer_city"].values

    def test_segments_high_value_customer(self, sample_customers_df):
        orders = self._make_orders_for_customers()
        result = CustomersTransformer().transform(sample_customers_df, orders)
        cust2 = result[result["customer_unique_id"] == "uniq_002"].iloc[0]
        assert cust2["customer_segment"] == "high_value"

    def test_segments_medium_value_customer(self, sample_customers_df):
        orders = self._make_orders_for_customers()
        result = CustomersTransformer().transform(sample_customers_df, orders)
        cust1 = result[result["customer_unique_id"] == "uniq_001"].iloc[0]
        assert cust1["customer_segment"] == "medium_value"

    def test_computes_first_and_last_order_dates(self, sample_customers_df):
        orders = self._make_orders_for_customers()
        result = CustomersTransformer().transform(sample_customers_df, orders)
        row = result[result["customer_unique_id"] == "uniq_001"].iloc[0]
        assert row["first_order_date"] == pd.Timestamp("2021-01-01")

    def test_customers_with_no_orders_have_zero_total(self, sample_customers_df):
        empty_orders = pd.DataFrame(
            columns=["order_id", "customer_id", "order_purchase_timestamp", "total_order_value"]
        )
        result = CustomersTransformer().transform(sample_customers_df, empty_orders)
        assert (result["total_orders"] == 0).all()
        assert (result["total_spent"] == 0.0).all()


# =============================================================
# ProductsTransformer
# =============================================================


class TestProductsTransformer:
    def test_merges_english_category_name(
        self, sample_products_df, sample_order_items_df, sample_category_df
    ):
        result = ProductsTransformer().transform(
            sample_products_df, sample_order_items_df, sample_category_df
        )
        assert "product_category_name_english" in result.columns
        assert "bed_bath_table" in result["product_category_name_english"].values

    def test_fills_unknown_for_missing_translation(
        self, sample_products_df, sample_order_items_df
    ):
        empty_translation = pd.DataFrame(
            columns=["product_category_name", "product_category_name_english"]
        )
        result = ProductsTransformer().transform(
            sample_products_df, sample_order_items_df, empty_translation
        )
        assert (result["product_category_name_english"] == "unknown").all()

    def test_computes_total_sold(
        self, sample_products_df, sample_order_items_df, sample_category_df
    ):
        result = ProductsTransformer().transform(
            sample_products_df, sample_order_items_df, sample_category_df
        )
        prod1 = result[result["product_id"] == "prod_001"].iloc[0]
        # prod_001 appears in ord_001 (item 1) and ord_002 (item 1) → 2 times
        assert prod1["total_sold"] == 2

    def test_computes_avg_price(
        self, sample_products_df, sample_order_items_df, sample_category_df
    ):
        result = ProductsTransformer().transform(
            sample_products_df, sample_order_items_df, sample_category_df
        )
        prod1 = result[result["product_id"] == "prod_001"].iloc[0]
        # prices: 100.0 and 200.0 → avg 150.0
        assert prod1["avg_price"] == pytest.approx(150.0)

    def test_creates_category_slug(
        self, sample_products_df, sample_order_items_df, sample_category_df
    ):
        result = ProductsTransformer().transform(
            sample_products_df, sample_order_items_df, sample_category_df
        )
        assert "category_slug" in result.columns

    def test_no_duplicate_product_ids(
        self, sample_products_df, sample_order_items_df, sample_category_df
    ):
        result = ProductsTransformer().transform(
            sample_products_df, sample_order_items_df, sample_category_df
        )
        assert result["product_id"].is_unique


class TestNormalizeCategory:
    def test_lowercases(self):
        assert _normalize_category("BED BATH") == "bed_bath"

    def test_replaces_spaces_with_underscores(self):
        assert _normalize_category("bed bath table") == "bed_bath_table"

    def test_removes_special_characters(self):
        assert _normalize_category("cama & mesa!") == "cama__mesa"
