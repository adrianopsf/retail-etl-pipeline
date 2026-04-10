"""Transformation logic for the orders domain."""

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

_DATETIME_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


class OrdersTransformer:
    """Builds the enriched orders fact table.

    Joins orders with order_items and order_payments to produce a single,
    analysis-ready DataFrame filtered to delivered orders only.
    """

    def transform(
        self,
        orders_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
        order_payments_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Combine and enrich orders with items and payment aggregates.

        Args:
            orders_df: Raw orders DataFrame (one row per order).
            order_items_df: Raw order_items DataFrame (multiple rows per order).
            order_payments_df: Raw order_payments DataFrame (multiple rows per order).

        Returns:
            Cleaned, enriched DataFrame with one row per delivered order,
            containing computed fields for total value, items, delivery days,
            and delivery-on-time flag.
        """
        logger.info("Starting orders transformation")

        orders = self._parse_datetimes(orders_df.copy())
        orders = self._filter_delivered(orders)
        orders = self._add_order_month(orders)
        orders = self._add_delivery_metrics(orders)

        items_agg = self._aggregate_items(order_items_df)
        payments_agg = self._aggregate_payments(order_payments_df)

        result = (
            orders
            .merge(items_agg, on="order_id", how="left")
            .merge(payments_agg, on="order_id", how="left")
        )

        result = self._fill_numeric_nulls(result)
        result = result.drop_duplicates(subset=["order_id"])

        logger.info(f"Orders transformation complete: {len(result):,} delivered orders")
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_datetimes(df: pd.DataFrame) -> pd.DataFrame:
        for col in _DATETIME_COLS:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    @staticmethod
    def _filter_delivered(df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df[df["order_status"] == "delivered"].copy()
        logger.info(f"  Filtered to delivered orders: {len(df):,} / {before:,}")
        return df

    @staticmethod
    def _add_order_month(df: pd.DataFrame) -> pd.DataFrame:
        df["order_month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)
        return df

    @staticmethod
    def _add_delivery_metrics(df: pd.DataFrame) -> pd.DataFrame:
        delivered = df["order_delivered_customer_date"]
        purchased = df["order_purchase_timestamp"]
        estimated = df["order_estimated_delivery_date"]

        df["delivery_days"] = (delivered - purchased).dt.days
        # -1 sentinel for orders without a confirmed delivery timestamp
        df["delivery_days"] = df["delivery_days"].fillna(-1).astype(int)

        df["is_late_delivery"] = delivered > estimated
        df["is_late_delivery"] = df["is_late_delivery"].fillna(False)

        return df

    @staticmethod
    def _aggregate_items(order_items_df: pd.DataFrame) -> pd.DataFrame:
        return (
            order_items_df
            .groupby("order_id", as_index=False)
            .agg(total_items=("order_item_id", "count"))
        )

    @staticmethod
    def _aggregate_payments(order_payments_df: pd.DataFrame) -> pd.DataFrame:
        return (
            order_payments_df
            .groupby("order_id", as_index=False)
            .agg(total_order_value=("payment_value", "sum"))
        )

    @staticmethod
    def _fill_numeric_nulls(df: pd.DataFrame) -> pd.DataFrame:
        df["total_items"] = df["total_items"].fillna(0).astype(int)
        df["total_order_value"] = df["total_order_value"].fillna(0.0)
        return df
