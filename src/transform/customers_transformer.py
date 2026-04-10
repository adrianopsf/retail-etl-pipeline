"""Transformation logic for the customers domain."""

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

_VALUE_THRESHOLDS = {"high": 500.0, "medium": 200.0}


def _classify_customer(total_spent: float) -> str:
    if total_spent >= _VALUE_THRESHOLDS["high"]:
        return "high_value"
    if total_spent >= _VALUE_THRESHOLDS["medium"]:
        return "medium_value"
    return "low_value"


class CustomersTransformer:
    """Builds the enriched customers dimension table.

    Joins the customers file with the transformed orders data to derive
    per-customer behavioural features.
    """

    def transform(
        self,
        customers_df: pd.DataFrame,
        orders_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Enrich customers with order-level aggregates.

        Args:
            customers_df: Raw customers DataFrame.
            orders_df: Transformed orders DataFrame produced by
                :class:`~src.transform.orders_transformer.OrdersTransformer`.
                Must contain ``customer_id``, ``order_purchase_timestamp``,
                and ``total_order_value``.

        Returns:
            Deduplicated customer DataFrame keyed on ``customer_unique_id``
            with total orders, total spent, value segment, and date features.
        """
        logger.info("Starting customers transformation")

        customers = self._clean_base(customers_df.copy())
        order_agg = self._aggregate_orders(orders_df)

        result = customers.merge(order_agg, on="customer_id", how="left")
        result = self._fill_nulls(result)
        result = self._add_value_segment(result)
        result = self._deduplicate(result)

        logger.info(
            f"Customers transformation complete: {len(result):,} unique customers"
        )
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_base(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["customer_id", "customer_unique_id", "customer_state"])
        df["customer_state"] = df["customer_state"].str.upper().str.strip()
        df["customer_city"] = df["customer_city"].str.title().str.strip()
        return df

    @staticmethod
    def _aggregate_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
        """Compute per-customer order metrics from the orders fact table."""
        ts_col = "order_purchase_timestamp"
        if not pd.api.types.is_datetime64_any_dtype(orders_df[ts_col]):
            orders_df = orders_df.copy()
            orders_df[ts_col] = pd.to_datetime(orders_df[ts_col], errors="coerce")

        return (
            orders_df
            .groupby("customer_id", as_index=False)
            .agg(
                total_orders=("order_id", "count"),
                total_spent=("total_order_value", "sum"),
                first_order_date=(ts_col, "min"),
                last_order_date=(ts_col, "max"),
            )
        )

    @staticmethod
    def _fill_nulls(df: pd.DataFrame) -> pd.DataFrame:
        df["total_orders"] = df["total_orders"].fillna(0).astype(int)
        df["total_spent"] = df["total_spent"].fillna(0.0).infer_objects(copy=False)
        return df

    @staticmethod
    def _add_value_segment(df: pd.DataFrame) -> pd.DataFrame:
        df["customer_segment"] = df["total_spent"].apply(_classify_customer)
        return df

    @staticmethod
    def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
        """Keep the customer_id with the highest spend for each unique customer."""
        before = len(df)
        df = (
            df
            .sort_values("total_spent", ascending=False)
            .drop_duplicates(subset=["customer_unique_id"])
            .reset_index(drop=True)
        )
        logger.info(f"  Deduplicated: {before:,} → {len(df):,} unique customers")
        return df
