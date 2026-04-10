"""Transformation logic for the products domain."""

import re

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_category(name: str) -> str:
    """Return a lowercase, ASCII-safe category slug.

    Example:
        >>> _normalize_category("Cama Mesa & Banho!")
        'cama_mesa_banho'
    """
    name = name.lower().strip()
    # Replace accented/special chars with ASCII equivalents via regex
    name = re.sub(r"[^a-z0-9\s_]", "", name)
    name = re.sub(r"\s+", "_", name)
    return name


class ProductsTransformer:
    """Builds the enriched products dimension table.

    Joins products with their English category names and derives
    sales-based metrics from the order_items dataset.
    """

    def transform(
        self,
        products_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
        category_translation_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Enrich products with sales metrics and translated category names.

        Args:
            products_df: Raw products DataFrame.
            order_items_df: Raw order_items DataFrame used to compute
                ``total_sold`` and ``avg_price`` per product.
            category_translation_df: Translation table mapping
                ``product_category_name`` → ``product_category_name_english``.

        Returns:
            Deduplicated products DataFrame with English category names,
            normalised category slugs, and order-level sales aggregates.
        """
        logger.info("Starting products transformation")

        products = self._translate_categories(
            products_df.copy(), category_translation_df
        )
        products = self._normalize_categories(products)
        sales_agg = self._aggregate_sales(order_items_df)

        result = products.merge(sales_agg, on="product_id", how="left")
        result = self._fill_nulls(result)
        result = result.drop_duplicates(subset=["product_id"]).reset_index(drop=True)

        logger.info(
            f"Products transformation complete: {len(result):,} unique products"
        )
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _translate_categories(
        df: pd.DataFrame,
        translation_df: pd.DataFrame,
    ) -> pd.DataFrame:
        df["product_category_name"] = df["product_category_name"].fillna("unknown")
        df = df.merge(translation_df, on="product_category_name", how="left")
        df["product_category_name_english"] = df[
            "product_category_name_english"
        ].fillna("unknown")
        return df

    @staticmethod
    def _normalize_categories(df: pd.DataFrame) -> pd.DataFrame:
        df["category_slug"] = df["product_category_name_english"].apply(
            _normalize_category
        )
        return df

    @staticmethod
    def _aggregate_sales(order_items_df: pd.DataFrame) -> pd.DataFrame:
        """Compute per-product sales metrics from order_items."""
        return (
            order_items_df
            .groupby("product_id", as_index=False)
            .agg(
                total_sold=("order_id", "count"),
                avg_price=("price", "mean"),
            )
            .assign(avg_price=lambda d: d["avg_price"].round(2))
        )

    @staticmethod
    def _fill_nulls(df: pd.DataFrame) -> pd.DataFrame:
        df["total_sold"] = df["total_sold"].fillna(0).astype(int)
        df["avg_price"] = df["avg_price"].fillna(0.0)
        return df
