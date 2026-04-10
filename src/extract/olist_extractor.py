"""Extractor for the Olist Brazilian E-Commerce CSV dataset."""

from pathlib import Path

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Maps logical dataset name → CSV filename and columns to parse as dates.
_DATASETS: dict[str, dict] = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "parse_dates": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "parse_dates": ["shipping_limit_date"],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "parse_dates": [],
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "parse_dates": ["review_creation_date", "review_answer_timestamp"],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "parse_dates": [],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "parse_dates": [],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "parse_dates": [],
    },
    "product_category_name": {
        "file": "product_category_name_translation.csv",
        "parse_dates": [],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "parse_dates": [],
    },
}


class OlistExtractor:
    """Reads Olist CSV files from a local directory into pandas DataFrames.

    Args:
        data_path: Directory that contains the raw CSV files (e.g. ``./data/raw``).

    Raises:
        NotADirectoryError: If *data_path* does not exist or is not a directory.
    """

    def __init__(self, data_path: str) -> None:
        self._root = Path(data_path)
        if not self._root.is_dir():
            raise NotADirectoryError(
                f"Data directory not found: {self._root.resolve()}\n"
                "Download the Olist dataset from Kaggle and place the CSV files there:\n"
                "  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_all(self) -> dict[str, pd.DataFrame]:
        """Extract every known Olist dataset.

        Returns:
            Mapping of dataset name → raw DataFrame with no transformations.
            Keys are the same as those in ``_DATASETS``.
        """
        logger.info(
            f"Extracting all {len(_DATASETS)} Olist datasets from {self._root.resolve()}"
        )
        results: dict[str, pd.DataFrame] = {}
        for name in _DATASETS:
            results[name] = self.extract_single(name)
        logger.info("Extraction complete — all datasets loaded")
        return results

    def extract_single(self, name: str) -> pd.DataFrame:
        """Extract one dataset by its logical name.

        Args:
            name: One of the keys in ``_DATASETS``
                (``"orders"``, ``"customers"``, ``"products"``, …).

        Returns:
            Raw DataFrame with date columns already parsed.

        Raises:
            ValueError: If *name* is not a recognised dataset key.
            FileNotFoundError: If the corresponding CSV file is missing.
        """
        if name not in _DATASETS:
            raise ValueError(
                f"Unknown dataset '{name}'. "
                f"Valid options: {sorted(_DATASETS)}"
            )

        spec = _DATASETS[name]
        path = self._root / spec["file"]

        if not path.exists():
            raise FileNotFoundError(
                f"Missing file: {path.resolve()}\n"
                f"Expected dataset '{name}' at '{spec['file']}'.\n"
                "Download the Olist dataset from:\n"
                "  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
            )

        logger.info(f"Reading '{name}' ← {spec['file']}")

        parse_dates = spec["parse_dates"] if spec["parse_dates"] else False
        df = pd.read_csv(path, parse_dates=parse_dates, low_memory=False)

        logger.info(f"  '{name}': {len(df):,} rows × {len(df.columns)} columns")
        return df
