from pathlib import Path
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

OLIST_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name": "product_category_name_translation.csv",
    "geolocation": "olist_geolocation_dataset.csv",
}


class OlistExtractor:
    def __init__(self, raw_path: str) -> None:
        self.raw_path = Path(raw_path)

    def extract(self, dataset: str) -> pd.DataFrame:
        if dataset not in OLIST_FILES:
            raise ValueError(f"Unknown dataset: '{dataset}'. Valid options: {list(OLIST_FILES)}")

        file_path = self.raw_path / OLIST_FILES[dataset]

        if not file_path.exists():
            raise FileNotFoundError(
                f"File not found: {file_path}\n"
                "Download the dataset from Kaggle: "
                "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
            )

        logger.info(f"Extracting '{dataset}' from {file_path.name}")
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"Extracted {len(df):,} rows from '{dataset}'")
        return df

    def extract_all(self) -> dict[str, pd.DataFrame]:
        return {name: self.extract(name) for name in OLIST_FILES}
