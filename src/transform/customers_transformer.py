import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming customers")
    df = df.copy()

    df = df.drop_duplicates(subset=["customer_id"])
    df = df.dropna(subset=["customer_id", "customer_unique_id", "customer_state"])

    df["customer_state"] = df["customer_state"].str.upper().str.strip()
    df["customer_city"] = df["customer_city"].str.title().str.strip()

    logger.info(f"Customers transformed: {len(df):,} rows")
    return df
