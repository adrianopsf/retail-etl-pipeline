import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

DATETIME_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming orders")
    df = df.copy()

    for col in DATETIME_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["delivery_days"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    df["is_late"] = (
        df["order_delivered_customer_date"] > df["order_estimated_delivery_date"]
    )

    df = df.dropna(subset=["order_id", "customer_id", "order_status"])

    logger.info(f"Orders transformed: {len(df):,} rows")
    return df
