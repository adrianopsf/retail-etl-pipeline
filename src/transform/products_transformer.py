import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform_products(
    products_df: pd.DataFrame,
    category_translation_df: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Transforming products")
    df = products_df.copy()

    df = df.merge(
        category_translation_df,
        on="product_category_name",
        how="left",
    )

    df = df.drop_duplicates(subset=["product_id"])
    df = df.dropna(subset=["product_id"])

    df["product_category_name_english"] = df[
        "product_category_name_english"
    ].fillna("unknown")

    logger.info(f"Products transformed: {len(df):,} rows")
    return df
