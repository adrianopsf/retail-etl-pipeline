import os
from dotenv import load_dotenv

from src.extract.olist_extractor import OlistExtractor
from src.transform.orders_transformer import transform_orders
from src.transform.customers_transformer import transform_customers
from src.transform.products_transformer import transform_products
from src.load.postgres_loader import PostgresLoader
from src.utils.db_connection import get_engine
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def run() -> None:
    raw_path = os.environ.get("DATA_RAW_PATH", "./data/raw")

    logger.info("=== Olist ETL Pipeline started ===")

    # Extract
    extractor = OlistExtractor(raw_path)
    raw = extractor.extract_all()

    # Transform
    orders = transform_orders(raw["orders"])
    customers = transform_customers(raw["customers"])
    products = transform_products(raw["products"], raw["product_category_name"])

    # Load
    engine = get_engine()
    loader = PostgresLoader(engine)

    loader.load(orders, table="orders")
    loader.load(customers, table="customers")
    loader.load(products, table="products")
    loader.load(raw["order_items"], table="order_items")
    loader.load(raw["order_payments"], table="order_payments")
    loader.load(raw["order_reviews"], table="order_reviews")
    loader.load(raw["sellers"], table="sellers")

    logger.info("=== Pipeline completed successfully ===")


if __name__ == "__main__":
    run()
