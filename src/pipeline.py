"""Main ETL pipeline orchestrator for the Olist dataset."""

import argparse
import os
import sys
import time
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from src.extract.olist_extractor import OlistExtractor
from src.load.postgres_loader import PostgresLoader
from src.transform.customers_transformer import CustomersTransformer
from src.transform.orders_transformer import OrdersTransformer
from src.transform.products_transformer import ProductsTransformer
from src.utils.db_connection import DatabaseConnection
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


class OlistETLPipeline:
    """End-to-end ETL pipeline for the Olist Brazilian E-Commerce dataset.

    Orchestrates extraction from CSV files, transformation into clean
    analytics-ready DataFrames, and loading into PostgreSQL.

    Args:
        data_path: Path to the directory containing raw CSV files.
            Defaults to the ``DATA_RAW_PATH`` environment variable,
            falling back to ``./data/raw``.
    """

    def __init__(self, data_path: str | None = None) -> None:
        self._data_path = data_path or os.environ.get("DATA_RAW_PATH", "./data/raw")
        self._extractor = OlistExtractor(self._data_path)
        self._orders_transformer = OrdersTransformer()
        self._customers_transformer = CustomersTransformer()
        self._products_transformer = ProductsTransformer()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """Execute the full extract → transform → load pipeline.

        Returns:
            Metrics dictionary with keys:
            - ``"status"``: ``"success"`` or ``"error"``
            - ``"duration_seconds"``: total wall-clock time
            - ``"rows_loaded"``: mapping of table name → row count
            - ``"started_at"`` / ``"finished_at"``: ISO-8601 timestamps
        """
        started_at = datetime.utcnow()
        logger.info("=" * 60)
        logger.info("Olist ETL Pipeline — starting full run")
        logger.info(f"Started at: {started_at.isoformat()} UTC")
        logger.info("=" * 60)

        t0 = time.perf_counter()

        try:
            raw_data = self.run_extract()
            transformed = self.run_transform(raw_data)

            with DatabaseConnection() as db:
                db.test_connection()
                loader = PostgresLoader(db.get_engine())
                rows_loaded = self.run_load(transformed, loader)

        except Exception as exc:
            duration = time.perf_counter() - t0
            logger.error(f"Pipeline failed after {duration:.1f}s: {exc}")
            raise

        finished_at = datetime.utcnow()
        duration = time.perf_counter() - t0

        metrics = {
            "status": "success",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round(duration, 2),
            "rows_loaded": rows_loaded,
        }

        logger.info("=" * 60)
        logger.info(f"Pipeline completed in {duration:.1f}s")
        for table, count in rows_loaded.items():
            logger.info(f"  {table}: {count:,} rows")
        logger.info("=" * 60)

        return metrics

    def run_extract(self) -> dict[str, pd.DataFrame]:
        """Extract all raw CSVs into DataFrames.

        Returns:
            Mapping of dataset name → raw DataFrame.
        """
        t0 = time.perf_counter()
        logger.info("--- EXTRACT ---")
        raw = self._extractor.extract_all()
        logger.info(f"Extract finished in {time.perf_counter() - t0:.1f}s")
        return raw

    def run_transform(self, raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Apply all domain transformations to the raw DataFrames.

        Args:
            raw_data: Output of :meth:`run_extract`.

        Returns:
            Mapping of table name → cleaned, enriched DataFrame.
        """
        t0 = time.perf_counter()
        logger.info("--- TRANSFORM ---")

        orders = self._orders_transformer.transform(
            raw_data["orders"],
            raw_data["order_items"],
            raw_data["order_payments"],
        )
        customers = self._customers_transformer.transform(
            raw_data["customers"],
            orders,
        )
        products = self._products_transformer.transform(
            raw_data["products"],
            raw_data["order_items"],
            raw_data["product_category_name"],
        )

        transformed = {
            "orders": orders,
            "customers": customers,
            "products": products,
            "order_items": raw_data["order_items"],
            "order_payments": raw_data["order_payments"],
            "order_reviews": raw_data["order_reviews"],
            "sellers": raw_data["sellers"],
        }

        logger.info(f"Transform finished in {time.perf_counter() - t0:.1f}s")
        return transformed

    def run_load(
        self,
        transformed_data: dict[str, pd.DataFrame],
        loader: PostgresLoader,
    ) -> dict[str, int]:
        """Load all transformed DataFrames into PostgreSQL staging schema.

        Args:
            transformed_data: Output of :meth:`run_transform`.
            loader: Initialised :class:`~src.load.postgres_loader.PostgresLoader`.

        Returns:
            Mapping of table name → number of rows inserted.
        """
        t0 = time.perf_counter()
        logger.info("--- LOAD ---")
        rows_loaded: dict[str, int] = {}

        for table, df in transformed_data.items():
            n = loader.load_to_staging(df, table_name=table)
            loader.verify_load(table, schema="staging", expected_rows=n)
            rows_loaded[table] = n

        logger.info(f"Load finished in {time.perf_counter() - t0:.1f}s")
        return rows_loaded


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Olist ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--step",
        choices=["all", "extract", "transform", "load"],
        default="all",
        help="Pipeline step to execute (default: all)",
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Path to raw CSV directory (overrides DATA_RAW_PATH env var)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    pipeline = OlistETLPipeline(data_path=args.data_path)

    started = datetime.utcnow().isoformat()
    logger.info(f"CLI invoked — step={args.step}, started at {started} UTC")

    try:
        if args.step == "all":
            pipeline.run()
        elif args.step == "extract":
            pipeline.run_extract()
        elif args.step == "transform":
            raw = pipeline.run_extract()
            pipeline.run_transform(raw)
        elif args.step == "load":
            # Load step requires prior transform; run both
            raw = pipeline.run_extract()
            transformed = pipeline.run_transform(raw)
            with DatabaseConnection() as db:
                loader = PostgresLoader(db.get_engine())
                pipeline.run_load(transformed, loader)

    except Exception as exc:
        logger.error(f"Fatal error: {exc}")
        sys.exit(1)

    sys.exit(0)
