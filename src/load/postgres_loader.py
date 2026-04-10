"""PostgreSQL loader with staging and analytics targets."""

import math

import pandas as pd
from sqlalchemy import Engine, text

from src.utils.logger import get_logger

logger = get_logger(__name__)

_VERIFY_TOLERANCE = 0.01  # 1 % row-count mismatch triggers an error


class PostgresLoader:
    """Loads transformed DataFrames into PostgreSQL schemas.

    Args:
        engine: An active SQLAlchemy :class:`~sqlalchemy.engine.Engine`.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_to_staging(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "staging",
    ) -> int:
        """Write *df* to a staging table, replacing any existing data.

        Args:
            df: DataFrame to load.
            table_name: Target table name (without schema prefix).
            schema: Target schema (default: ``"staging"``).

        Returns:
            Number of rows inserted.
        """
        return self._load(df, table_name, schema)

    def load_to_analytics(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "analytics",
    ) -> int:
        """Write *df* to an analytics table, replacing any existing data.

        Args:
            df: DataFrame to load.
            table_name: Target table name (without schema prefix).
            schema: Target schema (default: ``"analytics"``).

        Returns:
            Number of rows inserted.
        """
        return self._load(df, table_name, schema)

    def verify_load(
        self,
        table_name: str,
        schema: str,
        expected_rows: int,
    ) -> bool:
        """Count rows in the target table and compare to *expected_rows*.

        Args:
            table_name: Table name (without schema prefix).
            schema: Schema that contains the table.
            expected_rows: Row count that the load should have produced.

        Returns:
            ``True`` if the actual count is within 1 % of *expected_rows*.

        Raises:
            ValueError: If the discrepancy exceeds the 1 % tolerance.
        """
        actual = self._count_rows(table_name, schema)
        tolerance = math.ceil(expected_rows * _VERIFY_TOLERANCE)
        diff = abs(actual - expected_rows)

        if diff > tolerance:
            raise ValueError(
                f"Load verification failed for {schema}.{table_name}: "
                f"expected {expected_rows:,} rows, got {actual:,} "
                f"(diff {diff:,} > tolerance {tolerance:,})"
            )

        logger.info(
            f"Verified {schema}.{table_name}: {actual:,} rows "
            f"(expected {expected_rows:,})"
        )
        return True

    def execute(self, sql: str) -> None:
        """Execute arbitrary SQL inside a transaction.

        Args:
            sql: SQL statement to run.
        """
        with self._engine.begin() as conn:
            conn.execute(text(sql))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        chunksize: int = 1_000,
    ) -> int:
        n_rows = len(df)
        n_chunks = math.ceil(n_rows / chunksize)
        logger.info(
            f"Loading {n_rows:,} rows → {schema}.{table_name} "
            f"({n_chunks} chunks of {chunksize:,})"
        )

        df.to_sql(
            name=table_name,
            con=self._engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=chunksize,
            method="multi",
        )

        logger.info(f"Loaded {schema}.{table_name} — {n_rows:,} rows written")
        return n_rows

    def _count_rows(self, table_name: str, schema: str) -> int:
        with self._engine.connect() as conn:
            result = conn.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            )
            return result.scalar_one()
