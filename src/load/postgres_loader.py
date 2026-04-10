import pandas as pd
from sqlalchemy import Engine, text
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PostgresLoader:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def load(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "staging",
        if_exists: str = "replace",
        chunksize: int = 5_000,
    ) -> None:
        logger.info(f"Loading {len(df):,} rows into {schema}.{table}")
        df.to_sql(
            name=table,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        logger.info(f"Loaded {schema}.{table} successfully")

    def execute(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(sql))
