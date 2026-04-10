import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call

from src.load.postgres_loader import PostgresLoader


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def loader(mock_engine):
    return PostgresLoader(mock_engine)


@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})


def test_load_calls_to_sql(loader, sample_df):
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        loader.load(sample_df, table="orders")
        mock_to_sql.assert_called_once_with(
            name="orders",
            con=loader.engine,
            schema="staging",
            if_exists="replace",
            index=False,
            chunksize=5_000,
            method="multi",
        )


def test_load_accepts_custom_schema(loader, sample_df):
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        loader.load(sample_df, table="orders", schema="analytics")
        _, kwargs = mock_to_sql.call_args
        assert kwargs["schema"] == "analytics"


def test_execute_runs_sql(loader):
    loader.execute("SELECT 1")
    loader.engine.begin.assert_called_once()
