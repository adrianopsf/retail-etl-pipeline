"""Tests for PostgresLoader."""

import math
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.load.postgres_loader import PostgresLoader


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    # Simulate a scalar COUNT result for verify_load
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.scalar_one.return_value = 2
    engine.connect.return_value = mock_conn
    return engine


@pytest.fixture
def loader(mock_engine):
    return PostgresLoader(mock_engine)


@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})


# ------------------------------------------------------------------
# load_to_staging
# ------------------------------------------------------------------


def test_load_to_staging_calls_to_sql(loader, sample_df):
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        loader.load_to_staging(sample_df, table_name="orders")
        mock_to_sql.assert_called_once()
        _, kwargs = mock_to_sql.call_args
        assert kwargs["name"] == "orders"
        assert kwargs["schema"] == "staging"
        assert kwargs["if_exists"] == "replace"
        assert kwargs["index"] is False


def test_load_to_staging_returns_row_count(loader, sample_df):
    with patch("pandas.DataFrame.to_sql"):
        count = loader.load_to_staging(sample_df, table_name="orders")
    assert count == len(sample_df)


def test_load_to_analytics_uses_analytics_schema(loader, sample_df):
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        loader.load_to_analytics(sample_df, table_name="fact_orders")
        _, kwargs = mock_to_sql.call_args
        assert kwargs["schema"] == "analytics"


# ------------------------------------------------------------------
# verify_load
# ------------------------------------------------------------------


def test_verify_load_passes_when_counts_match(loader):
    # mock_engine.connect().execute().scalar_one() returns 2
    assert loader.verify_load("orders", schema="staging", expected_rows=2) is True


def test_verify_load_raises_when_discrepancy_exceeds_tolerance(loader):
    # Expected 100 rows but mock returns 2 → well over 1 % tolerance
    with pytest.raises(ValueError, match="Load verification failed"):
        loader.verify_load("orders", schema="staging", expected_rows=100)


def test_verify_load_tolerates_small_discrepancy(mock_engine, sample_df):
    # Return 99 rows when we expected 100 → 1 row diff, tolerance = ceil(100*0.01) = 1
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.scalar_one.return_value = 99
    mock_engine.connect.return_value = mock_conn

    loader = PostgresLoader(mock_engine)
    assert loader.verify_load("orders", schema="staging", expected_rows=100) is True


# ------------------------------------------------------------------
# execute
# ------------------------------------------------------------------


def test_execute_uses_transaction(loader):
    loader.execute("CREATE SCHEMA IF NOT EXISTS staging")
    loader._engine.begin.assert_called_once()
