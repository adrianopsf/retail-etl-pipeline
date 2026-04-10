"""Tests for PostgresLoader.

Unit tests (no DB required): mock the engine and to_sql.
Integration tests (require DB): use the session-scoped ``engine`` fixture
from conftest and are automatically skipped when PostgreSQL is unavailable.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from src.load.postgres_loader import PostgresLoader

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    """Mock engine with a connect() context manager that returns scalar 2."""
    engine = MagicMock()
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
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})


@pytest.fixture
def large_df() -> pd.DataFrame:
    return pd.DataFrame({"id": range(1, 101), "value": ["x"] * 100})


# ------------------------------------------------------------------
# load_to_staging — unit tests
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


def test_load_to_staging_uses_replace_strategy(loader, sample_df):
    """if_exists must be 'replace' so idempotent re-runs don't append duplicates."""
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        loader.load_to_staging(sample_df, table_name="orders")
        _, kwargs = mock_to_sql.call_args
        assert kwargs["if_exists"] == "replace"


def test_load_to_analytics_uses_analytics_schema(loader, sample_df):
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        loader.load_to_analytics(sample_df, table_name="fact_orders")
        _, kwargs = mock_to_sql.call_args
        assert kwargs["schema"] == "analytics"


def test_load_to_analytics_returns_row_count(loader, large_df):
    with patch("pandas.DataFrame.to_sql"):
        count = loader.load_to_analytics(large_df, table_name="dim_customers")
    assert count == 100


# ------------------------------------------------------------------
# verify_load — unit tests
# ------------------------------------------------------------------


def test_verify_load_passes_when_counts_match(loader):
    # mock returns scalar 2; expected = 2 → passes
    assert loader.verify_load("orders", schema="staging", expected_rows=2) is True


def test_verify_load_fails_when_counts_diverge(loader):
    """Expected 100 rows but DB returns 2 → well beyond 1 % tolerance."""
    with pytest.raises(ValueError, match="Load verification failed"):
        loader.verify_load("orders", schema="staging", expected_rows=100)


def test_verify_load_tolerates_one_row_discrepancy(mock_engine):
    """99 actual vs 100 expected → diff=1, tolerance=ceil(100*0.01)=1 → passes."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.scalar_one.return_value = 99
    mock_engine.connect.return_value = mock_conn

    assert PostgresLoader(mock_engine).verify_load(
        "orders", schema="staging", expected_rows=100
    ) is True


def test_verify_load_fails_at_two_rows_discrepancy_on_100(mock_engine):
    """100 rows expected, 98 actual → diff=2 > tolerance=1 → raises."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.scalar_one.return_value = 98
    mock_engine.connect.return_value = mock_conn

    with pytest.raises(ValueError, match="Load verification failed"):
        PostgresLoader(mock_engine).verify_load(
            "orders", schema="staging", expected_rows=100
        )


# ------------------------------------------------------------------
# execute — unit test
# ------------------------------------------------------------------


def test_execute_uses_transaction(loader):
    loader.execute("CREATE SCHEMA IF NOT EXISTS staging")
    loader._engine.begin.assert_called_once()


# ------------------------------------------------------------------
# Integration tests — require live PostgreSQL (skipped without one)
# ------------------------------------------------------------------


@pytest.mark.integration
def test_load_to_staging_returns_row_count_integration(engine):
    """Load a small DataFrame and verify the returned count is correct."""
    df = pd.DataFrame({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    loader = PostgresLoader(engine)

    with patch.object(loader, "_count_rows", return_value=3):
        count = loader.load_to_staging(df, table_name="_test_load_unit")

    assert count == 3


@pytest.mark.integration
def test_load_creates_table_if_not_exists(engine):
    """After loading, the target table must exist in the staging schema."""
    df = pd.DataFrame({"order_id": ["t1", "t2"], "value": [1.0, 2.0]})
    loader = PostgresLoader(engine)

    # Ensure the table doesn't exist from a previous run
    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS staging."_test_integration_orders"'))

    loader.load_to_staging(df, table_name="_test_integration_orders")

    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="staging")
    assert "_test_integration_orders" in tables

    # Cleanup
    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS staging."_test_integration_orders"'))


@pytest.mark.integration
def test_verify_load_passes_when_counts_match_integration(engine):
    """End-to-end: load then verify against the actual DB row count."""
    df = pd.DataFrame({"id": range(1, 6), "v": ["x"] * 5})
    loader = PostgresLoader(engine)

    loader.load_to_staging(df, table_name="_test_verify")
    assert loader.verify_load("_test_verify", schema="staging", expected_rows=5)

    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS staging."_test_verify"'))


@pytest.mark.integration
def test_verify_load_fails_when_counts_diverge_integration(engine):
    """After loading 5 rows, verifying against 100 must raise ValueError."""
    df = pd.DataFrame({"id": range(1, 6), "v": ["x"] * 5})
    loader = PostgresLoader(engine)

    loader.load_to_staging(df, table_name="_test_verify_fail")

    with pytest.raises(ValueError, match="Load verification failed"):
        loader.verify_load("_test_verify_fail", schema="staging", expected_rows=100)

    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS staging."_test_verify_fail"'))
