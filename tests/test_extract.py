import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

from src.extract.olist_extractor import OlistExtractor


def test_extract_raises_for_unknown_dataset(tmp_path):
    extractor = OlistExtractor(str(tmp_path))
    with pytest.raises(ValueError, match="Unknown dataset"):
        extractor.extract("nonexistent_table")


def test_extract_raises_when_file_missing(tmp_path):
    extractor = OlistExtractor(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        extractor.extract("orders")


def test_extract_returns_dataframe(tmp_path):
    csv_content = "order_id,customer_id\nord_001,cust_001\n"
    csv_path = tmp_path / "olist_orders_dataset.csv"
    csv_path.write_text(csv_content)

    extractor = OlistExtractor(str(tmp_path))
    df = extractor.extract("orders")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "order_id" in df.columns
