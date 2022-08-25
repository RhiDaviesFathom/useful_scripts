import pytest
from unittest.mock import patch
from useful_scripts import add_column_to_csv


@patch("useful_scripts.add_column_to_csv.add_column_in_csv")
def test_main(mock_add_column_to_csv):
    add_column_to_csv.add_column_in_csv()
    assert mock_add_column_to_csv.called
