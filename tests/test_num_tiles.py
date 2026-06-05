import pytest
from click.testing import CliRunner
from pathlib import Path
import pandas as pd

# Import elements from your script
from useful_scripts.num_tiles import (
    main,
    load_coordinates_to_dataframe,
    process_tiles,
)

# ==========================================
# FIXTURES (Sample Data Setups)
# ==========================================


@pytest.fixture
def valid_csv_content():
    # Concrete coordinates to let fathom_pyutils calculate actual names
    return "Latitude,Longitude\n23.03,72.58\n12.9789,77.5917\n23.03,72.58\n"


@pytest.fixture
def invalid_csv_content():
    return "WrongLat,WrongLon\n23.03,72.58\n"


@pytest.fixture
def empty_csv_content():
    return ""


# ==========================================
# UNIT TESTS
# ==========================================


def test_load_coordinates_to_dataframe_success(tmp_path, valid_csv_content):
    """Test that a valid CSV is correctly loaded into a DataFrame."""
    csv_file = tmp_path / "valid.csv"
    csv_file.write_text(valid_csv_content)

    df = load_coordinates_to_dataframe(csv_file)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["Latitude", "Longitude"]


def test_load_coordinates_to_dataframe_file_not_found():
    """Test that a missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_coordinates_to_dataframe(Path("non_existent_file.csv"))


def test_load_coordinates_to_dataframe_missing_columns(tmp_path, invalid_csv_content):
    """Test that a CSV missing correct headers throws a ValueError."""
    csv_file = tmp_path / "invalid.csv"
    csv_file.write_text(invalid_csv_content)

    with pytest.raises(
        ValueError, match="CSV must contain 'Latitude' and 'Longitude' columns"
    ):
        load_coordinates_to_dataframe(csv_file)


def test_load_coordinates_to_dataframe_empty_file(tmp_path, empty_csv_content):
    """Test that an empty CSV file triggers pandas EmptyDataError."""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text(empty_csv_content)

    with pytest.raises(pd.errors.EmptyDataError):
        load_coordinates_to_dataframe(csv_file)


def test_process_tiles_deduplication():
    """Test that process_tiles correctly counts unique tiles using live Tiles package."""
    # Dataframe containing 3 rows but only 2 unique locations
    df = pd.DataFrame(
        {"Latitude": [23.03, 12.9789, 23.03], "Longitude": [72.58, 77.5917, 72.58]}
    )

    unique_count = process_tiles(df)

    # Verification based on real library execution outputs
    assert unique_count == 2


# ==========================================
# END-TO-END CLI TESTS (using Click Runner)
# ==========================================


def test_cli_success(tmp_path, valid_csv_content):
    """Test successful CLI execution path with live dependency execution."""
    csv_file = tmp_path / "test_coords.csv"
    csv_file.write_text(valid_csv_content)

    runner = CliRunner()
    result = runner.invoke(main, ["--filepath", str(csv_file)])

    assert result.exit_code == 0
    assert "Result: Total number of tiles across the CSV is 2" in result.output


def test_cli_missing_file():
    """Test CLI behavior when pointing to a missing filepath."""
    runner = CliRunner()
    result = runner.invoke(main, ["--filepath", "missing_file.csv"])

    # Click's native validation flags missing parameters
    assert result.exit_code != 0
