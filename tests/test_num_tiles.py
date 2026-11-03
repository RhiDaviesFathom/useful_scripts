import logging
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from geometry_logic.num_tiles import (
    count_unique_tiles,
    load_coordinates_to_dataframe,
    main,
)

# --- load_coordinates_to_dataframe ------------------------------------------


def test_load_valid_csv(valid_csv):
    df = load_coordinates_to_dataframe(valid_csv)

    assert list(df.columns) == ["Latitude", "Longitude"]
    assert len(df) == 3


def test_load_strips_header_whitespace(write_csv):
    path = write_csv(" Latitude , Longitude \n23.03,72.58\n")

    df = load_coordinates_to_dataframe(path)

    assert list(df.columns) == ["Latitude", "Longitude"]


def test_load_drops_extra_columns(write_csv):
    path = write_csv("Name,Latitude,Longitude\nAhmedabad,23.03,72.58\n")

    df = load_coordinates_to_dataframe(path)

    assert list(df.columns) == ["Latitude", "Longitude"]


def test_load_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_coordinates_to_dataframe(tmp_path / "missing.csv")


@pytest.mark.parametrize(
    ("content", "missing"),
    [
        ("WrongLat,WrongLon\n23.03,72.58\n", "Latitude, Longitude"),
        ("Latitude\n23.03\n", "Longitude"),
    ],
)
def test_load_missing_columns_raises(write_csv, content, missing):
    path = write_csv(content)

    with pytest.raises(ValueError, match=f"missing: {missing}"):
        load_coordinates_to_dataframe(path)


def test_load_empty_file_raises(write_csv):
    path = write_csv("")

    with pytest.raises(pd.errors.EmptyDataError):
        load_coordinates_to_dataframe(path)


# --- count_unique_tiles -----------------------------------------------------


def test_count_unique_tiles_deduplicates():
    df = pd.DataFrame(
        {"Latitude": [23.03, 12.9789, 23.03], "Longitude": [72.58, 77.5917, 72.58]}
    )

    assert count_unique_tiles(df) == 2


def test_count_unique_tiles_empty_dataframe():
    df = pd.DataFrame({"Latitude": [], "Longitude": []})

    assert count_unique_tiles(df) == 0


# --- CLI --------------------------------------------------------------------


def test_cli_success(valid_csv):
    result = CliRunner().invoke(main, ["--filepath", str(valid_csv)])

    assert result.exit_code == 0
    assert "Result: Total number of tiles across the CSV is 2" in result.output


def test_cli_verbose_logs_tile_per_row(valid_csv, caplog):
    with caplog.at_level(logging.DEBUG, logger="geometry_logic.num_tiles"):
        result = CliRunner().invoke(main, ["-f", str(valid_csv), "--verbose"])

    assert result.exit_code == 0
    assert sum("-> " in r.getMessage() for r in caplog.records) == 3


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("WrongLat,WrongLon\n23.03,72.58\n", "must contain"),
        ("", "No columns to parse"),
    ],
    ids=["missing-columns", "empty-file"],
)
def test_cli_invalid_csv_exits_with_error(write_csv, content, expected):
    path = write_csv(content)

    result = CliRunner().invoke(main, ["-f", str(path)])

    assert result.exit_code == 1
    assert "Error:" in result.output
    assert expected in result.output


def test_cli_missing_file_is_usage_error():
    result = CliRunner().invoke(main, ["-f", str(Path("does-not-exist.csv"))])

    assert result.exit_code == 2
    assert "does not exist" in result.output
