"""Count the unique Fathom tiles covered by a CSV of coordinates.

The CSV must contain ``Latitude`` and ``Longitude`` columns. Each row is mapped to
the tile that contains it and the number of distinct tiles is reported.

Example:
    uv run num-tiles --filepath path/to/coordinates.csv
"""

import logging
from pathlib import Path

import click
import pandas as pd
from fathom_pyutils.tiles import TileGeom, TileName

LATITUDE = "Latitude"
LONGITUDE = "Longitude"
REQUIRED_COLUMNS = (LATITUDE, LONGITUDE)

logger = logging.getLogger(__name__)


def load_coordinates_to_dataframe(file_path: Path) -> pd.DataFrame:
    """Load the latitude and longitude columns from a CSV file.

    Whitespace around header names is ignored. Any columns other than
    ``Latitude`` and ``Longitude`` are dropped.

    Args:
        file_path: Path to the CSV file.

    Returns:
        A DataFrame with exactly the ``Latitude`` and ``Longitude`` columns.

    Raises:
        FileNotFoundError: If ``file_path`` does not exist.
        ValueError: If a required column is missing.
        pandas.errors.EmptyDataError: If the file is empty.
    """
    if not file_path.exists():
        msg = f"The file {file_path} does not exist."
        raise FileNotFoundError(msg)

    logger.info("Reading coordinates from %s", file_path)
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()

    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        msg = (
            f"CSV must contain {LATITUDE!r} and {LONGITUDE!r} columns; "
            f"missing: {', '.join(missing)}."
        )
        raise ValueError(msg)

    logger.info("Loaded %d rows from %s", len(df), file_path.name)
    return df[list(REQUIRED_COLUMNS)]


def count_unique_tiles(df: pd.DataFrame) -> int:
    """Count the distinct tiles that contain the given coordinates.

    Args:
        df: A DataFrame with ``Latitude`` and ``Longitude`` columns.

    Returns:
        The number of unique tile names.
    """
    tile_names: set[TileName] = set()
    for row_number, (lon, lat) in enumerate(
        zip(df[LONGITUDE], df[LATITUDE], strict=True), start=1
    ):
        tile_name = TileGeom.from_point(lon=lon, lat=lat).tilename
        logger.debug("Row %d (%s, %s) -> %s", row_number, lat, lon, tile_name)
        tile_names.add(tile_name)

    logger.info("Found %d unique tiles across %d rows", len(tile_names), len(df))
    return len(tile_names)


@click.command()
@click.option(
    "--filepath",
    "-f",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to a CSV file with Latitude and Longitude columns.",
)
@click.option("--verbose", "-v", is_flag=True, help="Log the tile name for every row.")
def main(filepath: Path, *, verbose: bool) -> None:
    """Count the unique Fathom tiles covered by a CSV of coordinates."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        df = load_coordinates_to_dataframe(filepath)
    except (FileNotFoundError, ValueError, pd.errors.EmptyDataError) as e:
        raise click.ClickException(str(e)) from e

    click.echo(
        f"Result: Total number of tiles across the CSV is {count_unique_tiles(df)}"
    )


if __name__ == "__main__":
    main()
