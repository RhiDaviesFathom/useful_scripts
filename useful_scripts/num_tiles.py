"""
A CLI tool to count total coordinate tiles from a CSV file.
The CSV must contain 'Latitude' and 'Longitude' columns. The tool reads the file,
processes the coordinates into dataclasses, and counts the total number of records.
Example usage:
    python num_tiles.py --filepath path/to/coordinates.csv"""

import logging
from pathlib import Path
import click
import pandas as pd
from fathom_pyutils.tiles import TileGeom
from typing import List, Set

# Configure logging format and level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_coordinates_to_dataframe(file_path: Path) -> pd.DataFrame:
    """Reads a CSV file, validates columns, and returns a pandas DataFrame."""
    logging.info(f"Attempting to read file from: {file_path}")

    # Check if file exists
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    try:
        # Read CSV
        df = pd.read_csv(file_path)

        # Normalize column names (strip whitespace and match case)
        df.columns = df.columns.str.strip()

        # Validate required columns
        required_columns = {"Latitude", "Longitude"}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            logging.error(f"CSV is missing required columns: {missing}")
            raise ValueError("CSV must contain 'Latitude' and 'Longitude' columns.")

        logging.info(f"Successfully loaded {len(df)} rows from {file_path.name}.")
        return df[["Latitude", "Longitude"]]

    except pd.errors.EmptyDataError:
        logging.error("The provided CSV file is empty.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading CSV: {e}")
        raise


def process_tiles(df: pd.DataFrame) -> int:
    """Generates tile names using from_point, prints them for debugging,

    and returns the count of unique tile names.
    """
    logging.info("Generating tile names using 'from_point' class method...")

    tile_names_list: List[str] = []

    # Iterate through DataFrame rows
    for _, row in df.iterrows():
        # Notice the signature mapping: lon is passed first, then lat
        tile_instance = TileGeom.from_point(lon=row["Longitude"], lat=row["Latitude"])
        tile_names_list.append(tile_instance.tilename)

    # --- DEBUGGING PRINT SECTION ---
    print("\n--- DEBUG: Generated Tile Names List ---")
    for idx, name in enumerate(tile_names_list, 1):
        print(f"Row {idx}: {name}")
    print("---------------------------------------\n")
    # -------------------------------

    # Filter to unique tile names
    unique_tile_names: Set[str] = set(tile_names_list)
    unique_count = len(unique_tile_names)

    logging.info(
        f"Found {unique_count} unique tiles out of {len(tile_names_list)} rows."
    )
    return unique_count


@click.command()
@click.option(
    "--filepath",
    "-f",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the input CSV file containing Latitude and Longitude.",
)
def main(filepath: Path):
    """A CLI tool to count total coordinate tiles from a CSV file."""
    try:
        df = load_coordinates_to_dataframe(filepath)
        tile_count = process_tiles(df)

        # Final result output
        click.echo(f"\nResult: Total number of tiles across the CSV is {tile_count}")

    except Exception as e:
        # Logs have already captured specific errors, graceful exit for user
        click.echo(
            f"\nProcess failed. Check logs above for details. Error: {e}",
            err=True,
        )


if __name__ == "__main__":
    main()
