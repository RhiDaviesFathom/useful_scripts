"""
Gives the ability to edit and create parquet files
"""
import os
import pathlib
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def _get_absolute_path_from_relative_path(relative_path: str) -> str:
    """
    Given a relative path (relative_path) to X from a file (test_file) this
    returns the absolute path to X
    """
    file_dir = pathlib.Path(__file__).parent.resolve()
    return f"{file_dir}/{relative_path}"


def _safe_mkdir(relative_path: str) -> None:
    """
    Makes directory if it doesn't exist. If it does, then deletes the directory and recreates it.
    """
    path = _get_absolute_path_from_relative_path(relative_path)
    try:
        os.stat(path)
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
    os.mkdir(path)


def create_catchment_parquet():
    """
    Uses a pandas dataframe to create a new parquet file.
    """
    catchment_df = pd.DataFrame(
        {
            "lat_id": [184065, 184065],
            "lon_id": [-13481, -13480],
            "peril_id": ["ORF", "ORF"],
            "return_period": [20, 20],
            "flood_depth_cm": [85, 57],
        }
    )

    table = pa.Table.from_pandas(catchment_df)
    dir_path = _get_absolute_path_from_relative_path("output")
    pq.write_table(table, f"{dir_path}/catchment_290.parquet")


def main():
    """
    Comment out the unnecessary bit so that when editing_parquet.py from the command line,
    the desired behaviour occurs.
    """

    # copy_parquet()
    create_catchment_parquet()


if __name__ == "__main__":
    main()
