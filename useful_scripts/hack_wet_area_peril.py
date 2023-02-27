import os
import sys

import pandas as pd


def generate_wet_area_pickle(output_dir: str):
    """
    Generates wet_area_peril.pickle from the provided catchments dir
    and wet_area_peril.pickle into given output dir
    """
    wet_area_df = pd.DataFrame()

    if not os.path.isdir(output_dir):
        raise IOError("Output directory doesn't exist.")

    wet_area_dict = {"lat_id": [191322, 183956], "lon_id": [-4197, -9970]}
    wet_area_df = pd.DataFrame(data=wet_area_dict)
    print(wet_area_df.to_string())

    wet_area_df.to_pickle(f"{output_dir}/wet_area_peril.pickle")


def generate_wet_area_parquet(output_dir: str):
    """
    Generates wet_area_peril.parquet from the provided catchments dir
    and wet_area_peril.pickle into given output dir
    """
    wet_area_df = pd.DataFrame()

    if not os.path.isdir(output_dir):
        raise IOError("Output directory doesn't exist.")

    wet_area_dict = {
        "lat_id": [191322],
        "lon_id": [-4197],
        "peril_id": ["OSF"],
    }
    wet_area_df = pd.DataFrame(data=wet_area_dict)
    print(wet_area_df.to_string())

    wet_area_df.to_parquet(f"{output_dir}/wet_area_peril.parquet")


# if __name__ == "__main__":
#     generate_wet_area_pickle(sys.argv[1])


if __name__ == "__main__":
    generate_wet_area_parquet(sys.argv[1])
