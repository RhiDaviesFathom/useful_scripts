import click
import numpy as np
from oasislmf.utils.peril import PERILS as OASIS_PERILS  # type: ignore

# This is a copy of the dict in src/complex_model/constants.py
FLUVIAL = OASIS_PERILS["river flood"]
PLUVIAL = OASIS_PERILS["flash flood"]
COASTAL = OASIS_PERILS["storm surge"]

# This is a copy of the dict in src/complex_model/constants.py
FLUVIAL_ID = FLUVIAL["id"]
PLUVIAL_ID = PLUVIAL["id"]
COASTAL_ID = COASTAL["id"]

PERIL_IDS = [
    FLUVIAL_ID,
    PLUVIAL_ID,
    COASTAL_ID,
]  # This is a copy of the dict in src/complex_model/constants.py

RETURN_PERIODS = {  # This is a copy of the dict in src/complex_model/constants.py
    "rp5_year_depth": 5,
    "rp10_year_depth": 10,
    "rp20_year_depth": 20,
    "rp50_year_depth": 50,
    "rp100_year_depth": 100,
    "rp200_year_depth": 200,
    "rp500_year_depth": 500,
    "rp1000_year_depth": 1000,
}

CATCHMENTS = {
    "lat_id": np.dtype("int64"),
    "lon_id": np.dtype("int64"),
    "rp5_year_depth": np.dtype("float64"),
    "rp10_year_depth": np.dtype("float64"),
    "rp20_year_depth": np.dtype("float64"),
    "rp50_year_depth": np.dtype("float64"),
    "rp100_year_depth": np.dtype("float64"),
    "rp200_year_depth": np.dtype("float64"),
    "rp500_year_depth": np.dtype("float64"),
    "rp1000_year_depth": np.dtype("float64"),
    "peril_id": np.dtype("object"),
    "catchment_id": np.dtype("int64"),
}

CATCHMENT_EVENTS = {
    "event_id": np.dtype("int64"),
    "return_period": np.dtype("int64"),
    "peril": np.dtype("object"),
    "catchment_id": np.dtype("int64"),
}


@click.command()
@click.option("--event_id", help="Event Id", type=click.INT)
@click.option(
    "--peril_id",
    help="The Peril ID. One of: ...",
    type=click.STRING,
)
def main(event_id: int, peril_id: str) -> None:
    """
    For a given event_id and peril, directory paths to the catchments and catchment_event folders,
    create 8 csvs (one per return period) for that event_id that has the folowing columns:
    lat_id
    lon_id
    flood_depth_cm
    """


# Read in all the catchment events
# Create a list of the catchment_ids involved in the event
# filter by these catchments
# merge

if __name__ == "__main__":
    main()
