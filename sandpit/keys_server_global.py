from __future__ import annotations

__all__ = ["GLOBAL_FLOODKeysLookup", "GLOBAL_FLOODKeysServer"]

# Python library imports
import io
import itertools
import json
import logging
import os

import numpy as np
import pandas as pd
import rasterio
import rasterio.windows as rasterio_windows

# oasislmf imports
from oasislmf.lookup.factory import BasicKeyServer
from oasislmf.lookup.interface import KeyLookupInterface
from oasislmf.utils.log import oasis_log
from oasislmf.utils.status import OASIS_KEYS_STATUS, OASIS_KEYS_FL
from oasislmf.utils.peril import PERIL_GROUPS
from oasislmf.utils.coverages import SUPPORTED_COVERAGE_TYPES

from typing import Any, List

from . import constants, file_managers

logger = logging.getLogger()

SUPPORTED_COVERAGE_TYPES_BY_ID = {
    coverage_dict["id"]: coverage_key
    for coverage_key, coverage_dict in SUPPORTED_COVERAGE_TYPES.items()
}

USED_LOC_DF_COLUMNS = {
    "accnumber",
    "bipoi",
    "buildingtype",
    "constructioncode",
    "firstfloorheight",
    "firstfloorheightunit",
    "floorsoccupied",
    "latitude",
    "loc_id",
    "locnumber",
    "locperilscovered",
    "user_vulnerability_id",
    "longitude",
    "numberofstoreys",
    "occupancycode",
    "portnumber",
    "postalcode",
    "yearbuilt",
}


class GLOBAL_FLOODKeysServer(BasicKeyServer):
    def write_keys_file(
        self,
        results: List[pd.DataFrame],
        successes_fp: str,
        errors_fp: str,
        output_format: str,
        keys_success_msg: str,
    ) -> Any:
        """
        We need to write out a model_data_files.csv which should include a list of ALL the catchment files that the
        execution stage will need to work its magic. We have to do this here, rather than in process_locations() below
        because process_locations() operates on "chunks" of the OED location file (so that it can be run in parallel).
        Whereas this function, via the 'results' argument, has access to each resulting output "chunk" of the keys file
        from process_locations() and so has access to every valid location in the OED input file.

        Args:
            results (typing.List[pd.DataFrame]): a list of dataframes representing the keys file in "chunks"
            successes_fp (str): successes file path
            errors_fp (str): errors file path
            output_format (str): Specifies if the output should be in oasis or json format.
            keys_success_msg (str): The oasislmf generated keys success message

        Returns:
            tuple: successes_fp, successes_count,(errors_fp, error_count)
        """
        model_files = set()
        model_files.update(constants.MODEL_FILES)
        iter_results, results_backup = itertools.tee(results)
        for result in iter_results:
            for catchment_id in result["catchment_id"].unique():
                if catchment_id < 1:
                    continue
                try:
                    catchment_id = int(catchment_id)
                    model_files.add(f"catchments/catchment_{catchment_id}.parquet")
                    model_files.add(
                        f"catchment_events/catchment_{catchment_id}_events.parquet"
                    )
                except ValueError:
                    pass
        model_files_df = pd.DataFrame(data=list(model_files))
        model_files_df.to_csv(
            f"{self.output_dir}/model_data_files.csv", index=False, header=False
        )
        return super().write_keys_file(
            results_backup, successes_fp, errors_fp, output_format, keys_success_msg
        )


class GLOBAL_FLOODKeysLookup(KeyLookupInterface):
    """Model Specific keys lookup logic

    Args:
        KeyLookupInterface (class): Interface for the KeyLookup as defined in oasislmf

    Raises:
        IOError: multiple vulnerability_scale_factory files
        IOError: multiple vulnerability_scale_factory files
        IndexError: could not find catchment for lat/lon. Results in a failed key

    Returns:
        pd.DataFrame: the keys DataFrame
    """

    @oasis_log()  # type: ignore
    def __init__(
        self,
        config: pd.DataFrame,
        config_dir: str | None,
        user_data_dir: str,
        output_dir: str,
    ):
        """
        Initialise the static data required for the lookup.

        Args:
            config (pd.DataFrame): the config DataFrame produced from the lookup_config.json
            config_dir (str): path to the config directory
            user_data_dir (str): path to the user data directory
            output_dir (str): path to the output directory

        Raises:
            IOError: multiple user_supplied_vulnerability files
            IOError: multiple vulnerability_scale_factory files
        """
        lookup_data_dir: str = str(config.get("lookup_data_dir") or "")  # type: ignore
        keys_data_dir: str = str(config.get("keys_data_path") or "")  # type: ignore
        self.keys_data_directory: str = (
            lookup_data_dir or keys_data_dir or "keys_data/UK_FLOOD"
        )
        self.user_data_file_manager = file_managers.StaticFileManager(user_data_dir)
        self.output_dir: str = output_dir

        # Validate the user uploaded files
        if self.user_data_file_manager:
            os_user_data_dir = os.listdir(user_data_dir)

            input_usv_filename_list = [
                f
                for f in os_user_data_dir
                if f.startswith("user_supplied_vulnerability")
            ]

            input_vsf_filename_list = [
                f
                for f in os_user_data_dir
                if f.startswith("vulnerability_scale_factor")
            ]

            # Validate the User Supplied vulnerability file
            if len(input_usv_filename_list) > 1:
                raise IOError(
                    "More than file beginning with user_supplied_vulnerability has been supplied, only one can be "
                    "supplied at a time."
                )
            if len(input_usv_filename_list) == 1:
                self.user_data_file_manager.get_user_supplied_vulnerability_file(
                    input_usv_filename_list[0]
                )

            # Validate the Vulnerability Scale Factor file
            if len(input_vsf_filename_list) > 1:
                raise IOError(
                    "More than file beginning with vulnerability_scale_factory has been supplied, only one can be "
                    "supplied at a time."
                )
            if len(input_vsf_filename_list) == 1:
                self.user_data_file_manager.get_vulnerability_scale_factors(
                    input_vsf_filename_list[0]
                )

        # vulnerability_dict based on MCM_code
        with io.open(
            os.path.join(self.keys_data_directory, "vulnerability_dict.csv"),
            "r",
            encoding="utf-8",
        ) as f:
            self.vulnerability_df = pd.read_csv(f, float_precision="high")

        # Reformulate in pandas dataframe
        self.vulnerability_df.loc[
            self.vulnerability_df["coveragetype_id"] == 1, "coveragetype_id"
        ] = SUPPORTED_COVERAGE_TYPES["buildings"]["id"]
        self.vulnerability_df.loc[
            self.vulnerability_df["coveragetype_id"] == 3, "coveragetype_id"
        ] = SUPPORTED_COVERAGE_TYPES["contents"]["id"]
        self.vulnerability_df.loc[
            self.vulnerability_df["coveragetype_id"] == 4, "coveragetype_id"
        ] = SUPPORTED_COVERAGE_TYPES["bi"]["id"]

        self.vulnerability_df.loc[
            self.vulnerability_df["peril_type"] == 1, "peril_type"
        ] = constants.FLUVIAL_ID
        self.vulnerability_df.loc[
            self.vulnerability_df["peril_type"] == 2, "peril_type"
        ] = constants.PLUVIAL_ID
        self.vulnerability_df.loc[
            self.vulnerability_df["peril_type"] == 3, "peril_type"
        ] = constants.COASTAL_ID
        self.vulnerability_df.loc[
            self.vulnerability_df["peril_type"] == -9999, "peril_type"
        ] = (
            constants.FLUVIAL_ID
            + ";"
            + constants.PLUVIAL_ID
            + ";"
            + constants.COASTAL_ID
        )

        # File that links residential OED OccupancyCode to MCM_code
        with io.open(
            os.path.join(self.keys_data_directory, "MCM_OED_residential.csv"),
            "r",
            encoding="utf-8",
        ) as f:
            self.mcm_res_df = pd.read_csv(f, float_precision="high")

        # File that links nonresidential OED OccupancyCode to MCM_code
        with io.open(
            os.path.join(self.keys_data_directory, "MCM_OED_nonresidential.csv"),
            "r",
            encoding="utf-8",
        ) as f:
            self.mcm_nonres_df = pd.read_csv(f, float_precision="high")

        # File that links (full precision) postcodes to their centroid latitude/longitude
        with io.open(
            os.path.join(self.keys_data_directory, "postcode_dict.csv"),
            "r",
            encoding="utf-8",
        ) as f:
            self.postcode_df = pd.read_csv(f, float_precision="high")

        self.postcode_df = self.postcode_df.rename(
            columns={
                "latitude": "pc_lat",
                "longitude": "pc_lon",
                "postcode": "postalcode",
            }
        )
        self.postcode_df["postalcode"] = (
            self.postcode_df["postalcode"].astype(str).str.replace(" ", "").str.upper()  # type: ignore
        )

        # File that links yearbuilt to yearbuilt categories
        with io.open(
            os.path.join(self.keys_data_directory, "yearbuilt.csv"),
            "r",
            encoding="utf-8",
        ) as f:
            self.yearbuilt_df = pd.read_csv(f, float_precision="high")

        # File that lists at-risk lat_ids and lon_ids
        self.at_risk_df = pd.read_parquet(
            os.path.join(self.keys_data_directory, "wet_area_peril.parquet")
        )

        self.default_field_values = {
            "locperilscovered": "ORF;OSF;WSS",
            "buildingtype": 0,
            "occupancycode": 1000,
            "constructioncode": 5000,
            "numberofstoreys": 0,
            "floorsoccupied": "0",
            "firstfloorheight": -999,
            "firstfloorheightunit": 1,
            "yearbuilt": 0,
            "bipoi": 365,
            "latitude": 0,
            "longitude": 0,
            "postalcode": "-1",
        }

        self.default_field_types = {
            "locperilscovered": str,
            "buildingtype": int,
            "occupancycode": int,
            "constructioncode": int,
            "numberofstoreys": int,
            "floorsoccupied": str,
            "firstfloorheight": float,
            "firstfloorheightunit": int,
            "yearbuilt": int,
            "bipoi": float,
            "latitude": float,
            "longitude": float,
            "postalcode": str,
        }

        self.coverages = [
            SUPPORTED_COVERAGE_TYPES["buildings"]["id"],
            SUPPORTED_COVERAGE_TYPES["contents"]["id"],
            SUPPORTED_COVERAGE_TYPES["bi"]["id"],
        ]
        self.perils = [constants.FLUVIAL_ID, constants.PLUVIAL_ID, constants.COASTAL_ID]

    def get_latlon(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        """
        Find the longitude and latitude

        Args:
            loc_df (pd.DataFrame): the location DataFrame

        Returns:
            pd.DataFrame: the location DataFrame
        """

        # Find postalcode lat/lon
        # FIXME: what are we going to do about this postal code stuff?
        loc_df["postalcode"] = (
            loc_df["postalcode"].astype(str).str.replace(" ", "").str.upper()  # type: ignore
        )
        loc_df = loc_df.merge(
            self.postcode_df, how="left", left_on="postalcode", right_on="postalcode"
        )

        # Overwrite if supplied lat/lon is invalid
        temp_ll = loc_df.loc[
            (loc_df["latitude"] == 0)
            & (loc_df["longitude"] == 0)
            & (loc_df["pc_lat"].isnull().all() is False)
            & (loc_df["pc_lon"].isnull().all() is False),
            ["pc_lat", "pc_lon"],
        ]
        temp_ll = temp_ll.filter(items=["latitude", "longitude"])

        loc_df.loc[
            (loc_df["latitude"] == 0)
            & (loc_df["longitude"] == 0)
            & (loc_df["pc_lat"].isnull().all() is False)
            & (loc_df["pc_lon"].isnull().all() is False),
            ["latitude", "longitude"],
        ] = temp_ll

        return loc_df

    def get_peril(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduce the perils covered from the location DataFrame

        Args:
            loc_df (pd.DataFrame): the location DataFrame

        Returns:
            pd.DataFrame: the location DataFrame
        """

        # Deal with peril groups
        loc_df.loc[
            (loc_df["locperilscovered"] == PERIL_GROUPS["all"]["id"]),
            ["locperilscovered"],
        ] = (
            constants.FLUVIAL_ID
            + ";"
            + constants.PLUVIAL_ID
            + ";"
            + constants.COASTAL_ID
        )
        loc_df.loc[
            (loc_df["locperilscovered"] == PERIL_GROUPS["flood w/o storm surge"]["id"]),
            ["locperilscovered"],
        ] = (
            constants.FLUVIAL_ID + ";" + constants.PLUVIAL_ID
        )
        loc_df.loc[
            (
                loc_df["locperilscovered"]
                == PERIL_GROUPS["windstorm w/ storm surge"]["id"]
            ),
            ["locperilscovered"],
        ] = constants.COASTAL_ID

        return loc_df

    def get_latlon_ids(self, keys_df: pd.DataFrame) -> pd.DataFrame:
        """
        Adding two new columns to the keys Dataframe: lat_id and lon_id

        Args:
            keys_df (pd.DataFrame): the keys DataFrame

        Returns:
            pd.DataFrame: the keys DataFrame
        """
        keys_df["lat_id"] = np.floor(np.asarray(keys_df["latitude"] * 3600))
        keys_df["lon_id"] = np.floor(np.asarray(keys_df["longitude"] * 3600))
        return keys_df

    def get_mobile_home(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        loc_df["mobilehome_cat"] = False
        loc_df.loc[
            (loc_df["constructioncode"] >= 5350)
            & (loc_df["constructioncode"] < 5400)
            & (loc_df["occupancycode"] < 1100),
            "mobilehome_cat",
        ] = True

        return loc_df

    def get_mcm_code_residential(self, loc_df_res: pd.DataFrame) -> pd.DataFrame:
        """
        # FIXME: this reference to mcm needs to be removed and replaced with new functionality.

        Args:
            loc_df_res (pd.DataFrame): the residential locations DataFrame

        Returns:
            pd.DataFrame: the residential locations Dataframe
        """

        # Get YearBuilt category
        loc_df_res = loc_df_res.merge(right=self.yearbuilt_df, how="left")  # type: ignore

        # Set building type for mobile homes
        loc_df_res.loc[loc_df_res["mobilehome_cat"], "building_cat"] = "bungalow"  # type: ignore

        # Assign MCM code based on BuildingType
        # set 1 story detached
        loc_df_res.loc[  # type: ignore
            (loc_df_res["buildingtype"] == 1)
            & (loc_df_res["numberofstoreys"] == 1)
            & (loc_df_res["building_cat"].isnull()),
            "building_cat",
        ] = "bungalow"
        # set detached
        loc_df_res.loc[  # type: ignore
            (loc_df_res["buildingtype"] == 1) & (loc_df_res["building_cat"].isnull()),
            "building_cat",
        ] = "detached"
        # set semi-detached
        loc_df_res.loc[  # type: ignore
            (loc_df_res["buildingtype"] == 2) & (loc_df_res["building_cat"].isnull()),
            "building_cat",
        ] = "semidetached"
        # set terraced
        loc_df_res.loc[  # type: ignore
            ((loc_df_res["buildingtype"] == 3) | (loc_df_res["buildingtype"] == 4))
            & (loc_df_res["building_cat"].isnull()),
            "building_cat",
        ] = "terraced"
        # set multi story bungalow
        loc_df_res.loc[  # type: ignore
            (loc_df_res["buildingtype"] == 5)
            & (loc_df_res["numberofstoreys"] > 1)
            & (loc_df_res["building_cat"].isnull()),
            "building_cat",
        ] = "detached"
        # set bungalow
        loc_df_res.loc[  # type: ignore
            (loc_df_res["buildingtype"] == 5) & (loc_df_res["building_cat"].isnull()),
            "building_cat",
        ] = "bungalow"

        # Assign MCM code based on OccupancyCode
        # OVERRIDE flats
        loc_df_res.loc[
            (loc_df_res["occupancycode"] == 1052)
            | (loc_df_res["occupancycode"] == 1055),
            "building_cat",
        ] = "flat"  # type: ignore
        # OVERRIDE terraced
        loc_df_res.loc[
            (loc_df_res["occupancycode"] == 1056), "building_cat"
        ] = "terraced"  # type: ignore

        # Merge to get MCM code
        loc_df_res = loc_df_res.merge(right=self.mcm_res_df, how="left")  # type: ignore

        # OVERRIDE temporary lodging
        loc_df_res.loc[
            (loc_df_res["occupancycode"] == 1053), ["mcm_code", "building_cat"]  # type: ignore
        ] = [
            51,
            "nonres",  # type: ignore
        ]  # assign directly as nonresidential
        # OVERRIDE group institutional housing
        loc_df_res.loc[
            (loc_df_res["occupancycode"] == 1054), ["mcm_code", "building_cat"]  # type: ignore
        ] = [
            6,
            "nonres",  # type: ignore
        ]  # assign directly as nonresidential
        # Make remaining valid OccupancyCodes a general MCM code
        loc_df_res.loc[
            (  # type: ignore
                (loc_df_res["occupancycode"] == 1050)
                | (loc_df_res["occupancycode"] == 1051)
                | (loc_df_res["occupancycode"] == 1000)
            )
            & (loc_df_res["building_cat"].isnull()),
            ["mcm_code", "building_cat"],
        ] = [
            1,
            "general_res",  # type: ignore
        ]  # assign directly as general residential

        return loc_df_res

    def get_mcm_code_non_residential(self, loc_df_nonres: pd.DataFrame) -> pd.DataFrame:
        """
        # FIXME: this reference to mcm needs to be removed and replaced with new functionality.

        Args:
            loc_df_nonres (pd.DataFrame): the nonresidential locations DataFrame

        Returns:
            pd.DataFrame: the nonresidential locations DataFrame
        """

        loc_df_nonres = loc_df_nonres.merge(right=self.mcm_nonres_df, how="left")  # type: ignore
        loc_df_nonres["building_cat"] = "nonres"

        return loc_df_nonres

    def get_number_of_storeys(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        """
        Using the building categories to deduce the number of stories per location.
        A new 'numberofstoreys' column is added to the locations DataFrame.

        Args:
            loc_df (pd.DataFrame): the locations DataFrame

        Returns:
            pd.DataFrame: the locations DataFrame
        """

        # bungalows never modified by number of storeys
        loc_df.loc[loc_df["building_cat"] == "bungalow", "numberofstoreys"] = 0

        # default for detached/semi/terraced
        loc_df.loc[
            (loc_df["numberofstoreys"] == 0)
            & (
                (loc_df["building_cat"] == "detached")
                | (loc_df["building_cat"] == "semidetached")
                | (loc_df["building_cat"] == "terraced")
            ),
            "numberofstoreys",
        ] = 2

        # flats default to 1 storey
        loc_df.loc[
            (loc_df["numberofstoreys"] == 0) & (loc_df["building_cat"] == "flat"),
            "numberofstoreys",
        ] = 1

        # general residential always 0
        loc_df.loc[loc_df["building_cat"] == "general_res", "numberofstoreys"] = 0

        # sets defaults for nonresidential
        loc_df.loc[
            (loc_df["numberofstoreys"] == 0) & (loc_df["building_cat"] == "nonres"),
            "numberofstoreys",
        ] = 1

        # cap at 6 storeys
        loc_df.loc[loc_df["numberofstoreys"] > 6, "numberofstoreys"] = 6

        return loc_df

    def get_floors_occupied(self, floorsoccupied: str) -> int:
        """
        Getting the number of floors occupied at a location

        Args:
            floorsoccupied (str): a string listing the floors that are occupied.

        Returns:
            int: the number of floors occupied
        """

        floors = min(list(map(int, floorsoccupied.split(";"))))
        floors = max(floors, 0)
        floors = min(3, floors)

        return floors

    def get_bi_poi(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        """
        Adding a bipoi column to the locations DataFrame

        Args:
            loc_df (pd.DataFrame): the locations DataFrame

        Returns:
            pd.DataFrame: the locations DataFrame
        """

        # residential (ALE)
        loc_df.loc[
            ((loc_df["bipoi"] > 0) & (loc_df["bipoi"] <= 183))
            & (
                (loc_df["building_cat"] == "detached")
                | (loc_df["building_cat"] == "semidetached")
                | (loc_df["building_cat"] == "terraced")
                | (loc_df["building_cat"] == "flat")
                | (loc_df["building_cat"] == "bungalow")
                | (loc_df["building_cat"] == "general_res")
            ),
            "bipoi_cat",
        ] = 1
        loc_df.loc[
            ((loc_df["bipoi"] > 183))
            & (
                (loc_df["building_cat"] == "detached")
                | (loc_df["building_cat"] == "semidetached")
                | (loc_df["building_cat"] == "terraced")
                | (loc_df["building_cat"] == "flat")
                | (loc_df["building_cat"] == "bungalow")
                | (loc_df["building_cat"] == "general_res")
            ),
            "bipoi_cat",
        ] = 183

        # nonresidential (BI)
        loc_df.loc[
            ((loc_df["bipoi"] > 0) & (loc_df["bipoi"] < 137))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 0
        loc_df.loc[
            ((loc_df["bipoi"] >= 137) & (loc_df["bipoi"] < 228))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 137
        loc_df.loc[
            ((loc_df["bipoi"] >= 228) & (loc_df["bipoi"] < 319))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 228
        loc_df.loc[
            ((loc_df["bipoi"] >= 319) & (loc_df["bipoi"] < 411))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 319
        loc_df.loc[
            ((loc_df["bipoi"] >= 411) & (loc_df["bipoi"] < 502))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 411
        loc_df.loc[
            ((loc_df["bipoi"] >= 502) & (loc_df["bipoi"] < 593))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 502
        loc_df.loc[
            ((loc_df["bipoi"] >= 593) & (loc_df["bipoi"] < 684))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 593
        loc_df.loc[
            ((loc_df["bipoi"] >= 684) & (loc_df["bipoi"] < 776))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 684
        loc_df.loc[
            ((loc_df["bipoi"] >= 776) & (loc_df["bipoi"] < 867))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 776
        loc_df.loc[
            ((loc_df["bipoi"] >= 867) & (loc_df["bipoi"] < 958))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 867
        loc_df.loc[
            ((loc_df["bipoi"] >= 958) & (loc_df["bipoi"] < 1049))
            & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 958
        loc_df.loc[
            (loc_df["bipoi"] >= 1049) & (loc_df["building_cat"] == "nonres"),
            "bipoi_cat",
        ] = 1049

        return loc_df

    def get_first_floor_height(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        """
        adding a 'firstfloorheight' column to the locations DataFrame

        Args:
            loc_df (pd.DataFrame): the locations DataFrame

        Returns:
            pd.DataFrame: the locations DataFrame
        """

        # correct feet entries
        loc_df.loc[
            (loc_df["firstfloorheightunit"] == 1)  # type: ignore
            & (loc_df["firstfloorheight"] != -999),
            "firstfloorheight",
        ] *= 0.3048

        # assign defaults
        loc_df.loc[
            (loc_df["mobilehome_cat"]) & (loc_df["firstfloorheight"] == -999),
            "firstfloorheight",
        ] = 0.6  # mobilehome default
        loc_df.loc[
            (loc_df["building_cat"] == "nonres") & (loc_df["firstfloorheight"] == -999),
            "firstfloorheight",
        ] = 0.6  # nonresidential default
        loc_df.loc[
            (loc_df["firstfloorheight"] == -999), "firstfloorheight"
        ] = 0.3  # remaining are residential

        # get ffh categories
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 0) & (loc_df["firstfloorheight"] < 0.05),
            "ffh_cat",
        ] = 0
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 0.05) & (loc_df["firstfloorheight"] < 0.15),
            "ffh_cat",
        ] = 0.05
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 0.15) & (loc_df["firstfloorheight"] < 0.25),
            "ffh_cat",
        ] = 0.15
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 0.25) & (loc_df["firstfloorheight"] < 0.35),
            "ffh_cat",
        ] = 0.25
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 0.35) & (loc_df["firstfloorheight"] < 0.65),
            "ffh_cat",
        ] = 0.35
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 0.65) & (loc_df["firstfloorheight"] < 1.35),
            "ffh_cat",
        ] = 0.65
        loc_df.loc[
            (loc_df["firstfloorheight"] >= 1.35) & (loc_df["firstfloorheight"] < 2.65),
            "ffh_cat",
        ] = 1.35
        loc_df.loc[(loc_df["firstfloorheight"] >= 2.65), "ffh_cat"] = 2.65

        return loc_df

    def format_keys_df(self, keys_df: pd.DataFrame) -> None:
        """
        Adds catchment_id (int) and model_data (json) columns to keys dataframe

        Args:
            keys_df (pd.DataFrame): the keys DataFrame

        Raises:
            IndexError: could not find catchment for lat/lon. Results in a failed key
        """
        keys_df["catchment_id"] = -1
        keys_df["area_peril_id"] = -1
        keys_df["model_data"] = ""
        success_keys = keys_df.loc[
            keys_df["status"] == OASIS_KEYS_STATUS["success"]["id"]
        ]
        with rasterio.open(f"{self.keys_data_directory}/uk.tif") as catchments_tif:
            for row in success_keys.itertuples():
                py, px = catchments_tif.index(row.longitude, row.latitude)
                try:
                    catchments_rasta = catchments_tif.read(
                        1, window=rasterio_windows.Window(px, py, 1, 1)
                    )
                    catchment_id = int(catchments_rasta[0][0])
                    if catchment_id < 0:
                        raise IndexError
                    keys_df.at[row.Index, "catchment_id"] = catchment_id
                    keys_df.at[row.Index, "model_data"] = json.dumps(
                        {
                            "lat_id": int(row.lat_id),
                            "lon_id": int(row.lon_id),
                            "catchment_id": int(catchment_id),
                            "vulnerability_id": int(row.vulnerability_id),
                            "user_vulnerability_id": int(row.user_vulnerability_id),
                            "peril_id": row.peril_id,
                            "coverage": SUPPORTED_COVERAGE_TYPES_BY_ID[
                                row.coverage_type
                            ],
                            "coverage_type_id": int(row.coverage_type),
                            "loc_number": int(row.locnumber),
                            "port_number": int(row.portnumber),
                            "acc_number": int(row.accnumber),
                        },
                        separators=(",", ":"),
                    )
                except IndexError:
                    keys_df.at[row.Index, "status"] = OASIS_KEYS_FL
                    keys_df.at[
                        row.Index, "message"
                    ] = "could not find catchment for lat/lon"

    @oasis_log()  # type: ignore
    def process_locations(self, loc_df: pd.DataFrame) -> pd.DataFrame:
        """
        Processing each of the locations to generate a keys DataFrame

        WARNING: if multiprocessing is in use, then the loc_df is only a slice of the input OED locations. Furthermore,
         the keys_df that is returned is a slice of the keys and NOT all of them.

        Args:
            loc_df (pd.DataFrame): the locations DataFrame

        Returns:
            pd.DataFrame: the keys DataFrame
        """
        loc_df = loc_df.rename(columns=str.lower)  # type: ignore
        if "locuserdef1" in loc_df.columns:
            loc_df.rename(
                columns={"locuserdef1": "user_vulnerability_id"}, inplace=True
            )
        else:
            loc_df["user_vulnerability_id"] = 0

        key: str
        value: Any
        for key, value in self.default_field_values.items():
            # Column not in data
            if key not in loc_df.columns:
                loc_df[key] = value  # assign default
                loc_df[key] = loc_df[key].astype(
                    self.default_field_types[key]
                )  # assign data type
            # Column exists
            else:
                loc_df[key] = loc_df[key].astype(
                    self.default_field_types[key]
                )  # assign data type
                loc_df[key] = loc_df[key].fillna(value)  # fill in NaNs with default

                # Fill in nodatas from oasislmf.utils.data.get_location_df
                # Seems like they do not infill for float, so the above should catch these
                if self.default_field_types[key] == str:
                    loc_df.loc[loc_df[key] == "", key] = value
                elif self.default_field_types[key] == int:
                    loc_df.loc[loc_df[key] == 0, key] = value
        # strip out all the columns which we don't use to save memory
        loc_df = loc_df.filter(
            items=list(USED_LOC_DF_COLUMNS),
            axis=1,
        )

        # mobile home
        loc_df = self.get_mobile_home(loc_df)

        # mcm code residential
        loc_df_res = loc_df[loc_df["occupancycode"] < 1100]
        if not loc_df_res.empty:
            loc_df_res = self.get_mcm_code_residential(loc_df_res)

        # mcm code non-residential
        loc_df_nonres = loc_df[loc_df["occupancycode"] >= 1100]
        if not loc_df_nonres.empty:
            loc_df_nonres = self.get_mcm_code_non_residential(loc_df_nonres)

        # combine res & nonres
        loc_df = pd.concat([loc_df_res, loc_df_nonres])

        # number of storeys
        loc_df = self.get_number_of_storeys(loc_df)

        # floors occupied
        loc_df["floorsoccupied"] = loc_df.apply(
            lambda x: self.get_floors_occupied(x["floorsoccupied"]), axis=1
        )

        # first floor height
        loc_df = self.get_first_floor_height(loc_df)

        # bi poi
        loc_df = self.get_bi_poi(loc_df)

        # get latlon
        loc_df = self.get_latlon(loc_df)

        # get peril
        loc_df = self.get_peril(loc_df)

        # get keys df
        keys_df = pd.DataFrame(columns=loc_df.columns)
        for peril in self.perils:
            for coverage in self.coverages:
                loc_df["coveragetype_id"] = coverage
                loc_df["peril_id"] = peril
                keys_df = pd.concat(
                    [
                        keys_df,
                        loc_df.loc[loc_df["locperilscovered"].str.contains(peril)],
                    ]
                )  # only write keys if relevant for LocPerilsCovered

        # remove values that would screw up lookup

        # floorsoccupied if not (contents) & (flat | nonres)
        keys_df.loc[
            (keys_df["coveragetype_id"] != SUPPORTED_COVERAGE_TYPES["contents"]["id"])
            | (
                (keys_df["building_cat"] != "flat")
                & (keys_df["building_cat"] != "nonres")
            ),
            "floorsoccupied",
        ] = -9999

        # bipoi_cat if not (bi)
        keys_df.loc[
            (keys_df["coveragetype_id"] != SUPPORTED_COVERAGE_TYPES["bi"]["id"]),
            "bipoi_cat",
        ] = -9999

        # numberofstoreys if (bi) | ((flats) & (contents))
        keys_df.loc[
            (keys_df["coveragetype_id"] == SUPPORTED_COVERAGE_TYPES["bi"]["id"])
            | (
                (keys_df["building_cat"] == "flat")
                & (
                    keys_df["coveragetype_id"]
                    == SUPPORTED_COVERAGE_TYPES["contents"]["id"]
                )
            ),
            "numberofstoreys",
        ] = 0

        # mcm_code if (bi) & (nonres)
        keys_df.loc[
            (keys_df["coveragetype_id"] == SUPPORTED_COVERAGE_TYPES["bi"]["id"])
            & (keys_df["building_cat"] == "nonres"),
            "mcm_code",
        ] = 0

        # sort out peril types for merge
        keys_df["peril_type"] = keys_df["peril_id"]
        keys_df.loc[
            keys_df["coveragetype_id"] == SUPPORTED_COVERAGE_TYPES["bi"]["id"],
            "peril_type",
        ] = (
            constants.FLUVIAL_ID
            + ";"
            + constants.PLUVIAL_ID
            + ";"
            + constants.COASTAL_ID
        )  # not relevant for BI

        keys_df["mcm_code"] = keys_df["mcm_code"].fillna(-1)
        keys_df["numberofstoreys"] = keys_df["numberofstoreys"].astype("int64")  # type: ignore
        keys_df["floorsoccupied"] = keys_df["floorsoccupied"].astype("int64")  # type: ignore
        keys_df["bipoi_cat"] = keys_df["bipoi_cat"].fillna(-1)

        # assign vulnerability_id
        keys_df = keys_df.merge(right=self.vulnerability_df, how="left")  # type: ignore

        # assign lat_ids and lon_ids
        keys_df = self.get_latlon_ids(keys_df)

        keys_df["vulnerability_id"] = keys_df["vulnerability_id"].fillna(-1)

        # filter the self.at_risk_df to make the merge less expensive
        self.at_risk_df = self.at_risk_df[
            self.at_risk_df["lat_id"].isin(list(keys_df.lat_id.unique()))
        ]
        self.at_risk_df = self.at_risk_df[
            self.at_risk_df["lon_id"].isin(list(keys_df.lon_id.unique()))
        ]

        # assign notatrisk
        temp_df = (
            keys_df.reset_index()
            .merge(right=self.at_risk_df, how="inner")  # type: ignore
            .set_index("index")
        )
        keys_df["at_risk"] = keys_df.index.isin(temp_df.index.tolist())

        # sort keys status
        keys_df.loc[
            (keys_df["vulnerability_id"] != -1)  # type: ignore
            & (keys_df["lat_id"].notna())  # type: ignore
            & (keys_df["lon_id"].notna())  # type: ignore
            & (keys_df["at_risk"]),
            ["status", "message"],
        ] = [
            OASIS_KEYS_STATUS["success"]["id"],
            "",  # type: ignore
        ]

        keys_df.loc[
            (keys_df["vulnerability_id"] != -1)  # type: ignore
            & (keys_df["lat_id"].notna())  # type: ignore
            & (keys_df["lon_id"].notna())  # type: ignore
            & (~keys_df["at_risk"]),
            ["status", "message"],
        ] = [
            OASIS_KEYS_STATUS["notatrisk"]["id"],
            "area-peril and vulnerability valid, but location not exposed to this peril",  # type: ignore
        ]

        keys_df.loc[
            (keys_df["vulnerability_id"] == -1)  # type: ignore
            & (keys_df["lat_id"].notna())  # type: ignore
            & (keys_df["lon_id"].notna()),  # type: ignore
            ["status", "message"],
        ] = [
            OASIS_KEYS_STATUS["fail_v"]["id"],
            "area-peril valid, vulnerability invalid",  # type: ignore
        ]

        keys_df.loc[
            (keys_df["vulnerability_id"] != -1)  # type: ignore
            & (keys_df["lat_id"].isna() | keys_df["lon_id"].isna()),
            ["status", "message"],
        ] = [
            OASIS_KEYS_STATUS["fail_ap"]["id"],
            "vulnerability valid, area-peril invalid",  # type: ignore
        ]

        keys_df.loc[
            (keys_df["vulnerability_id"] == -1)  # type: ignore
            & (keys_df["lat_id"].isna() | keys_df["lon_id"].isna()),
            ["status", "message"],
        ] = [
            OASIS_KEYS_STATUS["fail"]["id"],
            "area-peril and vulnerability invalid",  # type: ignore
        ]

        keys_df.rename(
            columns={
                "coveragetype_id": "coverage_type",
            },
            inplace=True,
        )
        self.format_keys_df(keys_df)
        keys_df = keys_df[
            [
                "loc_id",
                "peril_id",
                "coverage_type",
                "vulnerability_id",
                "status",
                "message",
                "model_data",
                "catchment_id",
                "lat_id",
                "lon_id",
                "area_peril_id",
            ]
        ]
        return keys_df

    # this enables multiprocessing in our keys server, meaning that process_locations() gets run in parallel with
    # each process receiving a chunk of the OED location file which Oasis combines into one keys file afterwards
    process_locations_multiproc = process_locations
