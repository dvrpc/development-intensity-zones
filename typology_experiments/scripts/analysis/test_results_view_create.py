"""
This script creates a test results view to put into analysis based on user input
"""

first_part_of_table_name = (
    "lodes"  # USER DECLARES FIRST PART OF TABLE NAME HERE (WOULD BE EITHER forecast OR lodes)
)


import pandas as pd

import geopandas as gpd

import numpy as np

import re


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


density_and_accessibility_table = db.get_dataframe_from_query(
    "SELECT * FROM analysis." + first_part_of_table_name + "_density_and_accessibility"
)  # Uses my function to bring in the density and accessibility table the user wants to bring in

thresholds = db.get_dataframe_from_query(
    "SELECT * FROM _resources.test_thresholds"
)  # Uses my function to bring in _resources.test_thresholds

data_for_area_type_column = db.get_dataframe_from_query(
    "SELECT accessibility_levels as accessibility_level, density_levels as density_level, area_type FROM _resources.test_classifications"
)  # Uses my function to bring in _resources.test_classifications, but to use to create the eventual area_type column

block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    'SELECT "GEOID" AS block_group20, geom FROM analysis.block_groups_dvrpc_2020'
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile


density_and_accessibility_table["density_level"] = np.where(
    density_and_accessibility_table["density"]
    < thresholds.loc[thresholds["levels"] == "low", "density_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility_table["density"]
            >= thresholds.loc[thresholds["levels"] == "low", "density_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility_table["density"]
            < thresholds.loc[thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility_table["density"]
                >= thresholds.loc[thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
            )
            & (
                density_and_accessibility_table["density"]
                < thresholds.loc[thresholds["levels"] == "high", "density_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                (
                    density_and_accessibility_table["density"]
                    >= thresholds.loc[thresholds["levels"] == "high", "density_thresholds"].iloc[0]
                )
                & (
                    density_and_accessibility_table["density"]
                    < thresholds.loc[
                        thresholds["levels"] == "very high", "density_thresholds"
                    ].iloc[0]
                ),
                "high",
                np.where(
                    density_and_accessibility_table["density"]
                    >= thresholds.loc[
                        thresholds["levels"] == "very high", "density_thresholds"
                    ].iloc[0],
                    "very high",
                    "N/A",
                ),
            ),
        ),
    ),
)  # Creates a new column called density_level which bins the values of density

density_and_accessibility_table["accessibility_level"] = np.where(
    density_and_accessibility_table["accessibility"]
    < thresholds.loc[thresholds["levels"] == "low", "accessibility_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility_table["accessibility"]
            >= thresholds.loc[thresholds["levels"] == "low", "accessibility_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility_table["accessibility"]
            < thresholds.loc[thresholds["levels"] == "moderate", "accessibility_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility_table["accessibility"]
                >= thresholds.loc[
                    thresholds["levels"] == "moderate", "accessibility_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility_table["accessibility"]
                < thresholds.loc[thresholds["levels"] == "high", "accessibility_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                (
                    density_and_accessibility_table["accessibility"]
                    >= thresholds.loc[
                        thresholds["levels"] == "high", "accessibility_thresholds"
                    ].iloc[0]
                )
                & (
                    density_and_accessibility_table["accessibility"]
                    < thresholds.loc[
                        thresholds["levels"] == "very high", "accessibility_thresholds"
                    ].iloc[0]
                ),
                "high",
                np.where(
                    density_and_accessibility_table["accessibility"]
                    >= thresholds.loc[
                        thresholds["levels"] == "very high", "accessibility_thresholds"
                    ].iloc[0],
                    "very high",
                    "N/A",
                ),
            ),
        ),
    ),
)  # Repeats the process, but for creating the new accessibility_level column instead


test_results_nonspatial = pd.merge(
    density_and_accessibility_table,
    data_for_area_type_column,
    on=["accessibility_level", "density_level"],
    how="left",
)  # Left joins data_for_area_type_column to density_and_accessibility_table to create area_type, and stores that result in a new object which contains a non-spatial test results table

test_results_view = block_groups_dvrpc_2020.merge(
    test_results_nonspatial, on=["block_group20"], how="left"
)  # Left joins test_results_nonspatial to block_groups_dvrpc_2020 to create the final test results view (which is spatial)

test_results_view = test_results_view[
    [
        "block_group20",
        "density",
        "accessibility",
        "density_level",
        "accessibility_level",
        "area_type",
        "geom",
    ]
]  # Reorders the columns


db.execute(
    "DROP VIEW IF EXISTS analysis." + first_part_of_table_name + "_test_results"
)  # First drops the view if it already exists

db.import_geodataframe(
    test_results_view,
    first_part_of_table_name + "_test_results_for_view_creation",
    schema="_test",
)  # Then uploads test_results_view at this point as a shapefile in _test (HAS TO BE NAMED ACCORDING TO THE VALUE OF first_part_of_table_name IN ORDER FOR THIS SCRIPT TO WORK). And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc

db.execute(
    "CREATE VIEW analysis."
    + first_part_of_table_name
    + "_test_results AS SELECT block_group20, density, accessibility, density_level, accessibility_level, area_type, geom FROM _test."
    + first_part_of_table_name
    + "_test_results_for_view_creation"
)  # Then generates the test results view
