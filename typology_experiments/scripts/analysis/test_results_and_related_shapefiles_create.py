"""
This script creates analysis.test_results, analysis.block_groups_dvrpc_2020_with_test_results, and analysis.block_group_land_by_developability_with_test_results
"""

import pandas as pd

import geopandas as gpd

import numpy as np


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


density_and_accessibility = db.get_dataframe_from_query(
    "SELECT * FROM analysis.density_and_accessibility"
)  # Uses my function to bring in analysis.density_and_accessibility

thresholds = db.get_dataframe_from_query(
    "SELECT * FROM _resources.test_thresholds"
)  # Uses my function to bring in _resources.test_thresholds

data_for_area_type_column = db.get_dataframe_from_query(
    "SELECT accessibility_levels as accessibility_level, density_levels as density_level, area_type FROM _resources.test_classifications"
)  # Uses my function to bring in _resources.test_classifications, but to use to create the eventual area_type column

block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_dvrpc_2020"
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile

block_group_land_by_developability = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_group_land_by_developability"
)  # Uses my function to bring in the analysis.block_group_land_by_developability shapefile


density_and_accessibility["density_level"] = np.where(
    density_and_accessibility["density"]
    < thresholds.loc[thresholds["levels"] == "low", "density_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility["density"]
            >= thresholds.loc[thresholds["levels"] == "low", "density_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility["density"]
            < thresholds.loc[thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility["density"]
                >= thresholds.loc[thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
            )
            & (
                density_and_accessibility["density"]
                < thresholds.loc[thresholds["levels"] == "high", "density_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                (
                    density_and_accessibility["density"]
                    >= thresholds.loc[thresholds["levels"] == "high", "density_thresholds"].iloc[0]
                )
                & (
                    density_and_accessibility["density"]
                    < thresholds.loc[
                        thresholds["levels"] == "very high", "density_thresholds"
                    ].iloc[0]
                ),
                "high",
                np.where(
                    density_and_accessibility["density"]
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

density_and_accessibility["accessibility_level"] = np.where(
    density_and_accessibility["accessibility"]
    < thresholds.loc[thresholds["levels"] == "low", "accessibility_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility["accessibility"]
            >= thresholds.loc[thresholds["levels"] == "low", "accessibility_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility["accessibility"]
            < thresholds.loc[thresholds["levels"] == "moderate", "accessibility_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility["accessibility"]
                >= thresholds.loc[
                    thresholds["levels"] == "moderate", "accessibility_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility["accessibility"]
                < thresholds.loc[thresholds["levels"] == "high", "accessibility_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                (
                    density_and_accessibility["accessibility"]
                    >= thresholds.loc[
                        thresholds["levels"] == "high", "accessibility_thresholds"
                    ].iloc[0]
                )
                & (
                    density_and_accessibility["accessibility"]
                    < thresholds.loc[
                        thresholds["levels"] == "very high", "accessibility_thresholds"
                    ].iloc[0]
                ),
                "high",
                np.where(
                    density_and_accessibility["accessibility"]
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


test_results = pd.merge(
    density_and_accessibility,
    data_for_area_type_column,
    on=["accessibility_level", "density_level"],
    how="left",
)  # Left joins data_for_area_type_column to density_and_accessibility to create area_type, and stores that result in a new object called test_results


block_groups_dvrpc_2020_with_test_results = block_groups_dvrpc_2020.merge(
    test_results.rename(columns={"block_group20": "GEOID"}), on=["GEOID"], how="left"
)  # Left joins test_results to block_groups_dvrpc_2020 to create analysis.block_groups_dvrpc_2020_with_test_results

block_groups_dvrpc_2020_with_test_results = block_groups_dvrpc_2020_with_test_results[
    [
        "STATEFP",
        "COUNTYFP",
        "TRACTCE",
        "BLKGRPCE",
        "GEOID",
        "NAMELSAD",
        "ALAND",
        "AWATER",
        "density",
        "accessibility",
        "density_level",
        "accessibility_level",
        "area_type",
        "geom",
    ]
]  # Reorders the columns

block_group_land_by_developability_with_test_results = block_group_land_by_developability.merge(
    test_results.rename(columns={"block_group20": "GEOID"}), on=["GEOID"], how="left"
)  # Left joins test_results to block_group_land_by_developability to create analysis.block_group_land_by_developability_with_test_results

block_group_land_by_developability_with_test_results = (
    block_group_land_by_developability_with_test_results[
        [
            "STATEFP",
            "COUNTYFP",
            "TRACTCE",
            "BLKGRPCE",
            "GEOID",
            "NAMELSAD",
            "ALAND",
            "AWATER",
            "developability",
            "density",
            "accessibility",
            "density_level",
            "accessibility_level",
            "area_type",
            "geom",
            "uid",
        ]
    ]
)  # Reorders the columns


db.import_dataframe(
    test_results,
    "analysis.test_results",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed analysis.test_results object/table as analysis.test_results

db.import_geodataframe(
    block_groups_dvrpc_2020_with_test_results,
    "block_groups_dvrpc_2020_with_test_results",
    schema="analysis",
)  # Uploads the completed analysis.block_groups_dvrpc_2020_with_test_results object/shapefile as analysis.block_groups_dvrpc_2020_with_test_results. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc

db.import_geodataframe(
    block_group_land_by_developability_with_test_results,
    "block_group_land_by_developability_with_test_results",
    schema="analysis",
)  # Uploads the completed analysis.block_group_land_by_developability_with_test_results object/shapefile as analysis.block_group_land_by_developability_with_test_results. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
