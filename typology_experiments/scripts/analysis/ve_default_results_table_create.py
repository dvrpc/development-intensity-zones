"""
This script creates a VisionEval default results table to put into analysis based on user input
"""

first_part_of_table_name = (
    "lodes"  # USER DECLARES FIRST PART OF TABLE NAME HERE (WOULD BE EITHER forecast OR lodes)
)


import pandas as pd

import geopandas as gpd

import numpy as np


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


density_and_accessibility_table = db.get_dataframe_from_query(
    "SELECT * FROM analysis." + first_part_of_table_name + "_density_and_accessibility"
)  # Uses my function to bring in the density and accessibility table the user wants to bring in

ve_thresholds = db.get_dataframe_from_query(
    "SELECT * FROM _resources.ve_thresholds"
)  # Uses my function to bring in _resources.ve_thresholds

data_for_area_type_column = db.get_dataframe_from_query(
    "SELECT accessibility_levels as accessibility_level, density_levels as density_level, area_type FROM _resources.ve_classifications"
)  # Uses my function to bring in _resources.ve_classifications, but to use to create the eventual area_type column


density_and_accessibility_table["density_level"] = np.where(
    density_and_accessibility_table["density"]
    < ve_thresholds.loc[ve_thresholds["levels"] == "low", "density_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility_table["density"]
            >= ve_thresholds.loc[ve_thresholds["levels"] == "low", "density_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility_table["density"]
            < ve_thresholds.loc[ve_thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility_table["density"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "moderate", "density_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility_table["density"]
                < ve_thresholds.loc[ve_thresholds["levels"] == "high", "density_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                density_and_accessibility_table["density"]
                >= ve_thresholds.loc[ve_thresholds["levels"] == "high", "density_thresholds"].iloc[
                    0
                ],
                "high",
                "N/A",
            ),
        ),
    ),
)  # Creates a new column called density_level which bins the values of density

density_and_accessibility_table["accessibility_level"] = np.where(
    density_and_accessibility_table["accessibility"]
    < ve_thresholds.loc[ve_thresholds["levels"] == "low", "accessibility_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility_table["accessibility"]
            >= ve_thresholds.loc[ve_thresholds["levels"] == "low", "accessibility_thresholds"].iloc[
                0
            ]
        )
        & (
            density_and_accessibility_table["accessibility"]
            < ve_thresholds.loc[
                ve_thresholds["levels"] == "moderate", "accessibility_thresholds"
            ].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility_table["accessibility"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "moderate", "accessibility_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility_table["accessibility"]
                < ve_thresholds.loc[
                    ve_thresholds["levels"] == "high", "accessibility_thresholds"
                ].iloc[0]
            ),
            "moderate",
            np.where(
                density_and_accessibility_table["accessibility"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "high", "accessibility_thresholds"
                ].iloc[0],
                "high",
                "N/A",
            ),
        ),
    ),
)  # Repeats the process, but for creating the new accessibility_level column instead


ve_default_results_table = pd.merge(
    density_and_accessibility_table,
    data_for_area_type_column,
    on=["accessibility_level", "density_level"],
    how="left",
)  # Left joins data_for_area_type_column to density_and_accessibility_table to create area_type, and stores that result in a new object which contains the VisionEval default results table


db.import_dataframe(
    ve_default_results_table,
    "analysis." + first_part_of_table_name + "_ve_default_results",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed VisionEval default results table to analysis
