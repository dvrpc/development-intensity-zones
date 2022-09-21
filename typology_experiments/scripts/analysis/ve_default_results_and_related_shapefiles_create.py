"""
This script creates analysis.ve_default_results, analysis.block_groups_dvrpc_2020_with_ve_default_results, and analysis.block_group_land_by_developability_with_ve_default_results, and separately exports analysis.block_groups_dvrpc_2020_with_ve_default_results and analysis.block_group_land_by_developability_with_ve_default_results as shapefiles
"""

import pandas as pd

import geopandas as gpd

import numpy as np


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


density_and_accessibility = db.get_dataframe_from_query(
    "SELECT * FROM analysis.density_and_accessibility"
)  # Uses my function to bring in analysis.density_and_accessibility

ve_thresholds = db.get_dataframe_from_query(
    "SELECT * FROM _resources.ve_thresholds"
)  # Uses my function to bring in _resources.ve_thresholds

data_for_area_type_column = db.get_dataframe_from_query(
    "SELECT accessibility_levels as accessibility_level, density_levels as density_level, area_type FROM _resources.ve_classifications"
)  # Uses my function to bring in _resources.ve_classifications, but to use to create the eventual area_type column

data_for_alt_area_type_column = db.get_dataframe_from_query(
    "SELECT accessibility_levels as accessibility_level, density_levels as alt_density_level, area_type as alt_area_type FROM _resources.ve_classifications"
)  # Uses my function to bring in _resources.ve_classifications, but to use to create the eventual alt_area_type column

block_groups_dvrpc_2020 = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_groups_dvrpc_2020"
)  # Uses my function to bring in the analysis.block_groups_dvrpc_2020 shapefile

block_group_land_by_developability = db.get_geodataframe_from_query(
    "SELECT * FROM analysis.block_group_land_by_developability"
)  # Uses my function to bring in the analysis.block_group_land_by_developability shapefile


density_and_accessibility["density_level"] = np.where(
    density_and_accessibility["density"]
    < ve_thresholds.loc[ve_thresholds["levels"] == "low", "density_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility["density"]
            >= ve_thresholds.loc[ve_thresholds["levels"] == "low", "density_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility["density"]
            < ve_thresholds.loc[ve_thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility["density"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "moderate", "density_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility["density"]
                < ve_thresholds.loc[ve_thresholds["levels"] == "high", "density_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                density_and_accessibility["density"]
                >= ve_thresholds.loc[ve_thresholds["levels"] == "high", "density_thresholds"].iloc[
                    0
                ],
                "high",
                "N/A",
            ),
        ),
    ),
)  # Creates a new column called density_level which bins the values of density

density_and_accessibility["alt_density_level"] = np.where(
    density_and_accessibility["alt_density"]
    < ve_thresholds.loc[ve_thresholds["levels"] == "low", "density_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility["alt_density"]
            >= ve_thresholds.loc[ve_thresholds["levels"] == "low", "density_thresholds"].iloc[0]
        )
        & (
            density_and_accessibility["alt_density"]
            < ve_thresholds.loc[ve_thresholds["levels"] == "moderate", "density_thresholds"].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility["alt_density"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "moderate", "density_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility["alt_density"]
                < ve_thresholds.loc[ve_thresholds["levels"] == "high", "density_thresholds"].iloc[0]
            ),
            "moderate",
            np.where(
                density_and_accessibility["alt_density"]
                >= ve_thresholds.loc[ve_thresholds["levels"] == "high", "density_thresholds"].iloc[
                    0
                ],
                "high",
                "N/A",
            ),
        ),
    ),
)  # Repeats the process, but for creating the new alt_density_level column instead

density_and_accessibility["accessibility_level"] = np.where(
    density_and_accessibility["accessibility"]
    < ve_thresholds.loc[ve_thresholds["levels"] == "low", "accessibility_thresholds"].iloc[0],
    "very low",
    np.where(
        (
            density_and_accessibility["accessibility"]
            >= ve_thresholds.loc[ve_thresholds["levels"] == "low", "accessibility_thresholds"].iloc[
                0
            ]
        )
        & (
            density_and_accessibility["accessibility"]
            < ve_thresholds.loc[
                ve_thresholds["levels"] == "moderate", "accessibility_thresholds"
            ].iloc[0]
        ),
        "low",
        np.where(
            (
                density_and_accessibility["accessibility"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "moderate", "accessibility_thresholds"
                ].iloc[0]
            )
            & (
                density_and_accessibility["accessibility"]
                < ve_thresholds.loc[
                    ve_thresholds["levels"] == "high", "accessibility_thresholds"
                ].iloc[0]
            ),
            "moderate",
            np.where(
                density_and_accessibility["accessibility"]
                >= ve_thresholds.loc[
                    ve_thresholds["levels"] == "high", "accessibility_thresholds"
                ].iloc[0],
                "high",
                "N/A",
            ),
        ),
    ),
)  # Repeats the process, but for creating the new accessibility_level column instead


ve_default_results = pd.merge(
    density_and_accessibility,
    data_for_area_type_column,
    on=["accessibility_level", "density_level"],
    how="left",
)  # Left joins data_for_area_type_column to density_and_accessibility to create area_type, and stores that result in a new object called ve_default_results

ve_default_results = pd.merge(
    ve_default_results,
    data_for_alt_area_type_column,
    on=["accessibility_level", "alt_density_level"],
    how="left",
)  # Left joins data_for_area_type_column to ve_default_results to both create alt_area_type and finish the creation of analysis.ve_default_results


block_groups_dvrpc_2020_with_ve_default_results = block_groups_dvrpc_2020.merge(
    ve_default_results.rename(columns={"block_group20": "GEOID"}), on=["GEOID"], how="left"
)  # Left joins ve_default_results to block_groups_dvrpc_2020 to create analysis.block_groups_dvrpc_2020_with_ve_default_results

block_groups_dvrpc_2020_with_ve_default_results = block_groups_dvrpc_2020_with_ve_default_results[
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
        "alt_density",
        "accessibility",
        "density_level",
        "alt_density_level",
        "accessibility_level",
        "area_type",
        "alt_area_type",
        "geom",
    ]
]  # Reorders the columns

block_group_land_by_developability_with_ve_default_results = block_group_land_by_developability.merge(
    ve_default_results.rename(columns={"block_group20": "GEOID"}), on=["GEOID"], how="left"
)  # Left joins ve_default_results to block_group_land_by_developability to create analysis.block_group_land_by_developability_with_ve_default_results

block_group_land_by_developability_with_ve_default_results = (
    block_group_land_by_developability_with_ve_default_results[
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
            "alt_density",
            "accessibility",
            "density_level",
            "alt_density_level",
            "accessibility_level",
            "area_type",
            "alt_area_type",
            "geom",
            "uid",
        ]
    ]
)  # Reorders the columns


db.import_dataframe(
    ve_default_results,
    "analysis.ve_default_results",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports the completed analysis.ve_default_results object/table as analysis.ve_default_results

block_groups_dvrpc_2020_with_ve_default_results.to_file(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/Shapes/block_groups_dvrpc_2020_with_ve_default_results.shp"
)  # Exports the completed analysis.block_groups_dvrpc_2020_with_ve_default_results object/shapefile as a regular shapefile on the G drive

block_group_land_by_developability_with_ve_default_results.to_file(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/Shapes/block_group_land_by_developability_with_ve_default_results.shp"
)  # Exports the completed analysis.block_group_land_by_developability_with_ve_default_results object/shapefile as a regular shapefile on the G drive

db.import_geodataframe(
    block_groups_dvrpc_2020_with_ve_default_results,
    "block_groups_dvrpc_2020_with_ve_default_results",
    schema="analysis",
)  # Uploads the completed analysis.block_groups_dvrpc_2020_with_ve_default_results object/shapefile as analysis.block_groups_dvrpc_2020_with_ve_default_results. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc

db.import_geodataframe(
    block_group_land_by_developability_with_ve_default_results,
    "block_group_land_by_developability_with_ve_default_results",
    schema="analysis",
)  # Uploads the completed analysis.block_group_land_by_developability_with_ve_default_results object/shapefile as analysis.block_group_land_by_developability_with_ve_default_results. And ignore the warning "Geometry column does not contain geometry" that comes up here, as it seems to load in to the database just fine, etc
