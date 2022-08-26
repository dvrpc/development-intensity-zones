"""
This script creates _resources.planning_area_key
"""

import pandas as pd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


geo_key = db.get_dataframe_from_query(
    "SELECT * FROM _resources.geo_key"
)  # Brings in _resources.geo_key


planning_area_key = db.get_dataframe_from_query(
    "SELECT planning_area AS geo_name, mcd_id, mcd, county_id AS county_id_5dig FROM _raw.geos_2021 WHERE mcd != 'Princeton Township'"
)  # Notably also drops the Princeton Township record (Ben Gruswitz said to hold off on dropping the Pine Valley Borough record for now), and renames planning_area to geo_name and county_id to county_id_5dig right before loading into Python

planning_area_key[["mcd_id", "county_id_5dig"]] = planning_area_key[
    ["mcd_id", "county_id_5dig"]
].astype(
    str
)  # Makes mcd_id and county_id_5dig string


planning_area_key_geo_keys = geo_key[geo_key["geo_type"] == "Planning Areas"][
    ["geo_name", "geo_key"]
]  # Subsets geo_key to just contain the geo_key values of the planning areas

planning_area_key_geo_keys["geo_name"] = planning_area_key_geo_keys["geo_name"].str.replace(
    "ies", "y", regex=True
)  # This and the next command match up the geo_name values of planning_area_key_geo_keys with those of planning_area_key to prepare for their tabular joining

planning_area_key_geo_keys["geo_name"] = planning_area_key_geo_keys["geo_name"].str.replace(
    "s", "", regex=True
)

planning_area_key = pd.merge(
    planning_area_key, planning_area_key_geo_keys, on="geo_name", how="left"
)  # Tabular joins planning_area_key_geo_keys to planning_area_key based on the geo_name column

planning_area_key = planning_area_key[
    ["geo_key", "county_id_5dig", "mcd_id", "mcd"]
]  # Only keeps the columns I want from planning_area_key and in the order I want them in


planning_area_key = pd.concat(
    [
        planning_area_key,
        pd.read_csv(
            "G:/Shared drives/Socioeconomic and Land Use Analytics/Tracking Progress/Data Preparation/postgres_db/phi_cpas_for_planning_areas.csv"
        ),
    ]
)  # Adds the new records to planning_area_key


db.import_dataframe(
    planning_area_key,
    f"_resources.planning_area_key",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Imports _resources.planning_area_key
