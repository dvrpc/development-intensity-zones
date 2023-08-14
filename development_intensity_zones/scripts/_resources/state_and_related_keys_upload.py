"""
This script creates _resources.state_key, _resources.dvrpc_key, _resources.county_key, and _resources.pa_suburban_key
"""

import pandas as pd
import numpy as np


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


from development_intensity_zones.helpers.helper_functions import *  # Imports all functions from helper_functions.py. THIS MUST ALWAYS COME AFTER THE "from development_intensity_zones import Database, DATABASE_URL" AND "db = Database(DATABASE_URL)" COMMANDS IN THAT ORDER


state_key = db.get_dataframe_from_query(
    "SELECT DISTINCT state_id, county_id AS county_id_5dig FROM _raw.geos_2021 ORDER BY state_id, county_id_5dig"
)  # Notably also sorts the data frame, first by state_id and then by county_id_5dig, and renames county_id to county_id_5dig upon loading into Python

state_key = state_key.astype(str)  # Makes all columns in state_key string

state_key.insert(
    1, "county_id_3dig", state_key["county_id_5dig"].str[-3:]
)  # Adds a column to state_key called county_id_3dig which extracts the last 3 digits of the county_id_5dig values, and puts it to the left of county_id_5dig


state_key.insert(
    0, "geo_key", np.where(state_key["state_id"] == "34", 2, 3)
)  # Adds a geo_key column to state_key, which equals 2 when state_id equals 34, otherwise (when state_id equals 42), geo_key = 3, and makes it the 1st column


dvrpc_key = duplicate_state_key(
    1
)  # Uses my function to create dvrpc_key by first making a copy of state_key and then making its geo_key column just equal 1


"""
NOTE THIS HAS SINCE BEEN MANUALLY REPLACED BY G:\Shared drives\Socioeconomic and Land Use Analytics\Tracking Progress\Data Preparation\postgres_db\county_key_replacement.gsheet
county_key = duplicate_state_key(
    list(range(9, 18))
)  # Uses my function to create county_key by first making a copy of state_key and then making its geo_key column equal 9-18
"""


pa_suburban_key = state_key[
    (state_key["state_id"] == "42") & (state_key["county_id_3dig"] != "101")
][
    ["state_id", "county_id_3dig", "county_id_5dig"]
]  # Starts creating pa_suburban_key by first making a subset of state_key where state_id equals 42 and county_id_3dig doesn't equal 101, and leaving out its geo_key column

pa_suburban_key.insert(0, "geo_key", 4)  # Adds a geo_key column to county_key, which is just 4


names_of_tables_to_upload = [
    "state_key",
    "dvrpc_key",
    "county_key",
    "pa_suburban_key",
]  # Creates a list containing the names of the objects/tables to upload

tables_to_upload_dictionary = {
    k: v for k, v in locals().items() if k in names_of_tables_to_upload
}  # Creates a dictionary of just the tables to upload


[
    db.import_dataframe(
        df, f"_resources.{tablename}", df_import_kwargs={"if_exists": "replace", "index": False}
    )
    for tablename, df in tables_to_upload_dictionary.items()
]  # Loads all the tables into the _resources schema of the database
