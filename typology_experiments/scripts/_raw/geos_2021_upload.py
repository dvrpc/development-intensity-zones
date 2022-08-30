"""
This script uploads the 2021 geos table into _raw
"""


import pandas as pd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


geos_2021 = pd.read_csv(
    "G:/Shared drives/Socioeconomic and Land Use Analytics/Tracking Progress/Data Preparation/Updates/geos_table.csv"
)  # Brings in the 2021 geos table


db.import_dataframe(
    geos_2021, "_raw.geos_2021", df_import_kwargs={"if_exists": "replace", "index": False}
)  # Exports the 2021 geos table to _raw
