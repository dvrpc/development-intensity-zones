"""
This script uploads _resources.block2020_parent_geos
"""

import pandas as pd

import gspread


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


from typology_experiments.helpers.helper_functions import *  # Imports all functions from helper_functions.py. THIS MUST ALWAYS COME AFTER THE

# "from typology_experiments import Database, DATABASE_URL" AND "db = Database(DATABASE_URL)" COMMANDS IN THAT ORDER


block2020_parent_geos = stream_in_google_sheets_table(
    "block2020_parent_geos",
    "block2020_parent_geos",
    "./typology-experiments-11bfdbe8dac4.json",
)  # Loads in block2020_parent_geos from Google Sheets using my stream_in_google_sheets_table() function. Also, note that the workbook and worksheet names are the same in this case


db.import_dataframe(
    block2020_parent_geos,
    "_resources.block2020_parent_geos",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports block2020_parent_geos
