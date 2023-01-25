"""
This script uploads _resources.thresholds. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT. BUT IF I DO END UP HAVING TO RERUN THIS SCRIPT FOR SOME REASON, REMEMBER TO FIRST DROP CASCADE _resources.thresholds BEFORE RERUNNING THIS
"""

import pandas as pd


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


thresholds = pd.read_csv(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/thresholds.csv"
)  # Brings in the thresholds CSV I madee

thresholds = thresholds.astype(
    {"density_index_thresholds": "float", "proximity_index_thresholds": "float"}
)  # Makes density_index_thresholds and proximity_index_thresholds float

db.import_dataframe(
    thresholds,
    "_resources.thresholds",
    df_import_kwargs={"if_exists": "replace", "index": False},
)  # Exports thresholds
