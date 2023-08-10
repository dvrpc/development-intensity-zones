"""
This script uploads _resources.thresholds. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT
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

thresholds = thresholds.sort_values(
    by=["level_code"], ascending=[True]
)  # Sorts thresholds by level_code


db.execute(
    "TRUNCATE _resources.thresholds"
)  # First wipes out all of the records in _resources.thresholds

db.import_dataframe(
    thresholds,
    "_resources.thresholds",
    df_import_kwargs={"if_exists": "append", "index": False},
)  # Then repopulates _resources.thresholds with the new records
