"""
This script uploads _resources.bones_thresholds and _resources.bones_classifications
"""

import pandas as pd

import openpyxl


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


bones_thresholds = pd.read_excel(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/bones threshold classifications tables.xlsx",
    sheet_name="bones_thresholds",
)  # Reads in the bones thresholds table

bones_thresholds = bones_thresholds.astype(
    {"density_thresholds": "float", "accessibility_thresholds": "float"}
)  # Makes density_thresholds and accessibility_thresholds float


bones_classifications = pd.read_excel(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/bones threshold classifications tables.xlsx",
    sheet_name="bones_classifications",
)  # Reads in the bones classifications table


tables_to_upload_dictionary = dict(
    zip(["bones_thresholds", "bones_classifications"], [bones_thresholds, bones_classifications])
)  # Creates a dictionary of the tables to upload, with the keys being the table/sheet names and the values being the tables themselves

[
    db.import_dataframe(
        df, f"_resources.{tablename}", df_import_kwargs={"if_exists": "replace", "index": False}
    )
    for tablename, df in tables_to_upload_dictionary.items()
]  # For each table in tables_to_upload_dictionary, exports it to _resources
