"""
This script uploads _resources.ve_thresholds and _resources.ve_classifications
"""

import pandas as pd

import openpyxl


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


tables_to_upload_dictionary = pd.read_excel(
    "G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/typology_experiments/ve threshold classifications tables.xlsx",
    sheet_name=None,
    index_col=0,
)  # Reads in all tables from that Excel workbook as a dictionary, with the keys being the table/sheet names and the values being the tables themselves, and the first column is always the index FOR NOW

tables_to_upload_dictionary = {
    k: v.reset_index(level=0) for (k, v) in tables_to_upload_dictionary.items()
}  # For each table in tables_to_upload_dictionary, makes the index the first column of it, and resets the index

[
    db.import_dataframe(
        df, f"_resources.{tablename}", df_import_kwargs={"if_exists": "replace", "index": False}
    )
    for tablename, df in tables_to_upload_dictionary.items()
]  # For each table in tables_to_upload_dictionary, exports it to _resources
