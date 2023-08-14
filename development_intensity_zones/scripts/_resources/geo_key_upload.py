"""
This script creates _resources.geo_key
"""

import pandas as pd
import numpy as np


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


geo_key = pd.DataFrame(
    {
        "geo_name": [
            "DVRPC Region",
            "NJ Counties",
            "PA Counties",
            "PA Suburban Counties",
            "Core Cities",
            "Developed Communities",
            "Growing Suburbs",
            "Rural Areas",
            "Bucks",
            "Burlington",
            "Camden",
            "Chester",
            "Delaware",
            "Gloucester",
            "Mercer",
            "Montgomery",
            "Philadelphia",
        ],
        "geo_type": list(
            np.repeat(np.array(["Regional", "Planning Areas", "Counties"]), [4, 4, 9], axis=0)
        ),
    }
)  # Creates just the geo_name and geo_type columns of the table detailing each geo_name value's key and type. ALSO, IT SEEMS MUCH SIMPLER TO CREATE geo_key THIS WAY THAN USING SQL TO
# MANIPULATE _raw.geos_2021

geo_key.insert(
    0, "geo_key", list(range(1, (len(geo_key["geo_name"]) + 1)))
)  # Inserts the column showing each geo_name value's key


db.import_dataframe(
    geo_key, f"_resources.geo_key", df_import_kwargs={"if_exists": "replace", "index": False}
)  # Imports _resources.geo_key
