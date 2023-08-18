"""
BEN SAID HOW WE MAY NEED TO REEVALUATE THE NEED FOR _raw.not_in_costar BASED ON ANY NEW ADDITIONS TO COSTAR DATA IN FUTURE DOWNLOADS. WE DOWNLOAD A SNAPSHOT OF COSTAR DATA EVERY 6 MONTHS (AFTER 1ST AND 3RD QUARTER OF EACH YEAR) - This script uploads the feature class, THAT BEN GRUSWITZ MANUALLY CREATED, containing AS EXAMPLES, those 2 really big Costar properties, that one in University City and the GSK campus in Montco, as _raw.not_in_costar (this way, more similar properties can get added to this in the future if need be)
"""

import geopandas as gpd


from development_intensity_zones import Database, DATABASE_URL

db = Database(DATABASE_URL)


not_in_costar = gpd.read_file(
    "U:/_OngoingProjects/CoStar/To be moved later/Transect for AGO/Transect for AGO.gdb",
    layer="not_in_costar",
)  # Reads in the feature class

db.import_geodataframe(
    not_in_costar, "not_in_costar", schema="_raw"
)  # Exports not_in_costar to _raw
