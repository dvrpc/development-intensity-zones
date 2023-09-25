import db_setup
import load
import json
import os
from dotenv import load_dotenv
import time

start_time = time.time()

load_dotenv()

dbname = r"development_intensity_zones"
schemas = ["source", "output"]
source_schema = "source"
source_path = "./source"
dvrpc_gis_sources = "source/dvrpc_data_sources.json"
sql = "sql/analysis.sql"
target_crs = "EPSG:26918"
census_url = "https://api.census.gov/data/2020/dec/pl"
census_geo = ["block", "block group"]


db_setup.create_database(dbname)

db_setup.create_schemas(dbname, schemas)

db_setup.create_postgis_extension(dbname)

with open(dvrpc_gis_sources, 'r') as config_file:
    urls_config = json.load(config_file)
urls = urls_config['urls']
for url_key, url_value in urls.items():
    load.dvrpc_data(dbname, source_schema, url_value, target_crs)

load.gdb_data(dbname, source_schema, source_path, target_crs)

for geo in census_geo:
    census_params = {
    "get": "P1_001N,P5_001N,H1_001N,H1_002N",
    "for": geo + ":*",
    "in": "state:10,24,34,42&county:*&tract:*",
    "key": os.getenv("CENSUS_KEY")
    }
    load.census_tables(dbname, source_schema, census_url, census_params, geo, ["state", "county", "tract", geo.replace(' ', '_')])

load.csv_tables(dbname, source_schema, source_path)

db_setup.execute_analysis(dbname, sql)

end_time = time.time()
duration = end_time - start_time
print("Script duration: {:.2f} seconds".format(duration))