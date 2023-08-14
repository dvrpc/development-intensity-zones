"""
This script populates _raw by bringing in all of the _raw TABLES IN THE typology-experiments DATABASE EXCEPT FOR pos_h2o_diz_zone_0. ALSO, REMEMBER THAT THIS SCRIPT IS ONLY MEANT TO BE RUN JUST ONCE AND THAT'S IT
"""

import geopandas as gpd
import re
import os
import psycopg2
from dotenv import load_dotenv  # This and the next command load in the repository's .env file

load_dotenv()


diz_conn_string = os.getenv("DATABASE_URL")  # Brings in the DIZ DB connection string

te_conn_string = re.sub(
    "development-intensity-zones", "typology-experiments", diz_conn_string
)  # Gets the TE DB connection string simply by replacing the DB name (all other info remains the same)

diz_conn = psycopg2.connect(diz_conn_string)  # This and the next command connect to both DBs

te_conn = psycopg2.connect(te_conn_string)

te_cursor = te_conn.cursor()  # Creates a cursor just for the TE DB, needed to query info from it


te_cursor.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='_raw'"
)  # This and the next command get the names of all tables in the _raw schema of the TE DB without their schema

te_raw_table_names_wo_schema = te_cursor.fetchall()

te_raw_table_names_wo_schema = sorted(
    ["".join(i) for i in te_raw_table_names_wo_schema]
)  # Converts all of the tuples in te_raw_table_names_wo_schema to strings, and alphabetically sorts them

te_raw_table_names_wo_schema = [
    i for i in te_raw_table_names_wo_schema if "pos_h2o_diz_zone_0" not in i
]  # Makes it so _raw.pos_h2o_diz_zone_0 won't get brought in

te_raw_table_col_names = (
    []
)  # First creates an empty list to store the name of each TE DB _raw table's geometry column in

for i in te_raw_table_names_wo_schema:
    te_cursor.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{i}' AND table_schema = '_raw'"
    )  # Then this and the next command get the names of ALL columns within each of those tables, and stores them in that list, where each table's column names is its own sub-list within that
    te_raw_table_col_names += [te_cursor.fetchall()]

te_raw_table_col_names = [
    ["".join(i) for i in te_raw_table_col_names[j]]
    for j in list(range(len(te_raw_table_col_names)))
]  # Turns every column name in every table's sub-list of them into a string

which_tables_are_spatial = [
    [i for i in list(range(len(j))) if j[i] in ["shape", "geom", "geometry"]]
    for j in te_raw_table_col_names
]  # For each table, starts the process of highlighting which ones are spatial by first getting the numerical position of its geometry column, if it has one

which_tables_arent_spatial = [
    i for i in range(len(which_tables_are_spatial)) if which_tables_are_spatial[i] == []
]  # Continues the process of highlighting which tables are spatial by getting the indexes of the table names which are NON-spatial

non_spatial_te_raw_table_names_wo_schema = [
    te_raw_table_names_wo_schema[x] for x in which_tables_arent_spatial
]  # Gets just the names of the NON-spatial tables

spatial_te_raw_table_names_wo_schema = sorted(
    list(set(te_raw_table_names_wo_schema) - set(non_spatial_te_raw_table_names_wo_schema))
)  # Gets the SPATIAL tables by getting the ones that are in the list of all of them but not the list of non-spatial ones, while keeping the tables' order so errors don't come up later

te_raw_table_geom_col_names = [
    s
    for l in [[i for i in j if i in ["shape", "geom", "geometry"]] for j in te_raw_table_col_names]
    for s in l
]  # Gets just the geometry column names (of the SPATIAL tables)


te_raw_spatial_tables = gpd.read_postgis(
    f"SELECT * FROM _raw.{spatial_te_raw_table_names_wo_schema[0]}",
    te_conn,
    geom_col=te_raw_table_geom_col_names[0],
)


diz_conn.close()  # This and the next 2 commands close the connections to both DBs now that I don't need to be connected to them anymore

te_conn.close()

te_cursor.close()
