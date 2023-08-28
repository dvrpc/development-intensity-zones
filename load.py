import os
import psycopg2
import requests
import math
from dotenv import load_dotenv
import geopandas as gpd
from pyproj import CRS
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine
import fiona

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

portal = {
    "username": os.getenv("PORTAL_USERNAME"),
    "password": os.getenv("PORTAL_PASSWORD"),
    "client": os.getenv("PORTAL_CLIENT"),
    "referer": os.getenv("PORTAL_URL"),
    "expiration": int(os.getenv("PORTAL_EXPIRATION")),
    "f": os.getenv("PORTAL_F")
}


def dvrpc_data(dbname, target_schema, url, target_crs):
    """
    Loads the DVRPC data from feature services into database.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    response = requests.post("https://arcgis.dvrpc.org/dvrpc/sharing/rest/generateToken", data=portal)
    response_obj = response.json()
    token = response_obj.get("token")

    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split("/")
    table_name = path_parts[-4].lower()
    base_url = url.split('?')[0]

    count_url = f"{base_url}?where=1=1&returnCountOnly=true&token={token}&f=json"
    count_response = requests.get(count_url)
    total_features = count_response.json().get("count")

    gdf_list = []

    total_chunks = math.ceil(total_features / 5000) # 2000 default esri record limit on feature services

    print(f"Loading {table_name}...\n")

    for chunk in range(total_chunks):
        offset = chunk * 5000
        query_url = f"{url}&resultOffset={offset}&resultRecordCount=5000&token={token}" # 2000 default
        response = requests.get(query_url)
        data = response.json()
        chunk_gdf = gpd.GeoDataFrame.from_features(data['features'])
        # source_crs = CRS.from_string(data['crs']['properties']['name'])
        # chunk_gdf.crs = source_crs
        # chunk_gdf = chunk_gdf.to_crs(target_crs)
        gdf_list.append(chunk_gdf)

    gdf = pd.concat(gdf_list, ignore_index=True)
    gdf.crs = target_crs
    gdf.to_postgis(table_name.lower(), engine, schema=target_schema, if_exists='replace', index=False)


def gdb_data(dbname, target_schema, source_path, target_crs):
    """
    Loads the feature classes from a gdb into database.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    for folder_name in os.listdir(source_path):
        if folder_name.endswith(".gdb"):
            gdb = os.path.join(source_path, folder_name)
            layer_names = fiona.listlayers(gdb)

            for layer_name in layer_names:
                gdf = gpd.read_file(gdb, layer=layer_name)
                gdf = gdf.to_crs(target_crs)

                print(f"Loading {layer_name}...\n")

                gdf.to_postgis(f"{layer_name.lower()}", engine, schema=target_schema, if_exists='replace', index=False)


def census_tables(dbname, target_schema, api_url, census_params, geo, variables_to_keep):
    """
    Streams in census data, turns the streaming results into a data frame, and converts all columns except for some to numeric
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    df = requests.get(api_url, params=census_params).json()
    df = pd.DataFrame(df[1:], columns=df[0])
    df.columns = map(lambda col: col.lower().replace(' ', '_'), df.columns)

    df[df.columns.drop(variables_to_keep)] = df[
        df.columns.drop(variables_to_keep)
    ].apply(
        pd.to_numeric, errors="coerce"
    )  # Converts all of the data frame's columns except for the user-defined list of variables to keep as string from string to numeric at once

    print(f"Loading Census {geo} table...\n")

    df.to_sql(f"tot_pops_and_hhs_2020_{geo.lower().replace(' ', '_')}", con=engine,  schema=target_schema, if_exists='replace', index=False)


def csv_tables(dbname, target_schema, source_path):
    """
    Loads the csv into database.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    for filename in os.listdir(source_path):
        if filename.endswith(".csv"):
            df = pd.read_csv(os.path.join(source_path, filename))
            table_name = os.path.splitext(os.path.basename(filename))[0]

            print(f"Loading {table_name}.csv...\n")
            df.to_sql(table_name, con=engine, schema=target_schema, if_exists='replace', index=False)