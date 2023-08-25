import os
import psycopg2
import requests
from dotenv import load_dotenv
import geopandas as gpd
from pyproj import CRS
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

def create_database(dbname):
    """
    Creates a PostgreSQL database with the given name.
    """
    pgconn = psycopg2.connect(
        host=host, port=port, database=database, user=user, password=password
    )
    pgconn.autocommit = True
    cur = pgconn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {dbname};")    
    cur.execute(f"CREATE DATABASE {dbname};")
    cur.close()
    pgconn.close()


def create_schemas(dbname, schemas):
    """
    Creates PostgreSQL schemas with the given names in the database.
    """
    cmpconn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = cmpconn.cursor()
    cmpconn.autocommit = True

    for schema in schemas:
        cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname='{schema}'")
        schema_exists = bool(cur.rowcount)
        if not schema_exists:
            cur.execute(f"CREATE SCHEMA {schema};")
    cur.close()
    cmpconn.close()


def create_postgis_extension(dbname):
    """
    Creates the POSTGIS extension in the database.
    """
    cmpconn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = cmpconn.cursor()
    cmpconn.autocommit = True

    cur.execute("SELECT 1 FROM pg_extension WHERE extname='postgis'")
    postgis_extension_exists = bool(cur.rowcount)

    if not postgis_extension_exists:
        cur.execute("CREATE EXTENSION POSTGIS;")
    cur.close()
    cmpconn.close()
