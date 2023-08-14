"""
This module contains a class that is capable of 
translating data to and from Python/SQL.

Example:

>>> from development-intensity-zones.database import Database
>>> my_uri ="postgresql://postgres:password@localhost:5432/indego_2021"
>>> db = Database(my_uri)

"""

import pandas as pd
import geopandas as gpd
import sqlalchemy
import psycopg2
from geoalchemy2 import Geometry, WKTElement


def sanitize_df_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean up a dataframe column names so it imports into SQL properly.
    This includes:
        - spaces in column names replaced with underscore
        - all column names are 100% lowercase
        - funky characters are stripped out of column names
    """

    bad_characters = [".", "-", "(", ")", "+", ":", "$"]

    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = [x.lower() for x in df.columns]

    # Remove '.' and '-' from column names.
    # i.e. 'geo.display-label' becomes 'geodisplaylabel'
    for s in bad_characters:
        df.columns = df.columns.str.replace(s, "", regex=False)

    return df


class Database:
    """
    The Database class currently has three methods:

        - query()
        - execute()
        - import_dataframe()

    Add more as you need them!
    """

    def __init__(self, uri: str):
        self.uri = uri

    def query(self, query: str):
        """
        - Use `psycopg2` to run a query and return the result as a list of lists
        - This will NOT commit any changes to the database

        Arguments:
            query (str): any valid SQL query that returns data

        Returns:
            list: with each row returned from the query as its own sub-list
        """

        connection = psycopg2.connect(self.uri)
        cursor = connection.cursor()

        cursor.execute(query)

        result = cursor.fetchall()

        cursor.close()
        connection.close()

        return [list(x) for x in result]

    def execute(self, query: str) -> None:
        """
        - Use psycopg2 to execute a query & commit it to the database

        Arguments:
            query (str): any valid SQL code that changes the database

        Returns:
            None: although the database is updated in-place with whatever is in the query
        """

        connection = psycopg2.connect(self.uri)
        cursor = connection.cursor()

        cursor.execute(query)

        cursor.close()
        connection.commit()
        connection.close()

        return None

    def import_dataframe(
        self, df: pd.DataFrame, new_tablename: str, df_import_kwargs: dict = {}
    ) -> None:
        """
        - Import an in-memory `pandas.DataFrame` into postgres

        Arguments:
            df (pd.DataFrame): data to load into postgres, as an in-memory dataframe
            new_tablename (str): name of the new table in SQL
            df_import_kwargs (dict): a key/value dict with any special arguments needed to write the data to SQL

        Returns:
            creates a new SQL table from the provided dataframe
        """

        # Clean up column names
        df = sanitize_df_for_sql(df)

        # Make sure the schema exists
        if "." in new_tablename:
            schema, tablename = new_tablename.split(".")
            self.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS {schema}
            """
            )
        else:
            schema = "public"
            tablename = new_tablename

        # Write to database
        engine = sqlalchemy.create_engine(self.uri)

        df.to_sql(tablename, engine, schema=schema, **df_import_kwargs)

        engine.dispose()

    def import_geodataframe(self, gdf: gpd.GeoDataFrame, new_tablename: str, schema: str) -> None:
        """
        - Import an in-memory geodataframe into PostGIS, using the specified table and schema names
        - The schema gets created if it does not already exist

        Arguments:
            gdf (gpd.GeoDataFrame): GIS data to import
            new_tablename (str): name of the new table
            schema (str): name of the schema to put the new table into

        Returns:
            None: but creates a new table within the Postgres db
        """

        # Make sure the target schema exists
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # gdf = self.sanitize_df_for_sql(gdf) #Commented out because this command caused the function not to work, it's possible Aaron Fraint's sanitize_df_for_sql() function only works for non-spatial data frames

        epsg_code = int(str(gdf.crs).split(":")[1])

        # Get a list of all geometry types in the dataframe
        geom_types = list(gdf.geometry.geom_type.unique())

        # Make sure that we don't have single-part and multi-part geometries in the same layer
        # Fail early if so. This is something that will mess you up within the database so you
        # need to address it before importing. One workaround is to force an 'explode' of the data
        # which makes all features single-part
        if len(geom_types) > 1:
            print("This table has multiple geometry types:")
            print(f"{geom_types=}")
            print("Update codebase to explode this kind of data")
            return None

        # Use the non-multi version of the geometry
        geom_type_to_use = min(geom_types, key=len).upper()

        # Replace the 'geom' column with 'geometry'
        if "geom" in gdf.columns:
            gdf["geometry"] = gdf["geom"]
            gdf.drop("geom", axis=1, inplace=True)

        # Drop the 'gid' column
        if "gid" in gdf.columns:
            gdf.drop("gid", axis=1, inplace=True)

        # Rename 'uid' to 'old_uid' if it exists in the geodataframe
        if "uid" in gdf.columns:
            gdf[f"old_uid"] = gdf["uid"]
            gdf.drop("uid", axis=1, inplace=True)

        # Build a 'geom' column using geoalchemy2
        # and drop the source 'geometry' column
        gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=epsg_code))
        gdf.drop("geometry", axis=1, inplace=True)

        # Ensure that the target schema exists

        # Write geodataframe to SQL database
        engine = sqlalchemy.create_engine(self.uri)
        gdf.to_sql(
            new_tablename,
            engine,
            schema=schema,
            index=False,
            dtype={"geom": Geometry(geom_type_to_use, srid=epsg_code)},
            if_exists="replace",
        )
        engine.dispose()

        # Make sure the resulting table has a spatial index and unique ID column
        queries = [
            f"""
                CREATE INDEX ON {schema}.{new_tablename}
                USING GIST (geom);
            """,
            f"""
                ALTER TABLE {schema}.{new_tablename}
                ADD uid serial PRIMARY KEY;
            """,
        ]

        for query in queries:
            self.execute(query)

    def get_dataframe_from_query(self, query: str) -> pd.DataFrame:
        """
        - Return a `pandas.Dataframe` from a SQL query
        """

        engine = sqlalchemy.create_engine(self.uri)

        df = pd.read_sql(query, engine)

        engine.dispose()

        return df

    def get_geodataframe_from_query(self, query: str) -> gpd.GeoDataFrame:
        """
        - Return a `geopandas.GeoDataFrame` from a SQL query
        """

        engine = sqlalchemy.create_engine(self.uri)

        gdf = gpd.GeoDataFrame.from_postgis(query, engine)

        engine.dispose()

        return gdf


if __name__ == "__main__":
    db = Database("postgresql://postgres:password@localhost:5432/indego_2021")
