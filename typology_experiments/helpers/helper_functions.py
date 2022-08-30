from __future__ import annotations


import pandas as pd

import numpy as np

import requests

from io import BytesIO

import pdfplumber  # FIRST HAD TO RUN "conda activate typology-experiments" AND THEN 'python -m pip install "pdfplumber"' BEFORE RUNNING THIS COMMAND

import re

import gspread  # FIRST HAD TO RUN "conda activate typology-experiments" AND THEN 'python -m pip install "gspread"' BEFORE RUNNING THIS COMMAND

import os

from os import listdir

from datetime import datetime

from pandas import DataFrame

from geopandas import GeoDataFrame


from typology_experiments import Database, DATABASE_URL

db = Database(DATABASE_URL)


def do_something(times: int) -> None:
    for _ in range(times):
        print("I'm doing something")  # Just serves as an example function Aaron Fraint wrote


def duplicate_state_key(new_geo_key_values):

    if "state_key" in locals().keys():
        pass
    else:
        state_key = db.get_dataframe_from_query(
            "SELECT * FROM _resources.state_key"
        )  # Brings in _resources.state_key only if it hasn't already been loaded in earlier in the script

    df = state_key.copy()  # Makes a copy of _resources.state_key

    df[
        "geo_key"
    ] = new_geo_key_values  # Overwrites the geo_key column of the _resources.state_key copy with the user's input

    return df  # Creates a function that creates a new lookup table that's exactly the same as _resources.state_key except for the geo_key column. REMEMBER THAT NO MATTER WHAT,


# _resources.state_key MUST ALREADY BE IN THE DATABASE BEFORE THIS FUNCTION IS RUN


def sort_geo_names_by_year_and_geo_key(df: pd.DataFrame) -> pd.DataFrame:

    if "geo_key" in locals().keys():
        pass
    else:
        geo_key = db.get_dataframe_from_query(
            "SELECT geo_name, geo_key FROM _resources.geo_key"
        )  # Brings in the geo_name and geo_key columns of _resources.geo_key only if _resources.geo_key hasn't already been loaded into the script this function is being used in

    df = pd.merge(
        df, geo_key[["geo_name", "geo_key"]], on="geo_name", how="left"
    )  # Tabular joins _resources.geo_key to the data frame based on the geo_name column

    df = df.sort_values(
        ["year", "geo_key"]
    )  # Sorts the data frame first by the year column and then by the geo_key column

    df = df[
        ["year", "geo_key"] + [list(df.columns)[-2]]
    ]  # Keeps only the columns of the data frame that I want and in the order that I want them in

    return df  # Creates a function that sorts a dataframe, first by its year column and then by its geo_key column. REMEMBER THAT NO MATTER WHAT, _resources.geo_key MUST ALREADY BE IN


# THE DATABASE BEFORE THIS FUNCTION IS RUN


def add_county_grouping_totals_and_sort(df: pd.DataFrame) -> pd.DataFrame:

    if "geo_key" in locals().keys():
        pass
    else:
        geo_key = db.get_dataframe_from_query(
            "SELECT * FROM _resources.geo_key"
        )  # Brings in _resources.geo_key only if _resources.geo_key hasn't already been loaded into the script this function is being used in

    df = pd.merge(
        df, geo_key[["geo_name", "geo_key"]], on="geo_name", how="left"
    )  # Tabular joins _resources.geo_key to the data frame based on the geo_name column

    if "county_key" in locals().keys():
        pass
    else:
        county_key = db.get_dataframe_from_query(
            "SELECT geo_key, county_id_5dig FROM _resources.county_key"
        )  # Brings in the geo_key and county_id_5dig columns of _resources.county_key only if _resources.county_key hasn't already been loaded into the script this function is being
    # used in. AND I'M DOING THIS AS A SEPARATE COMMAND FROM THE ONE THAT IMPORTS _resources.geo_key BECAUSE WHAT IF SAY _resources.geo_key HAS BEEN LOADED INTO THE SCRIPT BUT NOT
    # _resources.county_key, THEN I WOULDN'T WANT TO LOAD IN BOTH AT ONCE, ETC THAT GETS TOO COMPLICATED, LOADING THE TABLES IN SEPARATELY IS MUCH SIMPER THIS WAY

    df = pd.merge(
        df, county_key[["geo_key", "county_id_5dig"]], on="geo_key", how="left"
    )  # Tabular joins _resources.county_key to the data frame based on the geo_key column

    regional_geo_key_county_id_5digs = db.get_dataframe_from_query(
        "SELECT geo_key, county_id_5dig FROM _resources.dvrpc_key UNION SELECT geo_key, county_id_5dig FROM _resources.state_key UNION SELECT geo_key, county_id_5dig FROM _resources.pa_suburban_key"
    )  # Creates a table containing all of the regional geo_key values' county_id_5dig values

    county_grouping_info_keys = list(
        regional_geo_key_county_id_5digs["geo_key"].unique()
    )  # Gets the unique values of regional_geo_key_county_id_5digs's geo_key column, which will be the keys of the eventual county_grouping_info dictionary

    county_grouping_info_values = [
        list(
            regional_geo_key_county_id_5digs[regional_geo_key_county_id_5digs["geo_key"] == i][
                "county_id_5dig"
            ]
        )
        for i in county_grouping_info_keys
    ]  # Gets the county_id_5dig values of the counties that make up those groupings, which will be the values of the eventual county_grouping_info dictionary

    county_grouping_info = dict(
        zip(county_grouping_info_keys, county_grouping_info_values)
    )  # To start getting the values of the county grouping records, creates a dictionary, where the keys are the geo_key values of the county groupings, and the values are the
    # county_id_5dig values of the counties that make up those groupings

    df_county_grouping_totals = [
        pd.DataFrame(
            df[df.county_id_5dig.isin(list(county_grouping_info.values())[i])]
            .groupby(["year"])[[list(df.columns)[-3]]]
            .sum()
        )
        for i in range(len(county_grouping_info))
    ]  # For each year, this starts the process of getting their county grouping totals

    [
        df_county_grouping_totals[i].insert(1, "geo_key", list(county_grouping_info.keys())[i])
        for i in range(len(county_grouping_info))
    ]  # For each year's county grouping total dataframe, this then puts in their respective geo_key columns. ALSO IGNORE THE "[None, None, None, None]" THAT GETS PRINTED HERE, THE
    # COMMAND WORKS

    df_county_grouping_totals = pd.concat(
        df_county_grouping_totals
    )  # Turns the df_county_grouping_totals list of tables into one big table

    df_county_grouping_totals.reset_index(
        level=0, inplace=True
    )  # Turns df_county_grouping_totals' year index into a year column

    df_county_grouping_totals = df_county_grouping_totals[
        ["year", "geo_key"] + [list(df_county_grouping_totals.columns)[1]]
    ]  # Reorders the columns of df_county_grouping_totals to be the order I want them in

    df = pd.concat(
        [df[list(df_county_grouping_totals.columns)], df_county_grouping_totals]
    )  # Merges/row binds/etc the dataframe containing the county grouping rows to to the data frame in question, which only contains the same columns that the dataframe containing
    # the county grouping rows has

    df = df.sort_values(
        ["year", "geo_key"]
    )  # Sorts the data frame in question, first by its year column and then by its geo_key column

    return df  # Creates a function that finds the county grouping totals for a dataframe, merges/row binds those to that dataframe, and then sorts that final dataframe, first by its


# year column and then by its geo_key column. REMEMBER THAT NO MATTER WHAT, _resources.geo_key, _resources.county_key, _resources.dvrpc_key, _resources.state_key AND
# _resources.pa_suburban_key MUST ALREADY BE IN THE DATABASE BEFORE THIS FUNCTION IS RUN


def stream_in_census_data(api_url, list_of_variables_to_keep_as_string):

    df = requests.get(
        api_url
    ).json()  # Streams in the census table the user wants using the user-defined census API

    df = pd.DataFrame(
        df[1:], columns=df[0]
    )  # Turns the data frame that's currently a JSON into a data frame

    df[df.columns.drop(list_of_variables_to_keep_as_string)] = df[
        df.columns.drop(list_of_variables_to_keep_as_string)
    ].apply(
        pd.to_numeric, errors="coerce"
    )  # Converts all of the data frame's columns except for the user-defined list of variables to keep as string from string to numeric at once

    return df  # Creates a function that streams in census data, turns the streaming results into a data frame, and converts all columns except for some to numeric


def get_folder_paths(directory_path, keep_or_remove, filtering_text_pattern):

    folder_paths_list = [
        x[0] for x in os.walk(directory_path)
    ]  # Gets a list of all folder paths within the general directory

    folder_paths_list = [
        re.sub(r"\\", "/", i) for i in folder_paths_list
    ]  # Replaces those "\\"'s with "/"'s

    if keep_or_remove == "keep":
        folder_paths_list = list(
            filter(re.compile(filtering_text_pattern).search, folder_paths_list)
        )  # Just keeps the Updated_Tables folders from the result
    elif keep_or_remove == "remove":
        folder_paths_list = list(
            filter(
                lambda x: not re.compile(filtering_text_pattern).search(x),
                folder_paths_list,
            )
        )  # Removes the folder paths I don't want from the result
    else:
        pass

    return folder_paths_list  # Creates a function that lists all folders within a directory


def get_file_paths(
    folder_paths_list_name,
    keep_text_pattern,
    remove_or_not_and_text_pattern_if_necessary,
):
    file_paths_list = [
        os.listdir(i) for i in folder_paths_list_name
    ]  # For each of those folders, gets the list of file names within them

    file_paths_list = [
        list(filter(re.compile(keep_text_pattern).search, i)) for i in file_paths_list
    ]  # For each of those folders, starts the process of just keeping the names of the folders that contain the tables I want to upload

    file_paths_list = [
        [folder_paths_list_name[i] + "/" + j for j in file_paths_list[i]]
        for i in range(len(folder_paths_list_name))
    ]  # For each of those folders, adds those folders' file paths to the beginnings of each file name within those folders

    file_paths_list = [
        item for sublist in file_paths_list for item in sublist
    ]  # Flattens that list so that it's just 1 big list of table file paths

    if "remove" in remove_or_not_and_text_pattern_if_necessary:
        remove_text_pattern = re.sub(
            "remove ", "", remove_or_not_and_text_pattern_if_necessary
        )  # If the user wants to remove items/strings from the result, extract the text pattern that items/strings they want to remove contains

        file_paths_list = list(
            filter(lambda x: not re.compile(remove_text_pattern).search(x), file_paths_list)
        )  # Removes the file paths the user doesn't want from the result
    elif remove_or_not_and_text_pattern_if_necessary == "no removals":
        pass
    else:
        pass

    return file_paths_list  # Creates a function that lists the paths of all files within a list of directories


def get_last_modified_file_times(file_paths_list_name):
    last_modified_file_times_list = [
        os.path.getmtime(i) for i in file_paths_list_name
    ]  # Gets the last modified times of the files in the file paths list

    last_modified_file_times_list = [
        datetime.fromtimestamp(i).strftime("%m/%d/%Y, %H:%M:%S")
        for i in last_modified_file_times_list
    ]  # Converts the date-time values in the last
    # modified file times list to a more legible format

    return last_modified_file_times_list  # Creates a function that lists the last modified times of all file paths in a list


def create_file_path_tables_to_upload_dictionary(
    file_path_tables_to_upload_info_df_name,
):
    file_path_tables_to_upload_dictionary_keys = list(
        file_path_tables_to_upload_info_df_name["name_for_db"]
    )  # Creates the final list of eventual names of the tables for the database

    file_path_tables_to_upload_file_paths_list = list(
        file_path_tables_to_upload_info_df_name["file_path"]
    )  # Creates the final list of the file paths

    file_path_tables_to_upload_dictionary_values = [
        pd.read_csv(i) for i in file_path_tables_to_upload_file_paths_list
    ]  # Reads in the tables

    file_path_tables_to_upload_dictionary = dict(
        zip(
            file_path_tables_to_upload_dictionary_keys,
            file_path_tables_to_upload_dictionary_values,
        )
    )  # Puts the tables into a dictionary, where their keys are their eventual names for the database and their values are the tables themselves

    return file_path_tables_to_upload_dictionary  # Creates a function that creates a dictionary of tables to upload to the database, where each table is from a file path


def stream_in_google_sheets_table(
    google_sheets_workbook_name,
    name_of_worksheet_containing_table,
    credentials_json_file_path,
):
    from oauth2client.service_account import (
        ServiceAccountCredentials,
    )  # This and the next 2 commands brings in the credits needed to import the Google Sheets table

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        credentials_json_file_path, scope
    )  # Notably shows the path to that JSON file containing my credentials automatically generated by Google when creating my service account

    client = gspread.authorize(creds)

    google_sheets_workbook = client.open(
        google_sheets_workbook_name
    )  # First brings in the whole workbook itself

    google_sheets_worksheet = google_sheets_workbook.worksheet(
        name_of_worksheet_containing_table
    )  # Then brings in the worksheet/tab that contains the table

    google_sheets_table = pd.DataFrame(
        google_sheets_worksheet.get_all_records()
    )  # Then finally brings in the table from that worksheet/tab

    return google_sheets_table  # Creates a function that streams in a table from a Google Sheets workbook. TO ENSURE THIS FUNCTION WORKS, THE USER HAS TO GO INTO THE GOOGLE SHEETS


# WORKBOOK TO SHARE IT AS AN EDITOR WITH THEIR client_email AS SPECIFIED IN THAT JSON THAT CONTAINS THEIR CREDENTIALS


def get_indicator_schemas_tables_field_info(
    new_tables_info_df_name,
    new_or_old_table_name,
    unique_or_all,
    field_name,
    list_of_table_names,
):
    table_name_column_name = (
        new_or_old_table_name + "_table_name"
    )  # First gets the table_name column name

    if unique_or_all == "unique":
        field_info_list = [
            list(
                new_tables_info_df_name[new_tables_info_df_name[table_name_column_name] == i][
                    field_name
                ].unique()
            )
            for i in list_of_table_names
        ]  # For every new or old table name, gets the unique field name (ex. temporal, etc) field info, including blanks for now
    elif unique_or_all == "all":
        field_info_list = [
            list(
                new_tables_info_df_name[new_tables_info_df_name[table_name_column_name] == i][
                    field_name
                ]
            )
            for i in list_of_table_names
        ]  # For every new or old table name, gets all field name (ex. temporal, etc) field info, including blanks for now
    else:
        pass

    if field_name not in [
        "value_field_name_and_dt",
        "temporal_field",
        "old_table_temporal_field",
        "category_entry",
        "geo_key",
        "string_for_query",
        "old_table_name",
        "new_table_name_dict_item",
        "new_table_temporal_field",
        "temporal_field_for_query",
        "temporal_field_for_query_step1",
        "columns_to_order_by",
        "geo_field",
        "category_field",
        "value_field",
        "most_of_query_string",
    ]:
        [
            i.remove(" ") for i in field_info_list if len(i) > 1
        ]  # Excludes the one-space blanks only from tables which have at least 1 field of the type the user chooses, only if it's not certain fields being dealt with
    elif field_name in [
        "temporal_field",
        "old_table_temporal_field",
        "category_entry",
        "geo_key",
        "string_for_query",
        "old_table_name",
        "new_table_name_dict_item",
        "new_table_temporal_field",
        "temporal_field_for_query",
        "temporal_field_for_query_step1",
        "columns_to_order_by",
        "geo_field",
        "category_field",
        "value_field",
        "most_of_query_string",
    ]:
        [
            i.remove("") for i in field_info_list if len(i) > 1 and "" in i
        ]  # Excludes the no-space blanks only from tables which have at least 1 field of the type the user chooses, only if it's certain fields being dealt with, and if there's at
    # least 1 blank in the list within field_info_list
    else:
        pass

    field_info_dictionary = dict(
        zip(list_of_table_names, field_info_list)
    )  # Turns field_info_list into a dictionary, where the keys are the new table names and the values are the lists of the field type (ex. temporal) field name/data type combos

    return field_info_dictionary  # Creates a function that gets the indicator schemas new tables' field info based on the field type the user chooses (ex. temporal, etc)


def bring_in_bridge_owner_types(df: pd.DataFrame, maintenance_column_name) -> pd.DataFrame:

    owner_types = (
        pdfplumber.open(
            BytesIO(requests.get("https://www.fhwa.dot.gov/bridge/mtguide.pdf").content)
        )
        .pages[22]
        .extract_text()
    )  # Scrapes the text from the 23rd page of the NBI data dictionary PDF

    owner_types = re.sub("\n", "", owner_types)  # Gets rid of the "\n"'s from owner_types

    owner_types = re.sub(
        ".*Description            |    Item 22.*", "", owner_types
    )  # Isolates the text that makes up the table

    owner_types = re.split(
        "(\d+)", owner_types
    )  # Splits the one big owner_types string into a list of multiple strings, where the numbers are in their own strings and the text are in their own strings

    owner_types = [
        re.sub("  +", "", i) for i in owner_types
    ]  # Gets rid of all instances of 2 or more spaces from the strings

    owner_types.remove("")  # Drops any empty list elements

    owner_types = pd.DataFrame(
        {maintenance_column_name: owner_types[0::2], "Meaning": owner_types[1::2]}
    )  # Turns owner_types into a data frame, where the owner and maintenance values are the EVEN number-indexed items of the owner_types list, and the meanings are the ODD
    # number-indexed items of that same list

    owner_types["owner_type"] = np.where(
        owner_types[maintenance_column_name].str.contains("01|11|21"),
        "State",
        np.where(
            owner_types[maintenance_column_name].str.contains("02|03|04"),
            "Local",
            "Other",
        ),
    )  # Adds a column which makes 01, 11 and 21 state-owned bridges, 02, 03 and 04 locally-owned bridges, and everything else owned by someone else

    owner_types = owner_types.drop(
        columns=["Meaning"]
    )  # Drops Meaning now that it's no longer needed

    df = pd.merge(
        df, owner_types, on=maintenance_column_name, how="left"
    )  # LEFT joins owner_types to the data frame in question based on the user-inputted maintenance column name, so that way each bridge in it has an owner type

    return df  # Creates a function that makes it so each bridge in a data frame of them has an owner type
