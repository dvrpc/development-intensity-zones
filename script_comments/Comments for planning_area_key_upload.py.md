# Comments for planning_area_key_upload.py

##Note: Because underscores BEFORE LETTERS BUT NOT AFTER THEM are used to italicize words, sentences, paragraphs, etc in Obsidian, this Obsidian document has spaces instead of those ONLY FOR TEXT IN THIS OBSIDIAN DOCUMENT THAT'S OUTSIDE OF CODE LINES

##Script location: typology_experiments.scripts. resources.planning_area_key_upload.py



```planning_area_key[["mcd_id", "county_id_5dig"]] = planning_area_key[["mcd_id", "county_id_5dig"]].astype(str).astype(str)```: Makes mcd_id and county_id_5dig string instead of numeric. FOUND OUT HOW TO CHANGE dtypes OF SPECIFIC FIELDS FROM https://stackoverflow.com/a/36814203 (IN TURN FOUND ON https://stackoverflow.com/questions/36814100/pandas-to-numeric-for-multiple-columns ) AND https://stackoverflow.com/a/22006514 (IN TURN FOUND ON https://stackoverflow.com/questions/22005911/convert-columns-to-string-in-pandas )

```planning_area_key_geo_keys = geo_key[geo_key["geo_type"]=="Planning Areas"][["geo_name", "geo_key"]]```: Subsets geo_key to just contain the geo_key values of the planning areas. IT DOES THIS BY FIRST SUBSETTING geo_key TO ONLY HAVE ROWS WITH A VALUE OF "Planning Areas" FOR THE geo_type COLUMN, THEN KEEPING ONLY THE "geo_name" AND "geo_key" COLUMNS OF THAT SUBSET AND IN THAT COLUMN ORDER

```planning_area_key_geo_keys["geo_name"] = planning_area_key_geo_keys["geo_name"].str.replace("ies", "y", regex=True)```: This and the next command match up the geo_name values of planning_area_key_geo_keys with those of planning_area_key to prepare for their tabular joining. IT DOES THIS BY REPLACING THE "ies"'S WITH "y"'S AND THEN GETS RID OF ANY REMAINING "s"'S. FOUND OUT HOW TO REPLACE VALUES IN A COLUMN FROM https://pandas.pydata.org/docs/reference/api/pandas.Series.str.replace.html

```planning_area_key = pd.merge(planning_area_key, planning_area_key_geo_keys, on="geo_name", how="left")```: Tabular joins planning_area_key_geo_keys to planning_area_key based on the geo_name column. FOUND OUT HOW TO TABULAR JOIN DATAFRAMES FROM https://stackoverflow.com/questions/43297589/merge-two-data-frames-based-on-common-column-values-in-pandas 