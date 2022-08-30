# Comments for geo_key_upload.py

##Note: Because underscores BEFORE LETTERS BUT NOT AFTER THEM are used to italicize words, sentences, paragraphs, etc in Obsidian, this Obsidian document has spaces instead of those ONLY FOR TEXT IN THIS OBSIDIAN DOCUMENT THAT'S OUTSIDE OF CODE LINES

##Script location: typology_experiments.scripts. resources.geo_key_upload.py



```geo_key = pd.DataFrame({"geo_name": ["DVRPC Region", "NJ Counties", "PA Counties", "PA Suburban Counties", "Core Cities", "Developed Communities", "Growing Suburbs", "Rural Areas", "Bucks", "Burlington", "Camden", "Chester", "Delaware", "Gloucester", "Mercer", "Montgomery", "Philadelphia"], "geo_type": list(np.repeat(np.array(["Regional", "Planning Areas", "Counties"]), [4, 4, 9], axis=0))})```: Creates JUST THE geo_name AND geo_type COLUMNS of the table detailing each geo_name value's key (which also shows how to order the geo_name values in all tables structured like this) and type. ALSO, IT SEEMS MUCH SIMPLER TO CREATE geo_key THIS WAY THAN USING SQL TO MANIPULATE  raw.geos_2021. FOUND OUT HOW TO CREATE A PANDAS DATAFRAME FROM SCRATCH FROM https://datatofish.com/create-pandas-dataframe/ , AND FOUND OUT HOW TO REPLICATE STRINGS FROM https://stackoverflow.com/a/12235637 (IN TURN FOUND ON https://stackoverflow.com/questions/12235552/r-function-rep-in-python-replicates-elements-of-a-list-vector )

```geo_key.insert(0, "geo_key", list(range(1, (len(geo_key["geo_name"])+1))))```: Inserts the column showing each geo_name value's key (which is also the order the geo_name values should always be in), and as the first column. I wanted to do this separately from the previous command because this way, the creation of the order column would always be automatic no matter what changes in the geo_name COLUMN. FOUND OUT HOW TO INSERT A COLUMN INTO A PANDAS DATAFRAME FROM https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.insert.html