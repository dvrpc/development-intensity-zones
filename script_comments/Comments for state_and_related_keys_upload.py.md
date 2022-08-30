# Comments for state_and_related_keys_upload.py

##Note: Because underscores BEFORE LETTERS BUT NOT AFTER THEM are used to italicize words, sentences, paragraphs, etc in Obsidian, this Obsidian document has spaces instead of those ONLY FOR TEXT IN THIS OBSIDIAN DOCUMENT THAT'S OUTSIDE OF CODE LINES

##Script location: typology_experiments.scripts. resources.state_and_related_keys_upload.py



```state_key = db.get_dataframe_from_query("SELECT DISTINCT state_id, county_id AS county_id_5dig FROM _raw.geos_2021 ORDER BY state_id, county_id_5dig")```:  Notably also sorts the data frame, first by state_id and then by county_id_5dig, and renames county_id to county_id_5dig upon loading into Python. FOUND OUT HOW TO SORT A DATA FRAME USING SQL FROM https://www.postgresqltutorial.com/postgresql-order-by/ 

```state_key = state_key.astype(str)```:  Makes all columns in state_key string. FOUND OUT HOW TO MAKE ALL COLUMNS IN A DATA FRAME STRING FROM https://stackoverflow.com/a/53321008 (IN TURN FOUND ON https://stackoverflow.com/questions/22005911/convert-columns-to-string-in-pandas )

```state_key.insert(1, "county_id_3dig", state_key["county_id_5dig"].str[-3:])```:  Adds a column to state_key called county_id_3dig which extracts the last 3 digits of the county_id_5dig values, and puts it to the left of county_id_5dig. FOUND OUT HOW TO GET THE LAST N CHARACTERS OF A STRING FROM https://www.tutorialspoint.com/How-can-I-get-last-4-characters-of-a-string-in-Python 

```state_key.insert(0, "geo_key", np.where(state_key["state_id"]=="34", 2, 3))```:  Adds a geo_key column to state_key, which equals 2 when state_id equals 34, otherwise (when state_id equals 42), geo_key = 3, and makes it the 1st column. FOUND OUT HOW TO CREATE A COLUMN BASED ON A CONDITION OF ANOTHER COLUMN FROM https://thispointer.com/numpy-where-tutorial-examples-python/



```pa_suburban_key = state_key[(state_key["state_id"]=="42") & (state_key["county_id_3dig"]!="101")][["state_id", "county_id_3dig", "county_id_5dig"]]```:  Starts creating pa_suburban_key by first making a subset of state_key where state_id equals 42 and county_id_3dig doesn't equal 101, and leaving out its geo_key column. FOUND OUT HOW TO SUBSET A DATAFRAME USING MULTIPLE CONDITIONS FROM https://kanoki.org/2020/01/21/pandas-dataframe-filter-with-multiple-conditions/  



```tables_to_upload_dictionary = {k:v for k,v in locals().items() if k in names_of_tables_to_upload}```: Creates a dictionary of just the tables to upload BY PULLING OUT THE TABLES TO UPLOAD FROM THE DICTIONARY OF ALL OBJECTS CREATED IN THIS SCRIPT UP UNTIL THIS POINT BY THEIR OBJECT NAMES, AND PUTTING THOSE ITEMS INTO THEIR OWN DICTIONARY. FOUND OUT HOW TO GET A DICTIONARY OF ALL ITEMS CREATED IN THIS SCRIPT UP UNTIL THIS POINT FROM https://stackoverflow.com/a/4458733 (IN TURN FOUND ON https://stackoverflow.com/questions/4458701/how-to-get-the-list-of-all-initialized-objects-and-function-definitions-alive-in ), AND FOUND OUT HOW TO SUBSET A DICTIONARY FROM https://stackoverflow.com/a/49803523 (IN TURN FOUND ON https://stackoverflow.com/questions/3953371/get-a-sub-set-of-a-python-dictionary )





```
#THIS COMMENTED OUT CHUNK OF CODE SHOWS A HYPOTHETICAL EXAMPLE OF HOW TO CREATE A DICTIONARY OF TABLES TO UPLOAD INTO THE DATABASE AUTOMATICALLY BASED ON OBJECT NAME


```resource_table_names = [key for key in locals() if key.endswith("")]```: Creates a list containing the names of the objects/tables to upload to the  resources schema of the database. IT DOES THIS BY GETTING THE DICTIONARY OF ALL OBJECTS CREATED IN THIS SCRIPT UP UNTIL THIS POINT, AND JUST EXTRACTS THE KEYS THAT END WITH " table". FOUND OUT HOW TO GET A DICTIONARY OF ALL ITEMS CREATED IN THIS SCRIPT UP UNTIL THIS POINT FROM https://stackoverflow.com/a/4458733 (IN TURN FOUND ON https://stackoverflow.com/questions/4458701/how-to-get-the-list-of-all-initialized-objects-and-function-definitions-alive-in ), AND FOUND OUT HOW TO EXTRACT KEYS BASED ON A STRING PATTERN WITHIN THEM FROM https://stackoverflow.com/a/26204824 (IN TURN FOUND ON https://stackoverflow.com/questions/26204801/how-to-use-extract-a-list-of-keys-with-specific-pattern-from-dict-in-python )

```resource_tables_dictionary = {k:v for k,v in locals().items() if k in resource_table_names}```: Creates a dictionary of just the tables to upload BY PULLING OUT THE TABLES TO UPLOAD FROM THE DICTIONARY OF ALL OBJECTS CREATED IN THIS SCRIPT UP UNTIL THIS POINT BY THEIR OBJECT NAMES, AND PUTTING THOSE ITEMS INTO THEIR OWN DICTIONARY. FOUND OUT HOW TO SUBSET A DICTIONARY FROM https://stackoverflow.com/a/49803523 (IN TURN FOUND ON https://stackoverflow.com/questions/3953371/get-a-sub-set-of-a-python-dictionary )
```





``` 
#THIS COMMENTED OUT CHUNK OF CODE SHOWS AN EXAMPLE OF HOW TO USE AARON'S import_dataframe() FUNCTION TO IMPORT ONE DATAFRAME INTO POSTGRES


db = Database(DATABASE_URL)

db.import_dataframe(cpi, f"_resources.cpi", df_import_kwargs={"if_exists": "replace", "index": False})
```












