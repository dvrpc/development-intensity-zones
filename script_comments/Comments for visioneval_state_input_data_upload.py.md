# Comments for visioneval_state_input_data_upload.py

##Note: Because underscores BEFORE LETTERS BUT NOT AFTER THEM are used to italicize words, sentences, paragraphs, etc in Obsidian, this Obsidian document has spaces instead of those ONLY FOR TEXT IN THIS OBSIDIAN DOCUMENT THAT'S OUTSIDE OF CODE LINES

##Script location: typology_experiments.scripts. raw.visioneval_state_input_data_upload.py




```api_urls = ["https://api.census.gov/data/2020/dec/pl?get=P1_001N,H1_002N&for=block:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="+os.environ["CENSUS_KEY"], "https://api.census.gov/data/2020/dec/pl?get=P1_001N,H1_002N&for=block%20group:*&in=state:10,24,34,42&in=county:*&in=tract:*&key="+os.environ["CENSUS_KEY"]]```: Creates both the 2020 Decennial Census total households and population by 2020 block API URL to use, and the exact same API URL but by 2020 block group instead, and both for ALL of DE, MD, NJ and PA. FOUND OUT WHICH VARIABLES TO USE FROM BEN GRUSWITZ AND https://api.census.gov/data/2020/dec/pl/variables.html (IN TURN FOUND ON https://api.census.gov/data/2020/dec/ ), FOUND OUT HOW TO CONSTRUCT BOTH API URLS FROM https://api.census.gov/data/2020/dec/pl/examples.html (ALSO IN TURN FOUND ON https://api.census.gov/data/2020/dec/ ), AND FOUND OUT THE FIPS CODES FOR DELAWARE, MARYLAND, NEW JERSEY AND PENNSYLVANIA FROM https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696 


```dvrpc_land_use_2015 = esri2gpd.get("https://arcgis.dvrpc.org/portal/rest/services/Planning/DVRPC_LandUse_2015/FeatureServer/0", where="lu15sub = '13000'").to_crs(crs="EPSG:32618")```: Reads in as a geo data frame/shapefile in the standard DVRPC EPSG a GIS server link containing just the records I want from the 2015 DVRPC land use inventory. FOUND THE LINK TO THE SHAPEFILE FROM https://catalog.dvrpc.org/dataset/land-use-2015/resource/f261a917-f2a5-4b52-8284-6939c23dad81 