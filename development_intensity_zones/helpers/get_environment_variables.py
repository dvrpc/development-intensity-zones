import os
from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("DATABASE_URL")
CENSUS_KEY = os.getenv("CENSUS_KEY")
BLS_KEY = os.getenv("BLS_KEY")
GGMAP_KEY = os.getenv("GGMAP_KEY")
