# Development Intensity Zones

## What is this?

The method the DVRPC region is classified by levels of density and proximity is being updated. This repository was created for sharing code and documentation related to the process of grabbing and processing source data, then creating the density and proximity table, assigning levels of density and proximity to each 2020 block group in the DVRPC region.

DVRPC staff first classified the region by levels of density and proximity using municipalities as part of the LRP creation process. Much of the data processing for that was done using GIS. As the exact methodology for classifying the region by levels of density and proximity is refined, staff will be updating existing code within this repo.

## To do the analysis, run these scripts in order

1. raw_input_data_upload.py
2. analysis.block_groups_24co_2020 create.sql
3. block_groups_24co_2020_area_calcs_create.py - *Warning: This script will take roughly 1 hour and 38 minutes to run*
4. block_group_centroids_24co_2020_and_their_buffers_create.py
5. block_centroids_2020_with_2020_decennial_pop_and_hhs_create.py
6. block_group_land_by_developability_create.py - *Warning: This script will take roughly 2 hours and 10 minutes to run because the intersecting of the block groups with the developable block group fragments takes a while*
7. analysis.unprotected_land_area create.sql
8. crosswalks.sql
9. analysis.block_groups_dvrpc_2020 create.sql
10. analysis.crosswalks_block_groups_dvrpc_2020.sql
11. analysis.crosswalks_density_block_groups_dvrpc_2020.sql
12. analysis.costarproperties_region_plus_surrounding.sql
13. not_in_costar_upload.py
14. analysis.costarproperties_rentable_area_bg.sql - *Warning: This script may take a long time to run because the joining of the costar property locations to the block groups takes a while*
15. analysis.costar_number_of_stories create.sql - *Warning: This script may take a long time to run because the joining of the costar property locations to the block groups takes a while*
16. analysis.density_index create.sql
17. analysis.incorp_del_river_bg_centroids_24co_2020_buffers create.sql
18. proximity_index_step1_create.py - *Warning: Allow for about 45 minutes for this script to run because the joining of the costar property locations to the buffers takes a while*
19. analysis.proximity_index create.sql
20. analysis.diz_block_group_step1 create.sql
21. analysis.crosswalk_density_summary create.sql
22. analysis.diz_block_group create.sql
23. diz_create.py - *Warning: Allow for about 45 minutes for this script to run*
24. analysis.diz_mcd create.sql
25. analysis.diz_taz create.sql
26. analysis.diz_tract create.sql
27. analysis.diz_philadelphia_planning_district create.sql

## Objects created manually

- Ben Gruswitz manually created _raw.delaware_river_centerline using these steps: 
1) Got the default Delaware River centerline shapefile from an ArcGIS Online resource (he doesn't remember where it was)
2) Drew a straight line out from the end of it further into the Delaware Bay, so that we can continue to split buffers there
3) Dragged and dropped the final shapefile into the database as _raw.delaware_river_centerline
- Brian Carney manually copied _raw.pedestriannetwork_lines directly from the DVRPC GIS Postgres database into _raw he's pretty sure
- Ian Schwarzenberg manually exported demographics.census_mcds_2020 from the DVRPC GIS Postsgres database as _raw.census_mcds_2020
- Ian Schwarzenberg manually exported demographics.taz_2010_mcdaligned from the DVRPC GIS Postsgres database as _raw.taz_2010_mcdaligned
- Ian Schwarzenberg manually exported demographics.census_tracts_2020 from the DVRPC GIS Postsgres database as _raw.census_tracts_2020
- Ian Schwarzenberg manually exported demographics.census_mcds_phipd_2020 from the DVRPC GIS Postsgres database as _raw.dvrpc_mcd_phicpa
- Ian Schwarzenberg manually created _raw.pos_h2o_diz_zone_0 using these steps: 
1) Brought U:/_OngoingProjects/CoStar/To be moved later/Transect for AGO/db_exports.gdb/pos_h2o_transect_zone_0 straight into ArcMap (in turn got the data from Ben Gruswitz) 
2) Used [these insturctions](https://support.esri.com/en-us/knowledge-base/how-to-convert-3d-shapefiles-to-2d-shapefiles-000008433), along with choosing EPSG 26918 as the coordinate system, to create a 2D shapefile version of that feature class, and store that shapefile as G:\Shared drives\Socioeconomic and Land Use Analytics\Tracking Progress\Data Preparation\postgres_db\pos_h2o_transect_zone_0_2D.shp
3) Used the Repair Geometry tool to fix any geometry errors in that shapefile, and exported that shapefile as G:\Shared drives\Socioeconomic and Land Use Analytics\Tracking Progress\Data Preparation\postgres_db\pos_h2o_diz_zone_0_SHP.shp 
4) Using pos_h2o_diz_zone_0_upload.py (only needed to be run once and that's it), loaded that shapefile into the database as _raw.pos_h2o_diz_zone_0
- Ben Gruswitz and Sean Lawrence uploaded the 15 surrounding counties' protected land and water data from G:/Shared drives/Long Range Plan/2050B Plan/Centers Update/development_intensity_zones/pos_h2o separately as _raw.surrounding_counties_waterbodies and _raw.surrounding_county_pos
- Ben Gruswitz manually created and uploaded _resources.classifications and _resources.diz_zone_names

## Python environment

- Install [miniconda](https://docs.conda.io/en/latest/miniconda.html)
- Open a `conda` command prompt, `cd` into this folder, and run:

```
conda env create -f environment.yml
```

- You can now activate the environment with:

```
conda activate development-intensity-zones
```

- However, then run these 2 commands in order in the terminal, in order to fix an error that comes up later with geopandas (found out how to fix that error from https://stackoverflow.com/a/69642315, which in turn was found on https://stackoverflow.com/questions/69630630/on-fresh-conda-installation-of-pyproj-pyproj-unable-to-set-database-path-pypr ):

```
conda remove --force pyproj
python -m pip install "pyproj"
```

- With the environment activated and fully created for that matter, you can run scripts with:

```
python PATH/TO/FILE.py
```

- You can import functions and classes from other files in this repo like this:

```python
from development_intensity_zones.helpers import do_something


do_something(times=3)
```