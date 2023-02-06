# Typology Experiments

## What is this?

The method the DVRPC region is classified by levels of density and accessibility is being updated. This repository was created for sharing code and documentation related to the process of grabbing and processing source data, then creating the density and accessibility table, assigning levels of density and accessibility to each 2020 block group in the DVRPC region.

DVRPC staff first classified the region by levels of density and accessibility using municipalities as part of the LRP creation process. Much of the data processing for that was done using GIS. As the exact methodology for classifying the region by levels of density and accessibility is refined, staff will be updating existing code within this repo.

## To do the analysis, run these scripts in order

1. raw_input_data_upload.py - *I also wrote how tables uploaded to _raw not using this script were created in this script's Obsidian comments file*
2. analysis.block_groups_24co_2020 create.sql
3. block_group_centroids_24co_2020_and_their_buffers_create.py
4. block_centroids_2020_with_2020_decennial_pop_and_hhs_create.py
5. block_group_land_by_developability_create.py - *Warning: This script will take roughly 2 hours and 10 minutes to run because the intersecting of the block groups with the developable block group fragments takes a while*
6. unprotected_land_area_create.py
7. crosswalks.sql
8. analysis.block_groups_dvrpc_2020 create.sql
9. analysis.crosswalks_block_groups_dvrpc_2020.sql
10. analysis.crosswalks_density_block_groups_dvrpc_2020.sql
11. analysis.costarproperties_region_plus_surrounding.sql
12. analysis.costarproperties_rentable_area_bg.sql - *Warning: This script may take a long time to run because the joining of the costar property locations to the block groups takes a while*
13. analysis.costar_number_of_stories create.sql - *Warning: This script may take a long time to run because the joining of the costar property locations to the block groups takes a while*
14. analysis.density_index create.sql
15. analysis.incorp_del_river_bg_centroids_24co_2020_buffers create.sql
16. proximity_index_step1_create.py - *Warning: Allow for about 45 minutes for this script to run because the joining of the costar property locations to the buffers takes a while*
17. analysis.proximity_index create.sql
18. analysis.transect_step1 create.sql
19. analysis.crosswalk_density_summary create.sql
20. analysis.transect create.sql
21. analysis.transect_mcd_translation create.sql
22. analysis.transect_taz_translation create.sql
23. analysis.transect_tract_translation create.sql
24. analysis.transect_philadelphia_planning_district_translation create.sql

## Python environment

- Install [miniconda](https://docs.conda.io/en/latest/miniconda.html)
- Open a `conda` command prompt, `cd` into this folder, and run:

```
conda env create -f environment.yml
```

- You can now activate the environment with:

```
conda activate typology-experiments
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
from typology_experiments.helpers import do_something


do_something(times=3)
```