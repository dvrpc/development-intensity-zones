# Typology Experiments

## What is this?

The method the DVRPC region is classified by levels of density and accessibility is being updated. This repository was created for sharing code and documentation related to the process of grabbing and processing source data, then creating the density and accessibility table, assigning levels of density and accessibility to each 2020 block group in the DVRPC region.

DVRPC staff first classified the region by levels of density and accessibility using municipalities as part of the LRP creation process. Much of the data processing for that was done using GIS. As the exact methodology for classifying the region by levels of density and accessibility is refined, staff will be updating existing code within this repo.

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

- However, then run these commands in order in the terminal, in order to fix an error that comes up later with geopandas (found out how to fix that error that came up when using geopandas from https://stackoverflow.com/a/69642315, which in turn was found on https://stackoverflow.com/questions/69630630/on-fresh-conda-installation-of-pyproj-pyproj-unable-to-set-database-path-pypr ):

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
