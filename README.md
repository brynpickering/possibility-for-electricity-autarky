# Possibility for renewable electricity autarky in Europe

Is your European region able to provide itself with 100% renewable electricity?

This repository contains the entire research project, including code and report. The philosophy behind this repository is that no intermediary results are included, but all results are computed from raw data and code.

## Getting ready

### Installation

The following dependencies are needed to set up an environment in which the analysis can be run and the paper be build:

* [conda](https://conda.io/docs/index.html)
* `LaTeX` to [produce a PDF](http://pandoc.org/MANUAL.html#creating-a-pdf). Can be avoided by switching to [any other output format supported by pandoc](http://pandoc.org/index.html).

When these dependencies are installed, you can create a conda environment from within you can run the analysis:

    conda env create -f conda-environment.yml

Don't forget to activate the environment. To see what you can do now, run:

    snakemake --list

### Data to be retrieved manually

Whenever possible, data is downloaded automatically. As this is not always possible, you will need to retrieve the following data sets manually:

* [European Settlement Map 2012, Release 2017, 100m](https://land.copernicus.eu/pan-european/GHSL/european-settlement-map), to be placed at `./data/esm-100m-2017/`
* [World Exclusive Economic Zones v10](http://www.marineregions.org/downloads.php), to be placed in `./data/World_EEZ_v10_20180221`
* [Sonnendach.ch 2018-08-27](http://www.sonnendach.ch), to be placed in `./data/sonnendach/SOLKAT_20180827.gdb`
* [Federal Register of Buildings and Dwellings (RBD/GWR) 2018-07-01](https://www.bfs.admin.ch/bfs/en/home/registers/federal-register-buildings-dwellings.html), to be placed in `./data/gwr/`
* capacity factors from renewable.ninja, to be placed in `./data/capacityfactors/{technology}` for technology in ["wind-onshore", "wind-offshore", "rooftop-pv", "open-field-pv"] (where "open-field-pv" and "rooftop-pv" can be the same dataset and hence can be linked instead of copied)(to run simulations, see `Manual steps` below)

## Run the analysis

    snakemake paper

This will run all analysis steps to reproduce results and eventually build the paper.

You can also run certain parts only by using other `snakemake` rules; to get a list of all rules run `snakemake --list`.

To generate a PDF of the dependency graph of all steps, run:

    snakemake --rulegraph | dot -Tpdf > dag.pdf

(needs `dot`: `conda install graphviz`).

## Manual steps

At the moment, there is one manual step involved: running renewables.ninja simulations of wind and solar electricity. It is added to the automatic workflow as input data. Should you want to change the simulations, because you want to change parameters of the simulation (see `parameters.ninja` in the config), you can do that in three steps:

1) Create input files by first chaning the config, then running `smake -s rules/ninja-input.smk`.
2) Run the simulations on renewables.ninja.
3) Update the data in `data/capacityfactors/{technology}`.

## Run the tests

    snakemake test

## Repo structure

* `report`: contains all files necessary to build the paper; plots and result files are not in here but generated automatically
* `src`: contains the Python source code
* `tests`: contains the test code
* `config`: configurations used in the study
* `rules`: additional Snakemake rules and workflows
* `data`: place for raw data, whether retrieved manually and automatically
* `build`: will contain all results (does not exist initially)

## Citation

If you make use of this in academic work, please cite:

Tim Tröndle, Stefan Pfenninger, and Johan Lilliestam (in review). Home-made or made in Europe: on the possibility for renewable electricity autarky on all scales in Europe. Energy Strategy Reviews
