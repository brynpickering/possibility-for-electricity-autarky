"""This is a Snakemake file defining rules to retrieve raw data from online sources."""
import pycountry
import pandas as pd

URL_LOAD = "https://data.open-power-system-data.org/time_series/2018-06-30/time_series_60min_stacked.csv"
URL_NUTS = "https://ec.europa.eu/eurostat/cache/GISCO/distribution/v2/nuts/shp/NUTS_RG_01M_{}_4326.shp.zip"
URL_LAU = "http://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/COMM-01M-2013-SH.zip"
URL_DEGURBA = "http://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/DGURBA_2014_SH.zip"
URL_LAND_COVER = "http://due.esrin.esa.int/files/Globcover2009_V2.3_Global_.zip"
URL_PROTECTED_AREAS = "http://d1gam3xoknrgr2.cloudfront.net/current/WDPA_{}-shapefile.zip"
URL_SRTM_TILE = "http://viewfinderpanoramas.org/dem3/"
URL_GADM = "https://biogeo.ucdavis.edu/data/gadm3.6/gpkg/"
URL_BATHYMETRIC = "https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO1/data/bedrock/grid_registered/georeferenced_tiff/ETOPO1_Bed_g_geotiff.zip"
URL_POP = "http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GPW4_GLOBE_R2015A/GHS_POP_GPW42015_GLOBE_R2015A_54009_250/V1-0/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.zip"


RAW_SETTLEMENT_DATA = "data/esm-100m-2017/ESM_class{esm_class}_100m.tif"
RAW_EEZ_DATA = "data/World_EEZ_v10_20180221/eez_v10.shp"
RAW_INDUSTRY_DATA = "data/electricity-intensive-industry/energy-intensive-industries.xlsx"

localrules: raw_load, raw_gadm_administrative_borders_zipped, raw_protected_areas_zipped,
    raw_nuts_units_zipped, raw_lau_units_zipped, raw_urbanisation_zipped, raw_land_cover_zipped,
    raw_land_cover, raw_srtm_elevation_tile_zipped,
    raw_bathymetry_zipped, raw_bathymetry, raw_population_zipped, raw_population,
    raw_gadm_administrative_borders


rule raw_load:
    message: "Download raw load."
    output:
        protected("data/automatic/raw-load-data.csv")
    shell:
        "curl -sLo {output} '{URL_LOAD}'"


rule electricity_demand_national:
    message: "Determine yearly demand per country."
    input:
        "src/process_load.py",
        rules.raw_load.output
    output:
        "build/electricity-demand-national.csv"
    conda: "../envs/default.yaml"
    shell:
        PYTHON_SCRIPT_WITH_CONFIG


rule raw_gadm_administrative_borders_zipped:
    message: "Download administrative borders for {wildcards.country_code} as zip."
    output: protected("data/automatic/raw-gadm/{country_code}.zip")
    shell: "curl -sLo {output} '{URL_GADM}/gadm36_{wildcards.country_code}_gpkg.zip'"


rule raw_gadm_administrative_borders:
    message: "Unzip administrative borders of {wildcards.country_code} as zip."
    input: "data/automatic/raw-gadm/{country_code}.zip"
    output: temp("data/automatic/raw-gadm/gadm36_{country_code}.gpkg")
    shell: "unzip -o {input} -d data/automatic/raw-gadm"


rule all_gadm_administrative_borders:
    message: "Merge gadm administrative borders of all countries."
    input:
        ["data/automatic/raw-gadm/gadm36_{}.gpkg".format(country_code)
            for country_code in [pycountry.countries.lookup(country).alpha_3
                                 for country in config['scope']['countries']]
         ]
    output: temp("data/automatic/raw-gadm/gadm36.gpkg")
    params: crs = config["crs"]
    conda: '../envs/default.yaml'
    shell:
        """
        ogrmerge.py -o {output} -f gpkg -src_layer_field_content "{{LAYER_NAME}}" -t_srs {params.crs} -single {input}
        """


rule raw_nuts_units_zipped:
    message: "Download units as zip."
    output:
        protected("data/automatic/raw-nuts{}-units.zip".format(config["parameters"]["nuts-year"]))
    params:
        url = URL_NUTS.format(config["parameters"]["nuts-year"])
    shell:
        "curl -sLo {output} '{params.url}'"


rule raw_lau_units_zipped:
    message: "Download LAU units as zip."
    output:
        protected("data/automatic/raw-lau-units.zip")
    shell:
        "curl -sLo {output} '{URL_LAU}'"


rule administrative_borders_lau:
    message: "Normalise LAU administrative borders."
    input:
        src = "src/lau.py",
        zip = rules.raw_lau_units_zipped.output
    output:
        temp("build/raw-lau-identified.gpkg")
    shadow: "full"
    conda: "../envs/default.yaml"
    shell:
        """
        unzip {input.zip} -d ./build
        {PYTHON} {input.src} merge ./build/COMM_01M_2013_SH/data/COMM_RG_01M_2013.shp \
        ./build/COMM_01M_2013_SH/data/COMM_AT_2013.dbf ./build/raw-lau.gpkg
        {PYTHON} {input.src} identify ./build/raw-lau.gpkg ./build/raw-lau-identified.gpkg
        """


rule administrative_borders:
    message: "Normalise all administrative borders."
    input:
        src = "src/administrative_borders.py",
        nuts_zip = rules.raw_nuts_units_zipped.output,
        gadm_gpkg = rules.all_gadm_administrative_borders.output,
        lau_gpkg = rules.administrative_borders_lau.output
    output:
        "build/administrative-borders.gpkg"
    shadow: "full"
    params:
        year = config['parameters']['nuts-year']
    conda: "../envs/default.yaml"
    shell:
        """
        unzip {input.nuts_zip} -d ./build/NUTS_RG_01M_{params.year}_4326/
        {PYTHON} {input.src} ./build/NUTS_RG_01M_{params.year}_4326 {input.gadm_gpkg} {input.lau_gpkg} {output} {CONFIG_FILE}
        """


rule raw_urbanisation_zipped:
    message: "Download DEGURBA units as zip."
    output:
        protected("data/automatic/raw-degurba-units.zip")
    shell:
        "curl -sLo {output} '{URL_DEGURBA}'"


rule lau2_urbanisation_degree:
    message: "Urbanisation degrees on LAU2 level."
    input:
        src = "src/lau.py",
        lau2 = rules.administrative_borders_lau.output,
        degurba = rules.raw_urbanisation_zipped.output
    output:
        "build/administrative-borders-lau-urbanisation.csv"
    shadow: "full"
    conda: "../envs/default.yaml"
    shell:
        """
        unzip {input.degurba} -d ./build
        {PYTHON} {input.src} degurba {input.lau2} ./build/DGURBA_2014_SH/data/DGURBA_RG_01M_2014.shp {output}
        """


rule raw_land_cover_zipped:
    message: "Download land cover data as zip."
    output: protected("data/automatic/raw-globcover2009.zip")
    shell: "curl -sLo {output} '{URL_LAND_COVER}'"


rule raw_land_cover:
    message: "Extract land cover data as zip."
    input: rules.raw_land_cover_zipped.output
    output: temp("build/GLOBCOVER_L4_200901_200912_V2.3.tif")
    shadow: "minimal"
    shell: "unzip {input} -d ./build/"


rule raw_protected_areas_zipped:
    message: "Download protected areas data as zip."
    output: protected("data/automatic/raw-wdpa.zip")
    params:
        url = URL_PROTECTED_AREAS.format(config["parameters"]["wdpa-year"])
    shell: "curl -sLo {output} -H 'Referer: {params.url}' {params.url}"


rule raw_protected_areas:
    message: "Extract protected areas data as zip."
    input: rules.raw_protected_areas_zipped.output
    params:
        year = config["parameters"]["wdpa-year"]
    output:
        polygons = "build/raw-wdpa/wdpa-shapes.shp",
        polygon_data = "build/raw-wdpa/wdpa-shapes.dbf",
        points = "build/raw-wdpa/wdpa-points.shp"
    conda: "../envs/default.yaml"
    shell:
        """
        set +e
        unzip -o {input} -d build/raw-wdpa
        unzip -o build/raw-wdpa/WDPA_{params.year}-shapefile0.zip -d build/raw-wdpa/WDPA_0
        unzip -o build/raw-wdpa/WDPA_{params.year}-shapefile1.zip -d build/raw-wdpa/WDPA_1
        unzip -o build/raw-wdpa/WDPA_{params.year}-shapefile2.zip -d build/raw-wdpa/WDPA_2
        ogrmerge.py -single -o {output.polygons} \
        build/raw-wdpa/WDPA_0/WDPA_{params.year}-shapefile-polygons.shp \
        build/raw-wdpa/WDPA_1/WDPA_{params.year}-shapefile-polygons.shp \
        build/raw-wdpa/WDPA_2/WDPA_{params.year}-shapefile-polygons.shp
        ogrmerge.py -single -o {output.points} \
        build/raw-wdpa/WDPA_0/WDPA_{params.year}-shapefile-points.shp \
        build/raw-wdpa/WDPA_1/WDPA_{params.year}-shapefile-points.shp \
        build/raw-wdpa/WDPA_2/WDPA_{params.year}-shapefile-points.shp
        exitcode=$?
        if [ $exitcode -eq 1 ]
        then
            exit 1
        else
            exit 0
        fi
        """


rule raw_srtm_elevation_tile_zipped:
    message: "Download SRTM elevation data tile (tile={wildcards.tile}) from viewfinderpanoramas."
    output:
        protected("data/automatic/raw-srtm/srtm_{tile}.zip")
    shell:
        """
        curl -sLo {output} '{URL_SRTM_TILE}/{wildcards.tile}.zip'
        """


rule elevation_in_europe:
    message: "Merge all SRTM elevation data tiles."
    input:
        ["data/automatic/raw-srtm/srtm_{}.zip".format(idx)
         for idx, bounds in
         pd.read_csv("data/viewfinderpanoramas.csv", index_col=0, header=0).iterrows()
         if bounds['x_min'] >= config["scope"]["bounds"]["x_min"] and
         bounds['x_max'] <= config["scope"]["bounds"]["x_max"] and
         bounds['y_min'] >= config["scope"]["bounds"]["y_min"] and
         bounds['y_max'] <= config["scope"]["bounds"]["y_max"]]
    output:
        temp("build/elevation-europe.tif")
    conda: "../envs/default.yaml"
    shell:
        """
        find {input} -exec unzip -oj {{}} -d ./build/tmp_srtm_tiles \;
        find ./build/tmp_srtm_tiles -name '*.zip' -exec unzip -oj {{}} -d ./build/tmp_srtm_tiles \;
        find ./build/tmp_srtm_tiles -name '*.hgt' -type f -exec rio merge -o {output} {{}} +
        rm -r ./build/tmp_srtm_tiles
        """


rule raw_bathymetry_zipped:
    message: "Download bathymetric data as zip."
    output: protected("data/automatic/raw-bathymetric.zip")
    shell: "curl -sLo {output} '{URL_BATHYMETRIC}'"


rule raw_bathymetry:
    message: "Extract bathymetric data from zip."
    input: rules.raw_bathymetry_zipped.output
    output: temp("build/ETOPO1_Bed_g_geotiff.tif")
    shell: "unzip {input} -d ./build/"


rule land_cover_in_europe:
    message: "Clip land cover data to Europe."
    input: rules.raw_land_cover.output
    output: "build/land-cover-europe.tif"
    params: bounds = "{x_min},{y_min},{x_max},{y_max}".format(**config["scope"]["bounds"])
    conda: "../envs/default.yaml"
    shell: "rio clip {input} {output} --bounds {params.bounds}"


rule slope_in_europe:
    message: "Calculate slope and warp to resolution of study using {threads} threads."
    input:
        elevation = rules.elevation_in_europe.output,
        land_cover = rules.land_cover_in_europe.output
    output:
        "build/slope-europe.tif"
    threads: config["snakemake"]["max-threads"]
    conda: "../envs/default.yaml"
    shell:
        """
        gdaldem slope -s 111120 -compute_edges {input.elevation} build/slope-temp.tif
        rio warp build/slope-temp.tif -o {output} --like {input.land_cover} \
        --resampling max --threads {threads}
        rm build/slope-temp.tif
        """


rule protected_areas_points_to_circles:
    message: "Estimate shape of protected areas available as points only."
    input:
        "src/estimate_protected_shapes.py",
        rules.raw_protected_areas.output.points
    output:
        temp("build/protected-areas-points-as-circles.geojson")
    conda: "../envs/default.yaml"
    shell:
        PYTHON_SCRIPT_WITH_CONFIG


rule protected_areas_in_europe:
    message: "Rasterise protected areas data and clip to Europe."
    input:
        polygons = rules.raw_protected_areas.output.polygons,
        points = rules.protected_areas_points_to_circles.output,
        land_cover = rules.land_cover_in_europe.output
    output:
        "build/protected-areas-europe.tif"
    benchmark:
        "build/rasterisation-benchmark.txt"
    params:
        bounds = "{x_min},{y_min},{x_max},{y_max}".format(**config["scope"]["bounds"])
    conda: "../envs/default.yaml"
    shell:
        # The filter is in accordance to the way UNEP-WCMC calculates statistics:
        # https://www.protectedplanet.net/c/calculating-protected-area-coverage
        """
        fio cat --rs --bbox {params.bounds} {input.polygons} {input.points} | \
        fio filter "f.properties.STATUS in ['Designated', 'Inscribed', 'Established'] and \
        f.properties.DESIG_ENG != 'UNESCO-MAB Biosphere Reserve'" | \
        fio collect --record-buffered | \
        rio rasterize --like {input.land_cover} \
        --default-value 255 --all_touched -f "GTiff" --co dtype=uint8 -o {output}
        """


rule settlements:
    message: "Warp settlement data to CRS of study using {threads} threads."
    input:
        class50 = RAW_SETTLEMENT_DATA.format(esm_class="50"),
        class40 = RAW_SETTLEMENT_DATA.format(esm_class="40"),
        class41 = RAW_SETTLEMENT_DATA.format(esm_class="41"),
        class45 = RAW_SETTLEMENT_DATA.format(esm_class="45"),
        class30 = RAW_SETTLEMENT_DATA.format(esm_class="30"),
        class35 = RAW_SETTLEMENT_DATA.format(esm_class="35"),
        reference = rules.land_cover_in_europe.output
    output:
        buildings = "build/esm-class50-buildings.tif",
        urban_greens = "build/esm-class404145-urban-greens.tif",
        built_up = "build/esm-class303550-built-up.tif"
    threads: config["snakemake"]["max-threads"]
    shadow: "minimal"
    conda: "../envs/default.yaml"
    shell:
        """
        rio calc "(+ (+ (read 1) (read 2)) (read 3))" \
        {input.class40} {input.class41} {input.class45} -o build/esm-class404145-temp-not-warped.tif
        rio calc "(+ (+ (read 1) (read 2)) (read 3))" \
        {input.class50} {input.class30} {input.class35} -o build/esm-class303550-temp-not-warped.tif
        rio warp {input.class50} -o {output.buildings} \
        --like {input.reference} --threads {threads} --resampling bilinear
        rio warp build/esm-class404145-temp-not-warped.tif -o {output.urban_greens} \
        --like {input.reference} --threads {threads} --resampling bilinear
        rio warp build/esm-class303550-temp-not-warped.tif -o {output.built_up} \
        --like {input.reference} --threads {threads} --resampling bilinear
        """


rule bathymetry_in_europe:
    message: "Clip bathymetric data to study area and warp to study resolution."
    input:
        bathymetry = rules.raw_bathymetry.output,
        reference = rules.land_cover_in_europe.output
    output:
        "build/bathymetry-in-europe.tif"
    conda: "../envs/default.yaml"
    shell:
        """
        rio warp {input.bathymetry} -o {output} --like {input.reference} --resampling min
        """


rule eez_in_europe:
    message: "Clip exclusive economic zones to study area."
    input: RAW_EEZ_DATA
    output: "build/eez-in-europe.geojson"
    params:
        bounds="{x_min},{y_min},{x_max},{y_max}".format(**config["scope"]["bounds"]),
        countries=",".join(["'{}'".format(country) for country in config["scope"]["countries"]])
    conda: "../envs/default.yaml"
    shell:
        """
        fio cat --bbox {params.bounds} {input}\
        | fio filter "f.properties.Territory1 in [{params.countries}]"\
        | fio collect > {output}
        """


rule industry:
    message: "Preprocess data on electricity intensive industry."
    input:
        "src/industry.py",
        RAW_INDUSTRY_DATA
    output:
        "build/industrial-load.geojson"
    conda: "../envs/default.yaml"
    shell:
        PYTHON_SCRIPT


rule raw_population_zipped:
    message: "Download population data."
    output:
        protected("data/automatic/raw-population-data.zip")
    shell:
        "curl -sLo {output} '{URL_POP}'"


rule raw_population:
    message: "Extract population data as zip."
    input: rules.raw_population_zipped.output
    output: temp("build/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.tif")
    shadow: "minimal"
    shell:
        """
        unzip {input} -d ./build/
        mv build/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.tif {output}
        """


rule population_in_europe:
    message: "Clip population data to bounds of study."
    input:
        population = rules.raw_population.output,
    output:
        "build/population-europe.tif"
    params:
        bounds="{x_min},{y_min},{x_max},{y_max}".format(**config["scope"]["bounds"])
    conda: "../envs/default.yaml"
    shell:
        """
        rio clip --geographic --bounds {params.bounds} --co compress=LZW {input.population} -o {output}
        """
