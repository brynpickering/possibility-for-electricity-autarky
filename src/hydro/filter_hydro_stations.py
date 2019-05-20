import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


def filter_stations(path_to_stations, path_to_basins, path_to_output):
    stations = pd.read_csv(path_to_stations, index_col=0)
    hydrobasins = gpd.read_file(path_to_basins)
    is_in_basin = stations.apply(station_in_any_basin(hydrobasins), axis="columns")
    stations[is_in_basin].to_csv(
        path_to_output,
        header=True,
        index=True
    )


def station_in_any_basin(basins):
    def station_in_any_basin(station):
        point = Point(station.lon, station.lat)
        return basins.geometry.intersects(point).sum() > 0
    return station_in_any_basin


if __name__ == "__main__":
    filter_stations(
        path_to_stations=snakemake.input.stations,
        path_to_basins=snakemake.input.basins,
        path_to_output=snakemake.output[0]
    )
