"""This module determines land eligibility for renewable generation based on geospatial data."""
from enum import IntEnum

import click
import numpy as np
import rasterio

MAX_SLOPE_PV = 3
MAX_SLOPE_WIND = 20

DATATYPE = np.uint8


class Eligibility(IntEnum):
    """Categories defining land eligibility for renewable power."""
    NOT_ELIGIBLE = 0
    ROOFTOP_PV = 250
    ONSHORE_WIND_OR_PV_FARM = 200
    ONSHORE_WIND_FARM = 150
    OFFSHORE_WIND_FARM = 80

    @property
    def property_name(self):
        return "eligibility_{}_km2".format(self.name.lower())


class GlobCover(IntEnum):
    """Original categories taken from GlobCover 2009 land cover."""
    POST_FLOODING = 11
    RAINFED_CROPLANDS = 14
    MOSAIC_CROPLAND = 20
    MOSAIC_VEGETATION = 30
    CLOSED_TO_OPEN_BROADLEAVED_FOREST = 40
    CLOSED_BROADLEAVED_FOREST = 50
    OPEN_BROADLEAVED_FOREST = 60
    CLOSED_NEEDLELEAVED_FOREST = 70
    OPEN_NEEDLELEAVED_FOREST = 90
    CLOSED_TO_OPEN_MIXED_FOREST = 100
    MOSAIC_FOREST = 110
    MOSAIC_GRASSLAND = 120
    CLOSED_TO_OPEN_SHRUBLAND = 130
    CLOSED_TO_OPEN_HERBS = 140
    SPARSE_VEGETATION = 150
    CLOSED_TO_OPEN_REGULARLY_FLOODED_FOREST = 160
    CLOSED_REGULARLY_FLOODED_FOREST = 170
    CLOSED_TO_OPEN_REGULARLY_FLOODED_GRASSLAND = 180
    ARTIFICAL_SURFACES_AND_URBAN_AREAS = 190
    BARE_AREAS = 200
    WATER_BODIES = 210
    PERMANENT_SNOW = 220
    NO_DATA = 230


FARM = [GlobCover.POST_FLOODING, GlobCover.RAINFED_CROPLANDS,
        GlobCover.MOSAIC_CROPLAND, GlobCover.MOSAIC_VEGETATION]
FOREST = [GlobCover.CLOSED_TO_OPEN_BROADLEAVED_FOREST, GlobCover.CLOSED_BROADLEAVED_FOREST,
          GlobCover.OPEN_BROADLEAVED_FOREST, GlobCover.CLOSED_NEEDLELEAVED_FOREST,
          GlobCover.OPEN_NEEDLELEAVED_FOREST, GlobCover.CLOSED_TO_OPEN_MIXED_FOREST,
          GlobCover.MOSAIC_FOREST, GlobCover.CLOSED_TO_OPEN_REGULARLY_FLOODED_FOREST,
          GlobCover.CLOSED_TO_OPEN_REGULARLY_FLOODED_GRASSLAND]
VEGETATION = [GlobCover.MOSAIC_GRASSLAND, GlobCover.CLOSED_TO_OPEN_SHRUBLAND,
              GlobCover.CLOSED_TO_OPEN_HERBS, GlobCover.SPARSE_VEGETATION,
              GlobCover.CLOSED_TO_OPEN_REGULARLY_FLOODED_GRASSLAND]
BARE = [GlobCover.BARE_AREAS]


class ProtectedArea(IntEnum):
    """Derived from UNEP-WCMC data set."""
    PROTECTED = 255
    NOT_PROTECTED = 0


@click.command()
@click.argument("path_to_land_cover")
@click.argument("path_to_protected_areas")
@click.argument("path_to_slope")
@click.argument("path_to_bathymetry")
@click.argument("path_to_result")
def determine_eligible_land(path_to_land_cover, path_to_protected_areas, path_to_slope,
                            path_to_bathymetry, path_to_result):
    """Determines eligibility of land for renewables."""
    with rasterio.open(path_to_land_cover) as src:
        raster_affine = src.affine
        land_cover = src.read(1)
        crs = src.crs
    with rasterio.open(path_to_slope) as src:
        slope = src.read(1)
    with rasterio.open(path_to_protected_areas) as src:
        protected_areas = src.read(1)
    with rasterio.open(path_to_bathymetry) as src:
        bathymetry = src.read(1)
    eligibility = determine_eligibility(land_cover, protected_areas, slope, bathymetry)
    with rasterio.open(path_to_result, 'w', driver='GTiff', height=eligibility.shape[0],
                       width=eligibility.shape[1], count=1, dtype=DATATYPE,
                       crs=crs, transform=raster_affine) as new_geotiff:
        new_geotiff.write(eligibility, 1)


def determine_eligibility(land_cover, protected_areas, slope, bathymetry):
    eligibility = np.ones_like(land_cover, dtype=DATATYPE) * Eligibility.NOT_ELIGIBLE
    eligibility[land_cover == GlobCover.ARTIFICAL_SURFACES_AND_URBAN_AREAS] = \
        Eligibility.ROOFTOP_PV
    eligibility[(np.isin(land_cover, FARM + VEGETATION + BARE)) &
                (protected_areas == ProtectedArea.NOT_PROTECTED) &
                (slope <= MAX_SLOPE_PV)] = Eligibility.ONSHORE_WIND_OR_PV_FARM
    eligibility[(np.isin(land_cover, FARM + VEGETATION + BARE)) &
                (protected_areas == ProtectedArea.NOT_PROTECTED) &
                (slope <= MAX_SLOPE_WIND) & (slope > MAX_SLOPE_PV)] = Eligibility.ONSHORE_WIND_FARM
    eligibility[(np.isin(land_cover, FOREST)) &
                (protected_areas == ProtectedArea.NOT_PROTECTED) &
                (slope <= MAX_SLOPE_WIND)] = Eligibility.ONSHORE_WIND_FARM
    eligibility[(land_cover == GlobCover.WATER_BODIES) &
                (protected_areas == ProtectedArea.NOT_PROTECTED) &
                (bathymetry > -50)] = Eligibility.OFFSHORE_WIND_FARM
    return eligibility


if __name__ == "__main__":
    determine_eligible_land()