import pandas as pd
import geopandas as gpd
import shapely.geometry
import pycountry

DRIVER = "GeoJSON"


def dissolve_nuts3(path_to_csvs, path_to_units, path_to_gtc):
    """Dissolve NUTS3 data to EuroSPORES clusters."""

    def _get_alpha3(alpha2):
        if alpha2 == 'UK':
            alpha2 = 'GB'
        return pycountry.countries.get(alpha_2=alpha2).alpha_3

    locations = pd.read_excel(path_to_gtc, sheet_name="locations", header=0)
    # Any EuroSPORES clusters which are whole countries will be whole countries at the subregional scale too
    locations['NUTS3'] = locations.NUTS3.fillna(
        locations.Country.map(_get_alpha3)
    )
    locations.set_index('NUTS3', inplace=True)

    units = _update_units(path_to_units, locations)
    for path_to_csv in path_to_csvs:
        csv = _update_csv(path_to_csv, locations)
        csv.to_csv(path_to_csv.replace('subregional', 'eurospores'))

    units.to_file(
        path_to_units.replace('subregional', 'eurospores'), driver=DRIVER
    )


def _update_csv(path_to_csv, locations):

    _csv = pd.read_csv(path_to_csv, index_col=0, header=0)
    _csv['eurospores_id'] = (
        locations.reindex(_csv.index).EuroSPORES
    ).dropna()

    # We now deal with non-additive parameters (density and demand fraction)
    if '/population.csv' in path_to_csv:
        _csv['unit_area'] = _csv['population_sum'].div(_csv['density_p_per_km2'])
    elif '/demand.csv' in path_to_csv:
        _csv['industrial_demand'] = _csv['demand_twh_per_year'].mul(_csv['industrial_demand_fraction'])

    _csv = _csv.groupby('eurospores_id').sum()

    if '/population.csv' in path_to_csv:
        _csv['density_p_per_km2'] = _csv['population_sum'].div(_csv['unit_area'])
        _csv = _csv.drop('unit_area', axis=1)
    elif '/demand.csv' in path_to_csv:
        _csv['industrial_demand_fraction'] = _csv['industrial_demand'].div(_csv['demand_twh_per_year'])
        _csv = _csv.drop('industrial_demand', axis=1)

    _csv.index.rename('id', inplace=True)

    return _csv


def _update_units(path_to_units, locations):

    def _to_multi_polygon(geometry):
        if isinstance(geometry, dict):
            geometry = shapely.geometry.shape(geometry)
        if isinstance(geometry, shapely.geometry.polygon.Polygon):
            return shapely.geometry.MultiPolygon(polygons=[geometry])
        else:
            return geometry

    units = gpd.read_file(path_to_units).set_index('id')
    units['eurospores_id'] = (
        locations.reindex(units.index).EuroSPORES
    ).dropna()
    units.loc[~units.is_valid, 'geometry'] = \
    units.loc[~units.is_valid, 'geometry'].buffer(0)
    units = units.dissolve('eurospores_id')
    units.geometry = units.geometry.map(_to_multi_polygon)
    units.index.rename('id', inplace=True)
    units = units.reset_index()
    units.loc[units['type'].isnull(), 'name'] = 'eurospores_cluster'
    units['type'].fillna('eurospores_cluster', inplace=True)

    return units




if __name__ == "__main__":
    dissolve_nuts3(
        path_to_csvs=snakemake.input.csvs,
        path_to_units=snakemake.input.units,
        path_to_gtc=snakemake.input.gtc
    )