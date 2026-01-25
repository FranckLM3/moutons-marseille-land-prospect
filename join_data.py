import geopandas as gpd


all_land = gpd.read_file('mos_2022.geojson')
grass_meadows = all_land[all_land['landuse'].str.lower().isin(['grass', 'meadows'])]

owners = gpd.read_file('parcelles-des-personnes-morales.geojson')

result = gpd.sjoin(grass_meadows, owners, how='inner', predicate='intersects')

projected = result.to_crs('EPSG:2154')

result['computed_surface_m2'] = projected.geometry.area
filtered = result[result['computed_surface_m2'] > 5000]
filtered.to_file('meadows_owners_complete.geojson', driver='GeoJSON')