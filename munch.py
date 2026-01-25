import geopandas as gpd


meadows = gpd.read_file('qgis-grass-meadows.geojson')

owners = gpd.read_file('parcelles-des-personnes-morales.geojson')

result = gpd.sjoin(meadows, owners, how='inner', predicate='intersects')

projected = result.to_crs('EPSG:2154')

result['computed_surface_m2'] = projected.geometry.area
filtered = result[result['computed_surface_m2'] > 5000]
filtered.to_file('meadows_owners.geojson', driver='GeoJSON')

# result2 = gpd.sjoin(owners, meadows, how='inner', predicate='intersects')
# result2.to_file('owners_meadows.geojson', driver='GeoJSON')
