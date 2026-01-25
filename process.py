
import geopandas as gpd
from pathlib import Path    


inp = Path('data/MOS_2022/vect_occsol_mos_2022_com_amp.shp')
out = Path('front/mos_2022_occsol_filtered.geojson')

owners = gpd.read_file('data/parcelles-des-personnes-morales.geojson')
owners = owners.to_crs('EPSG:4326')

gdf = gpd.read_file(str(inp))

mask_lvl = ~gdf['niv1_2022'].astype(str).isin({'1','4', '5'})
gdf_lvl = gdf.loc[mask_lvl]

mask_loc = gdf_lvl['codeinsee'].astype(str).str.startswith('13055')
gdf_loc = gdf_lvl.loc[mask_loc]

mask_size = gdf_loc['area_m2'] > 5000
filtered = gdf_loc.loc[mask_size]

filtered = filtered.to_crs('EPSG:4326')

result = gpd.sjoin(filtered, owners, how='inner', predicate='intersects')

result.to_file(str(out), driver='GeoJSON', mode='w')
