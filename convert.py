import geopandas as gpd

gdf = gpd.read_file('MOS_2022/vect_occsol_mos_2022_com_amp.shp')

gdf.to_file('mos_2022.geojson', driver='GeoJSON')