
import geopandas as gpd
from pathlib import Path

"""Process MOS shapefile and join with canonical owners GeoJSON (no CLI args)."""

print("loading data...")
inp = Path('data/MOS_2022/vect_occsol_mos_2022_com_amp.shp')
out = Path('front/mos_2022_occsol_filtered.geojson')

gdf = gpd.read_file(str(inp))

print("filtering land use... combining niv1 and niv4 logic")
# Whitelist pour niv4 (seulement pour niv1=1)
allowed_niv4 = {
"1123",
"1217",
"1219",
"1225",
"1234",
"1244",
"1311",
"1321",
"1331",
"1411",
"1412",
"1413",
"1414",
"1415",
"1421",
"1422"
}


mask_niv2_or_3 = gdf['niv1_2022'].astype(str).isin({'2', '3'})
mask_niv4 = ((gdf['niv4_2022'].astype(str).isin(allowed_niv4)))
mask_combined = mask_niv2_or_3 | mask_niv4
gdf_filtered = gdf.loc[mask_combined]

print(f"  → kept {len(gdf_filtered)} features (from {len(gdf)} total)")

# print("filtering land use... location")
# mask_loc = gdf_filtered['codeinsee'].astype(str).str.startswith('13055')
# gdf_loc = gdf_filtered.loc[mask_loc]

print("filtering land use... size")
mask_size = gdf_filtered['area_m2'] > 5000
filtered = gdf_filtered.loc[mask_size]
# === OPTIMIZATION: simplify geometries and keep essential columns before spatial join ===
print("optimizing data before spatial join...")

# keep only essential columns from MOS data
essential_mos_cols = [
    'niv1_2022', 'niv2_2022', 'niv3_2022', 'niv4_2022',  # classification
    'lib1_2022', 'lib2_2022', 'lib3_2022',               # labels
    'area_m2', 'area_ha',                                 # surface
    'codeinsee', 'nom',                                   # location
    'geometry'
]
cols_to_keep = [c for c in essential_mos_cols if c in filtered.columns]
filtered = filtered[cols_to_keep].copy()

# round numeric values
if 'area_m2' in filtered.columns:
    filtered['area_m2'] = filtered['area_m2'].round(0)
if 'area_ha' in filtered.columns:
    filtered['area_ha'] = filtered['area_ha'].round(2)

# simplify geometries in projected CRS (much faster before sjoin)
print("  → simplifying geometries (tolerance=1m)...")
filtered_proj = filtered.to_crs('EPSG:2154')  # Lambert 93
filtered_proj['geometry'] = filtered_proj['geometry'].simplify(tolerance=1, preserve_topology=True)
filtered = filtered_proj.to_crs('EPSG:4326')  # Back to WGS84

print(f"  → kept {len(filtered)} features with {len(filtered.columns)} columns")


print("reading parcelles des personnes morales...")
# use canonical owners file
owners_file = Path('data/parcelles-des-personnes-morales.geojson')
if not owners_file.exists():
    raise FileNotFoundError(f"Owners file not found: {owners_file}")
print(f"  → using owners file: {owners_file}")
owners = gpd.read_file(str(owners_file))
owners = owners.to_crs('EPSG:4326')

# Keep only essential columns from owners data for the join
# Map possible column names in owners file to the names we want
col_map = {}
if 'denomination' in owners.columns:
    col_map['denomination'] = 'denomination'
elif 'denom' in owners.columns:
    col_map['denomination'] = 'denom'

if 'numero_siren' in owners.columns:
    col_map['siren'] = 'numero_siren'
elif 'numero_siren' in owners.columns:
    col_map['siren'] = 'numero_siren'
elif 'siren' in owners.columns:
    col_map['siren'] = 'siren'

# Keep only columns we will use plus geometry
keep_cols = list(set(col_map.values())) + ['geometry']
keep_cols = [c for c in keep_cols if c in owners.columns]
owners = owners[keep_cols].copy()
# rename mapped columns to consistent names
owners = owners.rename(columns={v: k for k, v in col_map.items() if v in owners.columns})

print(f"spatial join with {len(owners)} owners parcelles...")
result = gpd.sjoin(filtered, owners, how='inner', predicate='intersects')

# === POST-JOIN CLEANUP ===
print("post-join cleanup...")

# Drop index_right column from sjoin
if 'index_right' in result.columns:
    result = result.drop(columns=['index_right'])

# Drop any duplicate columns
result = result.loc[:, ~result.columns.duplicated()]

print(f"final result: {len(result)} features with {len(result.columns)} columns")

# Ensure output directory exists
out.parent.mkdir(parents=True, exist_ok=True)

print(f"writing {len(result)} features to {out}...")
result.to_file(str(out), driver='GeoJSON', mode='w')
