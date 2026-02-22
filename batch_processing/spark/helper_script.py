import geopandas as gpd
from pathlib import Path

base_path = Path(__file__).parents(2)

# Read the plates file into a GeoDataFrame
plates_gdf = gpd.read_file(base_path / "data" / "PB2002_plates.shp")

# Set CRS
plates_gdf = plates_gdf.set_crs("EPSG:4326")

# Select only the necessary columns
plates_wkt_df = plates_gdf[["PlateName", "geometry"]].copy()

# Convert geometry to WKT
plates_wkt_df["wkt"] = plates_wkt_df.geometry.to_wkt()

# Rename and select final columns
plates_wkt_df = plates_wkt_df \
    .drop(columns=["geometry"]) \
    .rename(columns={PlateName: "plate_name"})

# Save to CSV
output.to_csv("plate_polygons_wkt.csv", index=False)