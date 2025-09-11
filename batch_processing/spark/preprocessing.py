import os
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Initialize spark
spark = SparkSession.builder.appName("Spark Preprocessing").getOrCreate()
quiet_logs(spark)

# --- Sedona SQL registration (needed for ST_* functions) ---
spark._jvm.org.apache.sedona.sql.utils.SedonaSQLRegistrator.registerAll(spark._jsparkSession)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# ============================
# Read earthquake dataset
# ============================
df_batch = (spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/batch_data.csv"))

# Select and cast
df_batch = (df_batch.select(
        col("time").alias("time"),
        col("latitude").alias("latitude"),
        col("longitude").alias("longitude"),
        col("depth").alias("depth"),
        col("mag").alias("magnitude"),
        col("type").alias("type"),
        col("horizontalError").alias("horizontal_error"),
        col("depthError").alias("depth_error"),
        col("magError").alias("magnitude_error"),
        col("locationSource").alias("location_source"),
    )
    .withColumn("time", col("time").cast(DateType()))
    .withColumn("latitude", col("latitude").cast(FloatType()))
    .withColumn("longitude", col("longitude").cast(FloatType()))
    .withColumn("depth", col("depth").cast(FloatType()))
    .withColumn("magnitude", col("magnitude").cast(FloatType()))
    .withColumn("horizontal_error", col("horizontal_error").cast(FloatType()))
    .withColumn("depth_error", col("depth_error").cast(FloatType()))
    .withColumn("magnitude_error", col("magnitude_error").cast(FloatType()))
)

print("\n>> Showing Preprocessed Batch Data...")
time.sleep(1)
df_batch.show(5, truncate=False)

# ============================
# Read tectonic plate dataset
# ============================
df_tect_plates = (spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/tectonic_boundaries.csv"))

df_tect_plates = (df_tect_plates.select(
        col("plate").alias("plate_name"),
        col("lat").cast(DoubleType()).alias("latitude"),
        col("lon").cast(DoubleType()).alias("longitude")
    )
)

print("\n>> Showing Preprocessed Tectonic Plates Points...")
time.sleep(1)
df_tect_plates.show(5, truncate=False)

# ============================
# Build a polygon per plate (convex hull of its points)
# ============================
df_pts_geom = (df_tect_plates
    .select(
        "plate_name",
        expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))").alias("geom")
    )
)

df_plate_polygons = (df_pts_geom
    .groupBy("plate_name")
    .agg(expr("ST_ConvexHull(ST_Union_Aggr(geom))").alias("geom"))
)

# ============================
# Add containing_plate to earthquakes
# ============================
df_quakes_geom = df_batch.withColumn(
    "geom", expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))")
)

df_with_plate = (df_quakes_geom.alias("q")
    .join(df_plate_polygons.alias("p"),
          expr("ST_Contains(p.geom, q.geom)"),
          "left")
    .select(col("q.*"), col("p.plate_name").alias("containing_plate"))
)

print("\n>> Earthquakes with containing_plate (sample)")
df_with_plate.select("time","latitude","longitude","containing_plate").show(10, truncate=False)

print("\n>> Saving preprocessed data to HDFS...")

# Save earthquakes WITH the new column (same path as before)
(df_with_plate
    .drop("geom")  # geometry objects can't be saved to CSV
    .write.mode("overwrite").option("header", "true")
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/batch_data.csv"))

# Save plate points (unchanged)
(df_tect_plates
    .write.mode("overwrite").option("header", "true")
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/tectonic_boundaries.csv"))

print("\n>> Preprocessed data saved to HDFS.")
