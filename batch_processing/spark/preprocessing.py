import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from shapely.wkt import dumps, loads
from shapely.geometry import Polygon, Point

# Function to suppress logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Create a Spark session
spark = SparkSession \
    .builder \
    .appName("Spark Preprocessing") \
    .getOrCreate()

quiet_logs(spark)

# Get HDFS Namenode from environment variable
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Read earthquake dataset
df_batch = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/batch_data.csv") \
    .orderBy(desc("time")) \
    .limit(500)

# Select and Rename columns
df_batch = df_batch.select(
    col("time").alias("time"),
    col("latitude").alias("latitude"),
    col("longitude").alias("longitude"),
    col("depth").alias("depth"),
    col("mag").alias("magnitude"),
    col("type").alias("type"),
    col("horizontalError").alias("horizontal_error"),
    col("depthError").alias("depth_error"),
    col("magError").alias("magnitude_error"),
    col("locationSource").alias("location_source"))

# All columns are of String type (.dtypes), so converting types
df_batch = df_batch \
    .withColumn("time", col("time").cast(DateType())) \
    .withColumn("latitude", col("latitude").cast(FloatType())) \
    .withColumn("longitude", col("longitude").cast(FloatType())) \
    .withColumn("depth", col("depth").cast(FloatType())) \
    .withColumn("magnitude", col("magnitude").cast(FloatType())) \
    .withColumn("horizontal_error", col("horizontal_error").cast(FloatType())) \
    .withColumn("depth_error", col("depth_error").cast(FloatType())) \
    .withColumn("magnitude_error", col("magnitude_error").cast(FloatType())) \

# Show the dataframe
df_batch.show()

# Read tectonic plates dataset
df_tect_plates = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/tectonic_boundaries.csv")

# Select and Rename columns
df_tect_plates = df_tect_plates.select(
    col("plate").alias("plate_name"),
    col("lat").alias("latitude"),
    col("lon").alias("longitude"))

# All columns are of String type (.dtypes), so converting types
df_tect_plates = df_tect_plates \
    .withColumn("latitude", col("latitude").cast(FloatType())) \
    .withColumn("longitude", col("longitude").cast(FloatType()))

# Show the dataframe
df_tect_plates.show()

# Group by polygon name and aggregate vertices into a list
polygons_grouped = df_tect_plates.groupBy('plate_name').agg(collect_list(struct('latitude', 'longitude')).alias('vertices'))

# Broadcast the grouped polygon data
broadcast_polygons_list = spark.sparkContext.broadcast(polygons_grouped.collect())

# Show the broadcasted polygons
print(broadcast_polygons_list.value)

# UDF to check which polygon a point belongs to
@udf(StringType())
def point_in_which_polygon(x, y):
    point = Point(x, y)
    for polygon in broadcast_polygons_list.value:
        polygon_name, vertices = polygon['plate_name'], polygon['vertices']
        polygon_shape = Polygon([(vertex['latitude'], vertex['longitude']) for vertex in vertices])
        if polygon_shape.contains(point):
            return polygon_name
    return 'Not in any polygon'

# Add a column showing which polygon each point belongs to
df_batch = df_batch.withColumn('tectonic_plate', point_in_which_polygon(col('latitude'), col('longitude')))

df_batch.show()


## Write the batch dataframe to HDFS
df_batch.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/batch_data.csv")

# Write the tectonic plates dataframe to HDFS
df_tect_plates.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/tectonic_boundaries.csv")