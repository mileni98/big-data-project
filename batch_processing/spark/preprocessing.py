import os
#import shapely.geometry as sg
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


# Initialize spark
spark = SparkSession \
    .builder \
    .appName("Spark Preprocessing") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


# Read earthquake dataset
df_batch = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/batch_data.csv")

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
df_batch = df_batch.withColumn("time", col("time").cast(DateType())) \
    .withColumn("latitude", col("latitude").cast(FloatType())) \
    .withColumn("longitude", col("longitude").cast(FloatType())) \
    .withColumn("depth", col("depth").cast(FloatType())) \
    .withColumn("magnitude", col("magnitude").cast(FloatType())) \
    .withColumn("horizontal_error", col("horizontal_error").cast(FloatType())) \
    .withColumn("depth_error", col("depth_error").cast(FloatType())) \
    .withColumn("magnitude_error", col("magnitude_error").cast(FloatType())) \


df_batch.show()

# Read tectonic plate dataset
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
df_tect_plates = df_tect_plates.withColumn("latitude", col("latitude").cast(FloatType())) \
    .withColumn("longitude", col("longitude").cast(FloatType()))


'''
# Function that returns plate name based on coordinates, using Shapely polygons
@pandas_udf(StringType())
def plate_name(latitude, longitude):
    coordinate = sg.Point(longitude, latitude)
    for index, row in df_tect_plates.iterrows():
        polygon = sg.Polygon(zip(row["longitude"], row["latitude"]))
        if coordinate.within(polygon):
            return row["plate"]
    return "None"

df_batch = df_batch.withColumn("plate", plate_name(
    df_batch["latitude"], df_batch["longitude"]))

df_batch.show()
'''


# Save earthquake dataset to HDFS
df_batch.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/batch_data.csv")


# Save earthquake dataset to HDFS
df_tect_plates.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/tectonic_boundaries.csv")
