import os
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
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/batch_data.csv")

# Read tectonic plate dataset
df_tect_plates = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/tectonic_boundaries.csv")


# Batch processing querries
print("--- 2. Total number of earthquakes with magnitude over 8 for each decade. ---")
df_batch.withColumn("occurance_year", year(col("time"))) \
    .filter(col("magnitude") >= 8) \
    .filter(col("type") == "earthquake") \
    .withColumn("decade", floor(col("occurance_year") / 10) * 10) \
    .groupBy("decade") \
    .agg(
        count("*").alias("total_ocurrances")) \
    .orderBy(desc("decade")) \
    .show()
