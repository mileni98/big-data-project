import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org'). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)

# Initialize spark
spark = SparkSession \
    .builder \
    .appName('Spark Preprocessing') \
    .getOrCreate()

quiet_logs(spark)

GeoSparkRegistrator.registerAll(spark)


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

hospital_df = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/hospitals.csv")

hospital_df = hospital_df \
    .withColumn("id", col("id").cast(IntegerType())) \
    .withColumn("name", col("name").cast(StringType())) \
    .withColumn("city", col("city").cast(StringType())) \
    .withColumn("state", col("state").cast(StringType())) \
    .withColumn("lon", col("lon").cast(DoubleType())) \
    .withColumn("lat", col("lat").cast(DoubleType()))

hospital_df.show(3)



counties_df = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/countries.csv")

counties_df = counties_df \
    .withColumn("NAME", col("NAME").cast(StringType())) \
    .withColumn("STATE_NAME", col("STATE_NAME").cast(StringType())) \
    .withColumn("POP2000", col("POP2000").cast(IntegerType())) \
    .withColumn("shape_WKT", col("shape_WKT").cast(StringType())) 

counties_df = counties_df.select(
    col("NAME").alias("NAME"),
    col("STATE_NAME").alias("STATE_NAME"),
    col("POP2000").alias("POP2000"),
    col("shape_WKT").alias("shape_WKT"),
)

counties_df.show(3)



hospital_df.createOrReplaceTempView("hospitals")
hospital_df = spark.sql("SELECT *, ST_Point(lon, lat) as location from hospitals")
hospital_df.show(3, False)

counties_df.createOrReplaceTempView('counties')
counties_df = spark.sql("SELECT NAME, STATE_NAME, POP2000, ST_WKTToSQL(shape_WKT) as shape from counties")
counties_df.show(3)


hospital_df.createOrReplaceTempView('hospitals')
counties_df.createOrReplaceTempView('counties')


# Perform the spatial join
result_df = spark.sql("""
SELECT 
    h.name AS hospital_name, 
    c.NAME AS county_name, 
    c.STATE_NAME AS state_name
FROM 
    hospitals AS h
JOIN 
    counties AS c 
ON 
    ST_Contains(c.shape, h.location)
""")

# Show the results
result_df.show()
