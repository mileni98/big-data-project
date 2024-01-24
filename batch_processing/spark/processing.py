import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


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

HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']

# Read earthquake dataset
df_batch = spark.read \
    .option('delimiter', ',') \
    .option('header', 'true') \
    .csv(HDFS_NAMENODE + '/user/root/data-lake/transform/batch_data.csv')

# Read tectonic plate dataset
df_tect_plates = spark.read \
    .option('delimiter', ',') \
    .option('header', 'true') \
    .csv(HDFS_NAMENODE + '/user/root/data-lake/transform/tectonic_boundaries.csv')


# Batch processing querries
print('\n\n--- 1. Does the change of seasons influence the frequency and intensity of earthquakes? ---')
query_1 = df_batch.withColumn('month', month(col('time'))) \
    .filter(col('type') == 'earthquake') \
    .groupBy('month') \
    .agg( 
        count('*'). alias('total_occurances'),
        avg('magnitude').alias('average_magnitude')
    ) \
    .orderBy('month')

query_1.show()


print('\n\n--- 2. Total number of earthquakes with magnitude over 8 for each decade. ---')
earth_over_8 = df_batch.withColumn('occurance_year', year(col('time'))) \
    .filter(col('magnitude') >= 8) \
    .filter(col('type') == 'earthquake') \
    .withColumn('decade', floor(col('occurance_year') / 10) * 10) \
    .groupBy('decade') \
    .agg(
        count('*').alias('total_ocurrances')) \
    .orderBy(desc('decade')) \

earth_over_8.show()

earth_over_8.write \
  .format('jdbc') \
  .option('url', 'jdbc:postgresql://postgresql:5432/big_data') \
  .option('driver', 'org.postgresql.Driver') \
  .option('dbtable', 'public.price_decline') \
  .option('user', 'postgres') \
  .option('password', 'postgres') \
  .mode('overwrite') \
  .save()


# Add a new column for the season based on the month
df_batch = df_batch.withColumn("month", month(col("time")))

# Define seasons based on months
df_batch = df_batch.withColumn("season", when((col("month") >= 3) & (col("month") <= 5), "Spring")
                                 .when((col("month") >= 6) & (col("month") <= 8), "Summer")
                                 .when((col("month") >= 9) & (col("month") <= 11), "Fall")
                                 .otherwise("Winter"))

# Analyze the influence of seasons on earthquake frequency and intensity
print("\n\n--- 1. Does the change of seasons influence the frequency and intensity of earthquakes? ---")
earthquakes_by_season = df_batch.filter(col("type") == "earthquake") \
    .groupBy("season") \
    .agg(
        count("*").alias("total_occurrences"),
        avg("magnitude").alias("average_magnitude")
    ) \
    .orderBy("season")

earthquakes_by_season.show()


from pyspark.sql.functions import dayofyear, when

# Add a new column for the day of the year
df_batch = df_batch.withColumn("day_of_year", dayofyear(col("time")))

# Define seasons based on days of the year
df_batch = df_batch.withColumn("season", when((col("day_of_year") >= 80) & (col("day_of_year") < 172), "Spring")
                                 .when((col("day_of_year") >= 172) & (col("day_of_year") < 264), "Summer")
                                 .when((col("day_of_year") >= 264) & (col("day_of_year") < 355), "Fall")
                                 .otherwise("Winter"))

# Analyze the influence of seasons on earthquake frequency and intensity
print("\n\n--- 1. Does the change of seasons influence the frequency and intensity of earthquakes? ---")
earthquakes_by_season = df_batch.filter(col("type") == "earthquake") \
    .groupBy("season") \
    .agg(
        count("*").alias("total_occurrences"),
        avg("magnitude").alias("average_magnitude")
    ) \
    .orderBy("season")

earthquakes_by_season.show()
