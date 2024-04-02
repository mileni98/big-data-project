import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Function to suppress logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org'). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)

# Create a Spark session
spark = SparkSession \
    .builder \
    .appName('Spark Preprocessing') \
    .getOrCreate()

quiet_logs(spark)

# Get HDFS Namenode from environment variable
HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']

# Read earthquake dataset
df_batch = spark.read \
    .option('delimiter', ',') \
    .option('header', 'true') \
    .csv(HDFS_NAMENODE + '/user/root/data-lake/transform/batch_data.csv')

# Read tectonic plates dataset
df_tect_plates = spark.read \
    .option('delimiter', ',') \
    .option('header', 'true') \
    .csv(HDFS_NAMENODE + '/user/root/data-lake/transform/tectonic_boundaries.csv')



# ----------------------------------------------- Query 1 -----------------------------------------------
print('\n--- 1. Does the change of seasons influence the frequency and intensity of earthquakes? ---')
df_query1 = df_batch.withColumn('month', month(col('time')))
df_query1 = df_query1.withColumn('day', dayofmonth(col('time')))

# Define seasons
df_query1 = df_query1.withColumn('season', 
    when((col('month') == 3) & (col('day') >= 20), 'Spring')
    .when((col('month') == 4) | (col('month') == 5), 'Spring')
    .when((col('month') == 6) & (col('day') <= 20), 'Spring')
    
    .when((col('month') == 6) & (col('day') > 20), 'Summer')
    .when((col('month') == 7) | (col('month') == 8), 'Summer')
    .when((col('month') == 9) & (col('day') <= 22), 'Summer')

    .when((col('month') == 9) & (col('day') > 22), 'Fall')
    .when((col('month') == 10) | (col('month') == 11), 'Fall')
    .when((col('month') == 12) & (col('day') <= 21), 'Fall')

    .otherwise('Winter')
)

# Perform transformations
df_query1 = df_query1.filter(col('type') == 'earthquake') \
    .groupBy('season') \
    .agg(
        count('*').alias('total_occurrences'),
        avg('magnitude').alias('average_magnitude')
    ) \
    .orderBy(desc('total_occurrences'))

df_query1.show()


# ----------------------------------------------- Query 2 -----------------------------------------------
print('\n--- 2. Total number of earthquakes with magnitude over 8 for each decade. ---')
df_query2 = df_batch.withColumn('occurance_year', year(col('time')))

# Perform transformations
df_query2 = df_query2.filter(col('magnitude') >= 8) \
    .filter(col('type') == 'earthquake') \
    .withColumn('decade', floor(col('occurance_year') / 10) * 10) \
    .groupBy('decade') \
    .agg(
        count('*').alias('total_ocurrances')
    ) \
    .orderBy(desc('decade')) 

df_query2.show()

df_query2.write \
  .format('jdbc') \
  .option('url', 'jdbc:postgresql://postgresql:5432/big_data') \
  .option('driver', 'org.postgresql.Driver') \
  .option('dbtable', 'public.price_decline') \
  .option('user', 'postgres') \
  .option('password', 'postgres') \
  .mode('overwrite') \
  .save()


# ----------------------------------------------- Query 3 -----------------------------------------------
print('\n--- 3. What is the minimum and maximum depth recorded for earthquakes in the 20th and 21st centuries? ---')
df_query3 = df_batch.withColumn('century', 
    when(year(col('time')).between(1900, 1999), '20th')
    .when(year(col('time')).between(2000, 2099), '21st')
)

# Perform transformations
df_query3 = df_query3.filter(col('type') == 'earthquake') \
    .groupBy('century') \
    .agg(
        min('depth').alias('minimum_depth'),
        max('depth').alias('maximum_depth')
    ) \
    .orderBy('century') 
    
df_query3.show()


# ----------------------------------------------- Query 4 -----------------------------------------------
print('\n\n--- 4. How often do "sonic booms", "quarry blasts", "nuclear explosion" and "explosions" occur each year? ---')
df_query4 = df_batch.withColumn('occurance_year', year(col('time')))

df_query4 = df_query4.filter((col('type') == 'sonic boom') | (col('type') == 'quarry blast') | (col('type') == 'explosion') | (col('type') == 'nuclear explosion')) \
    .groupBy('occurance_year', 'type') \
    .agg(
        count('*').alias('total_occurrences')
    ) \
    .orderBy('occurance_year', 'type')

df_query4.show()


# ----------------------------------------------- Query 5 -----------------------------------------------
print('\n\n--- 5.? ---')



# ----------------------------------------------- Query 6 -----------------------------------------------
print('\n\n--- 6.? ---')



# ----------------------------------------------- Query 7 -----------------------------------------------
print('\n\n--- 7.? ---')



# ----------------------------------------------- Query 8 -----------------------------------------------
print('\n\n--- 8.? ---')



# ----------------------------------------------- Query 9 -----------------------------------------------
print('\n\n--- 9.? ---')



# ----------------------------------------------- Query 10 -----------------------------------------------
print('\n\n--- 10.? ---')









