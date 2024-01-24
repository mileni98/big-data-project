import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC = "earthquakes"

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName('Stream Processing') \
    .getOrCreate()

quiet_logs(spark)

df_stream = spark.readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
    .option("subscribe", TOPIC) \
    .load()


# Display intermediate results for debugging
query = df_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
