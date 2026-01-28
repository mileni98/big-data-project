import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

TOPIC = "earthquakes"

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName('Stream Processing') \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1," 
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
    ) \
    .getOrCreate()

quiet_logs(spark)

df_stream_raw = spark.readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
    .option("subscribe", TOPIC) \
    .load()

# Cast key/value to STRING
df_stream = df_stream_raw.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS json_str",
    "timestamp"
)

test_schema = StructType([
    StructField("time", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("mag", StringType(), True)
])

df_parsed_test = df_stream \
    .select(from_json(col("json_str"), test_schema).alias("data")) \
    .select("data.*")
    
# Write to Elasticsearch
query = df_parsed_test.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/es-checkpoints/") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "earthquakes") \
    .start()

# Display intermediate results for debugging
#query = df_parsed_test.writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .option("truncate", "false") \
#    .start()

query.awaitTermination()
