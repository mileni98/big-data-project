import os

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from preprocessing import preprocess_stream_data


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


# Initialize spark
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

# Register Sedona UDTs and UDFs
spark._jvm.org.apache.sedona.sql.utils.SedonaSQLRegistrator.registerAll(spark._jsparkSession)

TOPIC = "tsunamis"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


def load_kafka_stream(topic: str = TOPIC, starting_offset: str = "latest") -> DataFrame:
    """Load the Kafka stream from the topic into a Spark DataFrame."""
    return spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offset) \
        .load()

#.startingOffsets("earlies") \

def load_parquet_into_df(hdfs_path: str) -> DataFrame:
    """Load a parquet file from HDFS into a Spark DataFrame."""
    return spark.read.parquet(HDFS_NAMENODE + hdfs_path)


def load_postgres_to_dataframe(table_name: str) -> DataFrame:
    """Load a PostgreSQL table into a Spark DataFrame."""
    return spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://postgresql:5432/big_data') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', f'public.{table_name}') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .load()


def write_stream_to_elasticsearch(df_stream: DataFrame, index: str) -> StreamingQuery:
    """Write the stream to Elasticsearch."""
    return df_stream.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", f"/tmp/es-checkpoints-{index}/") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", index) \
        .start()


def write_stream_to_console(df_stream: DataFrame) -> StreamingQuery:
    """Write the stream to the console for debugging."""
    return df_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()


def run_query_1(df_stream: DataFrame, df_plates: DataFrame) -> DataFrame:
    """What is the number of tsunami events and their maximum and average intensity in 1 minute windows per tectonic plate (top 10 most active plates)."""
    
    # Assign in which plate the event happened
    df_stream_with_plate = df_stream \
        .join(
            broadcast(df_plates), # Copy to all workers as dataset is small
            expr("ST_Contains(geometry, location_geometry)"),
            "left"
        )
        
    # Compute calculations over 1 minute window
    df_aggregated = df_stream_with_plate \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("plate_name"),
        ) \
        .agg(
            count("*").alias("tsunami_count_1m"),
            max("ts_intensity").alias("max_intensity_1m"),
            avg("ts_intensity").alias("avg_intensity_1m"),
        ) \
        # For live leaderboard uncomment, change mode to 'complete' and disable writing to Elastic Search
        #.orderBy(desc("tsunami_count_1m")) \
        #.limit(10) \
        #.orderBy("window")
            
    return df_aggregated.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "plate_name",
        "tsunami_count_1m",
        "max_intensity_1m",
        "avg_intensity_1m",
    )
    
    
def run_query_2(df_stream: DataFrame, df_plates: DataFrame) -> DataFrame:
    """query_text"""
    pass    

def run_query_3(df_stream: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_4(df_stream: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_5(df_stream: DataFrame) -> DataFrame:
    """query_text"""
    pass


def main() -> None:
    """Main entrypoint for the consumer."""
    
    print("\n>> Loading tectonic plates data from HDFS...")
    df_plates = load_parquet_into_df("/user/root/data-lake/transform/plate_polygons_wkt_parquet")
    df_plates.printSchema()
    
    print("\n>> Loading Kafka stream...")
    df_stream_raw = load_kafka_stream(TOPIC)
    df_stream_raw.printSchema()
    
    print("\n>> Preprocessing the stream data...")
    df_stream = preprocess_stream_data(df_stream_raw)
    df_stream.printSchema()

    print("\n>> Running queries on the stream...")
    df_query_1 = run_query_1(df_stream, df_plates)
    #df_query_2 = run_query_2(df_stream, df_plates)
    #df_query_3 = run_query_3(df_stream)
    #df_query_4 = run_query_4(df_stream)
    #df_query_5 = run_query_5(df_stream)    

    # Write results to elastic searc and start the streaming processing
    query_1_es = write_stream_to_elasticsearch(df_query_1, index="query_1")
    #query_2_es = write_stream_to_elasticsearch(df_query_2, index="query_2")
    #query_3_es = write_stream_to_elasticsearch(df_query_3, index="query_3")
    #query_4_es = write_stream_to_elasticsearch(df_query_4, index="query_4")
    #query_5_es = write_stream_to_elasticsearch(df_query_5, index="query_5")
    
    # Debug to console
    query_1_debug = write_stream_to_console(df_query_1)
    #query_2_debug = write_stream_to_console(df_query_2)
    #query_3_debug = write_stream_to_console(df_query_3)
    #query_4_debug = write_stream_to_console(df_query_4)
    #query_5_debug = write_stream_to_console(df_query_5)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
