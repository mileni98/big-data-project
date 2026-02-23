from preprocessing import parse_stream

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery


TOPIC = "tsunamis"


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


def load_kafka_stream(topic: str = TOPIC) -> DataFrame:
    """Load the Kafka stream from the topic into a Spark DataFrame."""
    return spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
        .option("subscribe", topic) \
        .load()


def run_query_1(df_stream: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_2(df_stream: DataFrame) -> DataFrame:
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


def main() -> None:
    """Main entrypoint for the consumer."""

    print("\n>> Loading Kafka stream...")
    df_stream_raw = load_kafka_stream(TOPIC)

    print("\n>> Parsing stream...")
    df_stream = parse_stream(df_stream_raw)
    
    print("\n>> Building streaming queries...")
    #df_query_1 = run_query_1(df_stream)
    #df_query_2 = run_query_2(df_stream)
    #df_query_3 = run_query_3(df_stream)
    #df_query_4 = run_query_4(df_stream)
    #df_query_5 = run_query_5(df_stream)    

    # Write results to elastic searc and start the streaming processing
    print("\n>> Writing results to Elasticsearch...")
    query = write_stream_to_elasticsearch(df_stream, index="query")
    #query_1 = write_stream_to_elasticsearch(df_query_1, index="query_1")
    #query_2 = write_stream_to_elasticsearch(df_query_2, index="query_2")
    #query_3 = write_stream_to_elasticsearch(df_query_3, index="query_3")
    #query_4 = write_stream_to_elasticsearch(df_query_4, index="query_4")
    #query_5 = write_stream_to_elasticsearch(df_query_5, index="query_5")
    
    # Debug
    query = write_stream_to_console(df_stream)
    #query_1 = write_stream_to_console(df_query_1)
    #query_2 = write_stream_to_console(df_query_2)
    #query_3 = write_stream_to_console(df_query_3)
    #query_4 = write_stream_to_console(df_query_4)
    #query_5 = write_stream_to_console(df_query_5)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
