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
            count("*").alias("tsunami_count"),
            max("ts_intensity").alias("max_intensity"),
            avg("ts_intensity").alias("avg_intensity"),
        ) \
        # For live leaderboard uncomment, change mode to 'complete' and disable writing to Elastic Search
        #.orderBy(desc("tsunami_count")) \
        #.limit(10) \
        #.orderBy("window")
            
    return df_aggregated.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "plate_name",
        "tsunami_count",
        "max_intensity",
        "avg_intensity",
    )
    

def run_query_2(df_stream: DataFrame) -> DataFrame:
    """What are the average damage cost and houses destroyed by tsunamis per region (Mediteranian, Alaskan...)?"""
    # Convert damages to numbers
    df_stream_numeric = df_stream \
        .withColumn(
            "damage_severity",
            when(col("damage_total_description") == "Limited (<$1 million)", 1)
            .when(col("damage_total_description") == "Moderate (~$1 to $5 million)", 2)
            .when(col("damage_total_description") == "Severe (~$5 to $24 million)", 3)
            .when(col("damage_total_description") == "Extreme (~$25 million or more)", 4)
            .otherwise(None)
        ) \
        .withColumn(
            "houses_severity",
            when(col("houses_total_description") == "Few (~1 to 50 houses)", 1)
            .when(col("houses_total_description") == "Some (~51 to 100 houses)", 2)
            .when(col("houses_total_description") == "Many (~101 to 1000 houses)", 3)
            .when(col("houses_total_description") == "Very Many (~1001 or more houses)", 4)
            .otherwise(None)
        )
        
    # Aggregate per region over 1 minute window, rounded
    df_aggregated = df_stream_numeric \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("region")
        ) \
        .agg(
            count("*").alias("tsunami_count"),
            count("damage_severity").alias("damage_reports"),
            round(avg("damage_severity"), 0).alias("avg_damage_severity"),
            count("houses_severity").alias("house_reports"),
            round(avg("houses_severity"), 0).alias("avg_houses_severity")
        )
    
    # COnvert numbers back to descriptions
    df_result = df_aggregated \
        .withColumn(
            "avg_damage_severity",
            when(col("avg_damage_severity") == 1, "Limited (<$1 million)")
            .when(col("avg_damage_severity") == 2, "Moderate (~$1 to $5 million)")
            .when(col("avg_damage_severity") == 3, "Severe (~$5 to $24 million)")
            .when(col("avg_damage_severity") == 4, "Extreme (~$25 million or more)")
        ) \
        .withColumn(
            "avg_houses_severity",
            when(col("avg_houses_severity") == 1, "Few (~1 to 50 houses)")
            .when(col("avg_houses_severity") == 2, "Some (~51 to 100 houses)")
            .when(col("avg_houses_severity") == 3, "Many (~101 to 1000 houses)")
            .when(col("avg_houses_severity") == 4, "Very Many (~1001 or more houses)")
        )
        
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "region",
        "tsunami_count",
        "damage_reports",
        "avg_damage_severity",
        "house_reports",
        "avg_houses_severity"
    )


def run_query_3(df_stream: DataFrame) -> DataFrame:
    """What is the most common source of tsunami origin by country (earhtuqake, volcano, landslide...)?"""
    df_result = df_stream \
        .withWatermark("timestamp", "10 seconds") \
        .groupby(
            window(col("timestamp"), "1 minute"),
            col("country"),
            col("cause")
        ) \
        .agg(
            count("*").alias("source_count")
        )
        # For live leaderboard uncomment, change mode to 'complete' and disable writing to Elastic Search
        #.orderBy(desc("tsunami_count")) \
        #.limit(10) \
        #.orderBy("window")
        
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "country",
        "cause",
        "source_count"
    )
    

def run_query_4(df_stream: DataFrame, df_plates: DataFrame, df_plate_statistics: DataFrame) -> DataFrame:
    """What is the average magnitude and depth for an earthquake that produced tsunami per plate, enrichened with plates historical seismic activity?"""     
    
    # Filter only earthquake caused tsunamis
    df_filtered = df_stream.filter(col("cause") == "Earthquake")
    
    # Assign in which plate tsunami was recorded
    df_stream_with_plates = df_filtered.join(
        broadcast(df_plates), # Cpy to all workers as dataset is small
        expr("ST_CONTAINS(geometry, location_geometry)"),
        "left"
    )
    
    # Group by plate and aggreagete
    df_result = df_stream_with_plates \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("plate_name")
        ) \
        .agg(
            count("*").alias("count_1m"),
            avg("eq_magnitude").alias("avg_mag_1m"),
            avg("eq_depth").alias("avg_depth_1m")
        )
        
    # Enrich with plate statistics dataset
    df_result = df_result.join(
        broadcast(df_plate_statistics),
        "plate_name",
        "left"
    )
    
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "plate_name",
        "count_1m",
        "avg_mag_1m",
        "avg_depth_1m",
        col("total_occurrences").alias("hist_total"),
        col("average_magnitude").alias("hist_avg_mag"),
        col("max_magnitude").alias("hist_max_mag"),
        col("total_above_7").alias("hist_total_7+"),
        col("average_depth").alias("hist_avg_depth"),
        col("max_depth").alias("hist_max_depth"),
    )
        
        
def run_query_5(df_stream: DataFrame, df_plates: DataFrame, df_shared_borders: DataFrame, df_peak_plate_borders: DataFrame) -> DataFrame:
    """Are current near-boundary tsunami events (<100km from boundary) concentrated along historically most active tectonic plate borders (aggregation with query_9)?"""
     
    # Assign in which plate tsunami was recorded
    df_stream_with_plates = df_stream \
        .join(
            broadcast(df_plates),
            expr("ST_Contains(geometry, location_geometry)"),
            "left"
        )
                
    # Join with shared borders
    df_joined = df_stream_with_plates.join(
        broadcast(df_shared_borders),
        df_stream_with_plates["plate_name"] == df_shared_borders["plate"],
        "left"
    )
    
    # Calculate distances to border
    df_with_distances = df_joined.withColumn(
        "distance_to_border_km",
        expr("""
            ST_DistanceSphere(
                location_geometry,
                ST_ClosestPoint(shared_border, location_geometry)
            ) /1000
        """)
    )
    
    # Filter near border
    df_near_boundary = df_with_distances.filter(
        col("distance_to_border_km") < 100
    )
    
    # Safe measure
    df_near_boundary = df_near_boundary.dropDuplicates(["id"])
    
    # Rename peak historical border events
    df_peak_renamed = df_peak_plate_borders.select(
        col("plate").alias("hist_name"),
        col("neighbor_plate").alias("hist_neighbor_plate"),
        "total_occurrences"
    )
    
    # Join with historical border activity, from the peak year
    df_with_history = df_near_boundary.join(
        broadcast(df_peak_renamed),
        (df_near_boundary["plate_name"] == df_peak_renamed["hist_name"]) &
        (df_near_boundary["neighbor_plate"] == df_peak_renamed["hist_neighbor_plate"]),
        "left"
    )
    
    # Aggregate per window
    df_result = df_with_history \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            "plate_name",
            "neighbor_plate",
            "total_occurrences"
        ) \
        .agg(
            count("*").alias("near_boundary_count_1m"),
            avg("distance_to_border_km").alias("avg_km_to_border_1m")
        )    
        
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "plate_name",
        "neighbor_plate",
        col("total_occurrences").alias("total_earhquakes_peak_year"),
        "near_boundary_count_1m",
        "avg_km_to_border_1m"
    )        


def main() -> None:
    """Main entrypoint for the consumer."""
    
    print("\n>> Loading tectonic plates data from HDFS...")
    df_plates = load_parquet_into_df("/user/root/data-lake/transform/plate_polygons_wkt_parquet")
    df_plates.printSchema()
    
    print("\n>> Loading shared borders data from HDFS...")
    df_shared_borders = load_parquet_into_df("/user/root/data-lake/transform/shared_borders_parquet")
    
    print("\n>> Loading relevant postgre tables from batch processing... ")
    df_plate_statistics = load_postgres_to_dataframe("query_0_plate_statistics")
    df_peak_plate_borders = load_postgres_to_dataframe("query_9_peak_plate_borders")
    
    print("\n>> Loading Kafka stream...")
    df_stream_raw = load_kafka_stream(TOPIC)
    df_stream_raw.printSchema()
    
    print("\n>> Preprocessing the stream data...")
    df_stream = preprocess_stream_data(df_stream_raw)
    df_stream.printSchema()

    print("\n>> Running queries on the stream...")
    df_query_1 = run_query_1(df_stream, df_plates)
    df_query_2 = run_query_2(df_stream)
    df_query_3 = run_query_3(df_stream)
    df_query_4 = run_query_4(df_stream, df_plates, df_plate_statistics)
    df_query_5 = run_query_5(df_stream, df_plates, df_shared_borders, df_peak_plate_borders)    

    # Write results to elastic searc and start the streaming processing
    #query_1_es = write_stream_to_elasticsearch(df_query_1, index="query_1")
    #query_2_es = write_stream_to_elasticsearch(df_query_2, index="query_2")
    #query_3_es = write_stream_to_elasticsearch(df_query_3, index="query_3")
    #query_4_es = write_stream_to_elasticsearch(df_query_4, index="query_4")
    #query_5_es = write_stream_to_elasticsearch(df_query_5, index="query_5")
    
    # Debug to console
    #query_1_debug = write_stream_to_console(df_query_1)
    #query_2_debug = write_stream_to_console(df_query_2)
    #query_3_debug = write_stream_to_console(df_query_3)
    #query_4_debug = write_stream_to_console(df_query_4)
    query_5_debug = write_stream_to_console(df_query_5)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
