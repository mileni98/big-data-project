import os
import time

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org'). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)
    

# Initialize spark
spark = SparkSession \
    .builder \
    .appName('Spark Processing') \
    .getOrCreate()

quiet_logs(spark)

# Register Sedona UDTs and UDFs
spark._jvm.org.apache.sedona.sql.utils.SedonaSQLRegistrator.registerAll(spark._jsparkSession)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


def load_parquet_into_df(hdfs_path: str) -> DataFrame:
    """Load a parquet file from HDFS into a Spark DataFrame."""
    return spark.read.parquet(HDFS_NAMENODE + hdfs_path)
    
    
def save_dataframe_to_postgres(df: DataFrame, table_name: str) -> None:
    """Save a Spark DataFrame to a PostgreSQL table. NOTE: Overwrites the table if it already exists."""
    df.write \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://postgresql:5432/big_data') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', f'public.{table_name}') \
        .option('user', 'postgres') \
        .option('password', 'postgres') \
        .mode('overwrite') \
        .save()


def run_query_1(df_batch: DataFrame) -> DataFrame:
    """How often do sonic_booms, quarry_blasts, nuclear_explosion and explosions occur each year? """
    df_years = df_batch.withColumn("year", year(col("time")))
    
    # Filter by type
    df_filtered = df_years.filter(
        (col("type") == "sonic_boom") |
        (col("type") == "quarry blast") |
        (col("type") == "nuclear explosion") |
        (col("type") == "explosion")
    )
    
    return df_filtered \
        .groupBy("year", "type") \
        .agg(count("*").alias("total_occurrences")) \
        .orderBy("year", "type")
        

def run_query_2(df_batch: DataFrame) -> DataFrame:
    """What is the total number of earthquakes with magnitude over 5 for each decade grouped into ranges?"""
    # FIlter by magnitude and type
    df_filtered = df_batch.filter(
        (col("magnitude") >= 5) & 
        (col("type") == "earthquake")
    )
        
    # Extract decade from timestamp
    df_decade = df_filtered.withColumn(
        "decade",
        floor(year(col("time")) / 10) * 10
    )
    
    return df_decade.groupBy("decade").agg(
        count(when((col("magnitude") >= 5) & (col("magnitude") < 6), True)).alias("mag_5.0-5.9"),
        count(when((col("magnitude") >= 6) & (col("magnitude") < 7), True)).alias("mag_6.0-6.9"),
        count(when((col("magnitude") >= 7) & (col("magnitude") < 8), True)).alias("mag_7.0-7.9"),
        count(when((col("magnitude") >= 8) & (col("magnitude") < 9), True)).alias("mag_8.0-8.9"),
        count(when(col("magnitude") >= 9, True)).alias("mag_9.0+")
    ).orderBy(desc("decade"))
        

def run_query_3(df_batch: DataFrame) -> DataFrame:
    """What are the maximum depths and magnitudes recorded for earthquakes in the 20th and 21th centuries, including where they occurred?"""
    # Filter by earhquake type
    df_filtered = df_batch.filter(col("type") == "earthquake")
    
    # Extract century from timestamp
    df_century = df_filtered.withColumn(
        "century",
        when(year(col("time")).between(1901, 2000), "20th")
        .when(year(col("time")).between(2001, 2100), "21st")
    )
    
    # Define windows
    windowSpec_depth = Window.partitionBy("century").orderBy(desc("depth"))
    windowSpec_magnitude = Window.partitionBy("century").orderBy(desc("magnitude"))
    
    # Get the max depth dataframe per century
    df_max_depth = df_century \
        .withColumn("rank_depth", row_number().over(windowSpec_depth)) \
        .filter(col("rank_depth") == 1) \
        .select(
            "century",
            col("depth").alias("maximum_depth"),
            col("plate_name").alias("plate_max_depth"),
            col("place").alias("place_max_depth")
        )
    
    # Get the max magnitude dataframe per century
    df_max_magnitude = df_century \
        .withColumn("rank_magnitude", row_number().over(windowSpec_magnitude)) \
        .filter(col("rank_magnitude") == 1) \
        .select(
            "century",
            col("magnitude").alias("maximum_magnitude"),
            col("plate_name").alias("plate_max_magnitude"),
            col("place").alias("place_max_magnitude")
        )

    return df_max_depth \
        .join(df_max_magnitude, "century", "outer") \
        .orderBy("century")


def run_query_4(df_batch: DataFrame) -> DataFrame:
    """Does the change of seasons influence the frequency and intensity of earthquakes across tectonic plates?"""
    # Filter by earhquake type
    df_filtered = df_batch.filter(col("type") == "earthquake")
    
    # Extract month and day from timestamp
    df_time = df_filtered \
        .withColumn('month', month(col('time'))) \
        .withColumn('day', dayofmonth(col('time')))

    # Define seasons
    df_time = df_time.withColumn('season', 
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
    
    # Aggregate per plate per season
    df_result = df_time.groupBy("plate_name", "season").agg(
        count("*").alias("total_occurrences"),
        avg("magnitude").alias("average_magnitude")
    )
    
    # WIindow each plate to order by frequency
    windowSpec = Window.partitionBy("plate_name").orderBy(desc("total_occurrences"))
    return df_result \
        .withColumn("season_rank", rank().over(windowSpec)) \
        .orderBy("plate_name", "season_rank")


def run_query_5(df_batch: DataFrame) -> DataFrame:
    """Are strongerd earthquakes statistically closer to the tectonic plate boundaries?"""
    # Filter by earhquake type and just in case non null distance values
    df_filtered = df_batch.filter(
        (col("type") == "earthquake") &
        (col("magnitude").isNotNull()) &
        (col("distance_km").isNotNull())
    )
    
    # Create magnitute categories
    df_magnitude = df_filtered.withColumn(
        "magnitude_category",
        when(col("magnitude") < 5, "5.0-")
        .when((col("magnitude") >= 5) & (col("magnitude") < 6), "5.0-5.9")
        .when((col("magnitude") >= 6) & (col("magnitude") < 7), "6.0-6.9")
        .when((col("magnitude") >= 7) & (col("magnitude") < 8), "7.0-7.9")
        .when((col("magnitude") >= 8) & (col("magnitude") < 9), "8.0-8.9")
        .when(col("magnitude") >= 9, "9.0+")
    )
    
    # Aggregate statiscitc
    return df_magnitude.groupBy("magnitude_category").agg(
        count("*").alias("total_occurrences"),
        avg("magnitude").alias("average_magnitude"),
        avg("distance_km").alias("average_distance_km"),
        stddev("distance_km").alias("stddev_distance_km")
    ).orderBy(desc("magnitude_category"))
    

def run_query_6(df_batch: DataFrame) -> DataFrame:
    """Does increasing number of stations reduce uncertainty in earthquake magnitude and depth estimation?"""
    # Filter by earhquake and non empty stations
    df_filtered = df_batch.filter(
        (col("type") == "earthquake") &
        (col("num_stations").isNotNull()) &
        (col("num_stations") > 0)
    )
    
    # Create station number categories
    df_result = df_filtered.withColumn(
        "station_category",
        when((col("num_stations") >=1) & (col("num_stations") < 3), "1-2")
        .when((col("num_stations") >= 3) & (col("num_stations") < 6), "3-5")
        .when((col("num_stations") >= 6) & (col("num_stations") < 10), "6-9")
        .when((col("num_stations") >= 10) & (col("num_stations") < 20), "10-19")
        .when((col("num_stations") >= 20) & (col("num_stations") < 50), "20-49")
        .when((col("num_stations") >= 50) & (col("num_stations") < 100), "50-99")
        .when((col("num_stations") >= 100) & (col("num_stations") < 200), "100-199")
        .when(col("num_stations") >= 200, "200+")
    )
    
    return df_result.groupBy("station_category").agg(
        count("*").alias("total_occurrences"),
        avg("num_stations").alias("average_num_stations"),
        avg("magnitude_error").alias("average_magnitude_error"),
        avg("depth_error").alias("average_depth_error")
    ).orderBy("average_num_stations")


def run_query_7(df_batch: DataFrame) -> DataFrame:
    """Has the accuracy and quality of seizmic measurement improved over the decades?"""
    df_filtered = df_batch.filter(col("type") == "earthquake")
    
    # Extract decade from timestamp
    df_decade = df_filtered.withColumn(
        "decade",
        floor(year(col("time")) / 10) * 10
    )
    
    return df_decade.groupBy("decade").agg(
        count("*").alias("total_occurrences"),
        avg("num_stations").alias("average_num_stations"),
        avg("magnitude_error").alias("average_magnitude_error"),
        avg("depth_error").alias("average_depth_error"),
        avg("horizontal_error").alias("average_horizontal_error")
    ).orderBy(desc("decade"))


def run_query_8(df_batch: DataFrame) -> DataFrame:
    """Which magnitute calculation method performs best for different magnitude ranges?"""
    df_filtered = df_batch.filter(
        (col("type") == "earthquake") &
        (col("magnitude_type").isNotNull()) &
        (col("magnitude_error").isNotNull()) &
        (col("magnitude") >= 0)
    )
    
    # Group magnitudes into ranges
    df_magnitude_ranges = df_filtered.withColumn(
        "magnitude_range",
        when((col("magnitude") >= 0) & (col("magnitude") < 2), "0.0-1.9")
        .when((col("magnitude") >= 2) & (col("magnitude") < 4), "2.0-3.9")
        .when((col("magnitude") >= 4) & (col("magnitude") < 6), "4.0-5.9")
        .when((col("magnitude") >= 6) & (col("magnitude") < 8), "6.0-7.9")
        .when((col("magnitude") >= 8), "8.0+")
    )
    
    # Compare methods
    df_stats = df_magnitude_ranges \
        .groupBy("magnitude_range", "magnitude_type") \
        .agg(
            count("*").alias("total_occurrences"),
            avg("magnitude_error").alias("average_magnitude_error"),
            stddev("magnitude_error").alias("stddev_magnitude_error")
        )
        
    # Remove smaller gorups as they might skew 
    df_stats = df_stats.filter(
        ((col("magnitude_range") == "0.0-1.9") & (col("total_occurrences") >= 200)) |
        ((col("magnitude_range") == "2.0-3.9") & (col("total_occurrences") >= 100)) |
        ((col("magnitude_range") == "4.0-5.9") & (col("total_occurrences") >= 50)) |
        ((col("magnitude_range") == "6.0-7.9") & (col("total_occurrences") >= 10)) |
        (col("magnitude_range") == "8.0+")   # keep all
    )
    
    # Define window to rank methods per magnitude range
    windowSpec = Window.partitionBy("magnitude_range").orderBy(asc("average_magnitude_error"))
    
    return df_stats \
        .withColumn("method_rank", row_number().over(windowSpec)) \
        .filter(col("method_rank") == 1) \
        .drop("method_rank") \
        .orderBy("magnitude_range")
        

def run_query_9(df_batch: DataFrame, df_shared_borders: DataFrame) -> DataFrame:
    """Which pairs of tectonic plates exhibit the highest seismic activity along their shared boundary during one random peak seismic year, most amount of earthquakes with magnitute>7? (Top 5)"""
    # Filter by earhquake
    df_filtered = df_batch.filter(
        (col("type") == "earthquake") &
        (col("plate_name").isNotNull()) &
        (col("magnitude") >= 0.1)
    )
    
    # Extract year from timestam
    df_time = df_filtered.withColumn("year", year(col("time")))
    
    # Get the peak seismic year
    df_peak_year = df_time \
        .filter(col("magnitude") > 7) \
        .groupBy("year") \
        .agg(count("*").alias("total_occurrences")) \
        .orderBy(desc("total_occurrences")) \
        .limit(1) \
        .select("year")
        
    # Could use .collect() and just get the year
    df_peak = df_time.join(df_peak_year, "year") 
    
    # Join with shared borders df
    df_joined = df_peak.join(
        broadcast(df_shared_borders),
        df_peak["plate_name"] == df_shared_borders["plate"],
    )
    
    # Calculate distance to all shared border segments
    df_joined = df_joined.withColumn(
        "distance_to_shared_border_km",
        expr("""
            ST_DistanceSphere(
                location_geometry,
                ST_ClosestPoint(shared_border, location_geometry)
            ) / 1000
        """)
    )
    
    # Define window to rank shared borders per earthquake
    windowSpec = Window.partitionBy("id").orderBy(asc("distance_to_shared_border_km"))
    
    return df_joined \
        .withColumn("border_rank", row_number().over(windowSpec)) \
        .filter(col("border_rank") == 1) \
        .groupBy("year", "plate", "neighbor_plate") \
        .agg(count("*").alias("total_occurrences")) \
        .orderBy(desc("total_occurrences")) \
        .limit(5)
    

def run_query_10(df_batch: DataFrame) -> DataFrame:
    """How does earthquake frequency evolve around a major earhtquake (magnitude > 7) in the radius of 100km?"""
    # Filter by earhquake
    df_filtered = df_batch.filter((col("type") == "earthquake"))
    
    # FIlter mainshocks
    df_mainshock = df_filtered \
        .filter(col("magnitude") >= 7) \
        .select(
            col("id").alias("mainshock_id"),
            col("time").alias("mainshock_time"),
            col("location_geometry").alias("mainshock_geometry")
        )

    # Join mainshocks with all earthquakes fo find within range
    df_joined = df_mainshock.join(
        df_filtered,
        (datediff(df_filtered["time"], df_mainshock["mainshock_time"]) >= -3) &
        (datediff(df_filtered["time"], df_mainshock["mainshock_time"]) <= 7) &
        (expr("""
            ST_DistanceSphere(
                mainshock_geometry,
                location_geometry
            ) <= 100000
        """)
        )
    )

    # Calculate day difference 
    df_result = df_joined.withColumn(
        "day_diff",
        datediff(col("time"), col("mainshock_time"))
    )
    
    return df_result.groupBy("day_diff").agg(
        count("*").alias("total_occurrences"),
        #avg("magnitude").alias("average_magnitude") # Takes too much time
    ).orderBy("day_diff")
   

def run_save_show(df: DataFrame, table_name: str) -> None:
    """Helper function to save a dataframe to postgres and show it."""
    df_cached = df.cache()  # Cache to prevent 2 executions on both save and show
    save_dataframe_to_postgres(df_cached, table_name)
    df_cached.show()
    df_cached.unpersist() # Free memory after use


def main() -> None:
    """Main entrypoint for the processing job."""
    
    start = time.time()
    
    print("\n>> Loading batch data from HDFS...")
    df_batch = load_parquet_into_df("/user/root/data-lake/transform/batch_data_parquet")
    df_batch.show(5)
    
    print("\n>> Loading tectonic plates data from HDFS...")
    df_plates = load_parquet_into_df("/user/root/data-lake/transform/plate_polygons_wkt_parquet")
    df_plates.show(5)
    
    print("\n>> Loading helper shared borders data from HDFS...")
    df_shared_borders = load_parquet_into_df("/user/root/data-lake/transform/shared_borders_parquet")
    df_shared_borders.show(5)
    
    print("\n>> Running Query 1: 'How often do sonic_booms, quarry_blasts, nuclear_explosion and explosions occur each year?'...")
    run_save_show(run_query_1(df_batch), "query_1_yearly_explosion_counts")
            
    print("\n>> Running Query 2: 'What is the total number of earthquakes with magnitude over 5 for each decade grouped into ranges?'...")
    run_save_show(run_query_2(df_batch), "query_2_decade_magnitude_distribution")

    print("\n>> Running Query 3: 'What are the maximum depths and magnitudes recorded for earthquakes in he #20th and 21th centuries, including where they occurred?'...")
    run_save_show(run_query_3(df_batch), "query_3_century_extreme_earthquakes")

    print("\n>> Running Query 4: 'Does the change of seasons influence the frequency and intensity of earthquakes across tectonic plates?'...")
    run_save_show(run_query_4(df_batch), "query_4_seasonal_plate_activity")

    print("\n>> Running Query 5: 'Are stronger earthquakes statistically closer to the tectonic plate boundaries?'...")
    run_save_show(run_query_5(df_batch), "query_5_magnitude_distance_to_plate_boundary")

    print("\n>> Running Query 6: 'Does increasing number of stations reduce uncertainty in earthquake magnitude and depth estimation?'...")
    run_save_show(run_query_6(df_batch), "query_6_stations_error_analysis")

    print("\n>> Running Query 7: 'Has the accuracy and quality of seizmic measurement improved over the decades?'...")
    run_save_show(run_query_7(df_batch), "query_7_measurement_quality_over_decades")

    print("\n>> Running Query 8: 'Which magnitute calculation method performs best for different magnitude ranges?'...")
    run_save_show(run_query_8(df_batch), "query_8_magnitude_method_comparison")
    
    print("\n>> Running Query 9: 'Which pairs of tectonic plates exhibit the highest seismic activity along their shared boundary during one random peak seismic year, most amount of earthquakes with magnitute>7? (Top 5)'...")
    run_save_show(run_query_9(df_batch, df_shared_borders), "query_9_peak_plate_borders")
    
    print("\n>> Running Query 10: 'How does earthquake frequency evolve around a major earhtquake (magnitude > 7) in the radius of 100km?'...")
    run_save_show(run_query_10(df_batch), "query_10_aftershock_evolution")
    
    print(f"\n>> Processing finished in {time.time() - start:.2f} seconds.")   
    spark.stop()


if __name__ == "__main__":
    main()