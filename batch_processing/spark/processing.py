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
    # Read the earthquake dataset from HDFS
    return spark.read.parquet(HDFS_NAMENODE + hdfs_path)
    
    
def save_dataframe_to_postgres(df: DataFrame, table_name: str) -> None:
    """Save a Spark DataFrame to a PostgreSQL table."""
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
        

def run_query_2(df_batch: DataFrame) -> DataFrame:
    """What are the minimum and maximum depths and magnitudes recorded for earthquakes in the 20th and 21th centuries?"""
    # Filter by earhquake type
    df_filtered = df_batch.filter(col("type") == "earthquake")
    
    # Extract century from timestamp
    df_century = df_filtered.withColumn(
        "century",
        when(year(col("time")).between(1901, 2000), "20th")
        .when(year(col("time")).between(2001, 2100), "21st")
    )

    return df_century.groupBy("century").agg(
        min("depth").alias("minimum_depth"),
        max("depth").alias("maximum_depth"),
        min("magnitude").alias("minimum_magnitude"),
        max("magnitude").alias("maximum_magnitude")
    ).orderBy("century")


def run_query_3(df_batch: DataFrame) -> DataFrame:
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


def run_query_4(df_batch: DataFrame) -> DataFrame:
    """Are strongerd earthquakes statistically closer to the tectonic plate boundaries?"""
    pass


def run_query_5(df_batch: DataFrame) -> DataFrame:
    """Does increasing number of stations reduce uncertainty in earthquake magnitude and depth estimation?"""
    pass


def run_query_6(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_7(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_8(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_9(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_10(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def main() -> None:
    """Main entrypoint for the processing job."""
    
    start = time.time()
    
    print("\n>> Loading batch data from HDFS...")
    df_batch = load_parquet_into_df("/user/root/data-lake/transform/batch_data_parquet")
    df_batch.show(5)
    
    print("\n>> Loading tectonic plates data from HDFS...")
    df_plates = load_parquet_into_df("/user/root/data-lake/transform/plate_polygons_wkt_parquet")
    df_plates.show(5)
    
    print("\n>> Running Query 1: 'What is the total number of earthquakes with magnitude over 5 for each decade grouped into ranges?'...")
    df_query_1 = run_query_1(df_batch)
    df_query_1.show()
    
    print("\n>> Running Query 2: 'What are the minimum and maximum depths and magnitudes recorded for earthquakes in the 20th and 21th centuries?'...")
    df_query_2 = run_query_2(df_batch)
    df_query_2.show()
    
    print("\n>> Running Query 3: 'Does the change of seasons influence the frequency and intensity of earthquakes across tectonic plates?'...")
    df_query_3 = run_query_3(df_batch)
    df_query_3.show()
    
    print("\n>> Running Query 4: 'query_text'...")
    df_query_4 = run_query_4(df_batch)
    
    print("\n>> Running Query 5: 'query_text'...")
    df_query_5 = run_query_5(df_batch)
    
    print("\n>> Running Query 6: 'query_text'...")
    df_query_6 = run_query_6(df_batch)
    
    print("\n>> Running Query 7: 'query_text'...")
    df_query_7 = run_query_7(df_batch)
    
    print("\n>> Running Query 8: 'query_text'...")
    df_query_8 = run_query_8(df_batch)
    
    print("\n>> Running Query 9: 'query_text'...")
    df_query_9 = run_query_9(df_batch)
    
    print("\n>> Running Query 10: 'query_text'...")
    df_query_10 = run_query_10(df_batch)
    
    print(f"\n>> Processing finished in {time.time() - start:.2f} seconds.")

if __name__ == "__main__":
    main()





