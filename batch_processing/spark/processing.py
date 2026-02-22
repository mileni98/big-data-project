import os
import time

from pyspark.sql.types import *
from pyspark.sql.functions import *
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


def load_csv_into_df(hdfs_path: str) -> DataFrame:
    """Load a CSV file from HDFS into a Spark DataFrame."""
    # Read the earthquake dataset from HDFS
    return spark.read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .csv(HDFS_NAMENODE + hdfs_path)
    
    
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
    """query_text"""
    pass


def run_query_2(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_3(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_4(df_batch: DataFrame) -> DataFrame:
    """query_text"""
    pass


def run_query_5(df_batch: DataFrame) -> DataFrame:
    """query_text"""
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
    df_batch = load_csv_into_df("/user/root/data-lake/transform/batch_data.csv")
    df_batch.show(5)
    
    print("\n>> Loading tectonic plates data from HDFS...")
    df_plates = load_csv_into_df("/user/root/data-lake/transform/plate_polygons_wkt.csv")
    df_plates.show(5)
    
    print("\n>> Running Query 1: 'query_text'...")
    df_query_1 = run_query_1(df_batch)
    
    print("\n>> Running Query 2: 'query_text'...")
    df_query_2 = run_query_2(df_batch)
    
    print("\n>> Running Query 3: 'query_text'...")
    df_query_3 = run_query_3(df_batch)
    
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





