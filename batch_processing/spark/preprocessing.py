import os
import time

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


# Initialize spark
spark = SparkSession \
    .builder \
    .appName("Spark Preprocessing") \
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
    

def preprocess_batch_data(df_batch: DataFrame) -> DataFrame:
    """Preprocess the earthquake dataset with column selection, type casting and add a geometry column needed for spatial join."""

    # Select needed columns, rename them and cast to appropriate types
    df_batch = df_batch.select(
        col("id"),
        to_timestamp("time").alias("time"),
        col("latitude").cast(FloatType()),
        col("longitude").cast(FloatType()),
        col("depth").cast(FloatType()),
        col("mag").cast(FloatType()).alias("magnitude"),
        col("type"),
        col("horizontalError").cast(FloatType()).alias("horizontal_error"),
        col("depthError").cast(FloatType()).alias("depth_error"),
        col("magError").cast(FloatType()).alias("magnitude_error"),
        col("locationSource").alias("location_source"),
    )
        
    # Add geometry column using Sedona's ST_Point function
    return df_batch.withColumn(
        "location_geometry",
        expr("ST_Point(longitude, latitude)")
    )


def preprocess_plates_data(df_plates: DataFrame) -> DataFrame:
    """Preprocess the tectonic plates dataset by selecting needed columns, converting WKT to geometry and computing boundary geometry for distance calculations later."""

    # Select all columns and rename them
    df_plates = df_plates.select(
        col("plate_name"),
        col("wkt")
    )
    
    # Get geometry for getting plate, and boundry for distance calculation
    return df_plates \
        .withColumn("geometry", expr("ST_GeomFromWKT(wkt)")) \
        .withColumn("boundary_geometry", expr("ST_Boundary(geometry)")) \
        .drop("wkt") # This is geometry storet as text


def assign_plate_to_earthquakes(df_batch: DataFrame, df_plates: DataFrame) -> DataFrame:
    """Assign the containing plate for each earthquake using a spatial join"""
    return df_batch \
        .join(
            broadcast(df_plates), # Copy to all workers as the dataset is small
            expr("ST_Covers(geometry, location_geometry)"), # ST_Contains can be as well
            "left"
        ) \
        .select(
            df_batch["*"],
            df_plates["plate_name"], 
            df_plates["boundary_geometry"] # Add so the distance can be calculated
        )
    

def compute_distance_to_boundary(df_batch: DataFrame) -> DataFrame:
    """Compute the distance to the plate boundary for each earthquake using ST_DistanceSphere from the ST_ClosesPoint on the boundary."""
    return df_batch \
        .withColumn(
            "distance_km",
            expr("""
                ST_DistanceSphere(
                    location_geometry,
                    ST_ClosestPoint(boundary_geometry, location_geometry)
                ) / 1000
            """)
        ) \
        .drop("boundary_geometry")
    

def save_dataframe_to_hdfs_parquet(df: DataFrame, hdfs_path: str) -> None:
    """Save a DataFrame to HDFS as a parquet file."""
    df.write \
        .mode("overwrite") \
        .parquet(HDFS_NAMENODE + hdfs_path)


def main() -> None:
    """Main entrypoint for the preprocessing job."""
    
    start = time.time()
    
    print("\n>> Loading batch data from HDFS...")
    df_batch = load_csv_into_df("/user/root/data-lake/raw/batch_data.csv")
    df_batch.show(5)
    
    print("\n>> Loading tectonic plates data from HDFS...")
    df_plates = load_csv_into_df("/user/root/data-lake/raw/plate_polygons_wkt.csv")
    df_plates.show(5)
    
    print("\n>> Preprocessing batch data...")
    df_batch = preprocess_batch_data(df_batch)

    print("\n>> Preprocessing plates data...")
    df_plates = preprocess_plates_data(df_plates)
    
    print("\n>> Assigning containing plate for each earthquake...")
    df_batch = assign_plate_to_earthquakes(df_batch, df_plates)
    
    print("\n>> Computing distance to the plates boundary...")
    df_batch = compute_distance_to_boundary(df_batch)
    df_batch.show(5)
    
    print("\n>> Saving processed batch data to HDFS...")
    save_dataframe_to_hdfs_parquet(df_batch, "/user/root/data-lake/transform/batch_data_parquet")
    
    print("\n>> Saving tectonic plates data to HDFS...")
    save_dataframe_to_hdfs_parquet(df_plates, "/user/root/data-lake/transform/plate_polygons_wkt_parquet")

    print(f"\n>> Preprocessing finished in {time.time() - start:.2f} seconds.")

    
if __name__ == "__main__":
    main()



