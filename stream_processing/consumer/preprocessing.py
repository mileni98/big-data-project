from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def preprocess_stream_data(df_stream_raw: DataFrame) -> DataFrame:
    """Parse the raw Kafka stream, extract the JSON string, and convert it to a structured DataFrame."""
    # Extract key and value from the kafka stream
    df_stream = df_stream_raw.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS json_str",
        "timestamp"
    )
    
    # Define the schema for the tsunami data, all as String
    tsunami_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("YEAR", StringType(), True),
        StructField("MONTH", StringType(), True),
        StructField("DAY", StringType(), True),
        StructField("HOUR", StringType(), True),
        StructField("MINUTE", StringType(), True),
        StructField("LATITUDE", StringType(), True),
        StructField("LONGITUDE", StringType(), True),
        StructField("LOCATION_NAME", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("REGION", StringType(), True),
        StructField("CAUSE", StringType(), True),
        StructField("EVENT_VALIDITY", StringType(), True),
        StructField("EQ_MAGNITUDE", StringType(), True),
        StructField("EQ_DEPTH", StringType(), True),
        StructField("TS_INTENSITY", StringType(), True),
        StructField("DAMAGE_TOTAL_DESCRIPTION", StringType(), True),
        StructField("HOUSES_TOTAL_DESCRIPTION", StringType(), True),
        StructField("DEATHS_TOTAL_DESCRIPTION", StringType(), True),
        #StructField("URL", StringType(), True),
        #StructField("COMMENTS", StringType(), True),
    ])

    # Parse JSON and assign schema
    df_stream_parsed = df_stream \
        .select(
            col("timestamp"),
            from_json(col("json_str"), tsunami_schema).alias("data"),
        ) \
        .select("timestamp", "data.*")
        
    # Rename all columns to lowercase 
    for column_name in df_stream_parsed.columns:
        df_stream_parsed = df_stream_parsed.withColumnRenamed(column_name, column_name.lower())
        
    # Cast appropriate types
    df_stream_parsed = df_stream_parsed \
        .withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) \
        .withColumn("day", col("day").cast("int")) \
        .withColumn("hour", col("hour").cast("int")) \
        .withColumn("minute", col("minute").cast("int")) \
        .withColumn("latitude", col("latitude").cast("float")) \
        .withColumn("longitude", col("longitude").cast("float")) \
        .withColumn("eq_magnitude", col("eq_magnitude").cast("float")) \
        .withColumn("eq_depth", col("eq_depth").cast("float")) \
        .withColumn("ts_intensity", col("ts_intensity").cast("float"))
        
    # Add geometry column using Sedona's ST_Point function
    return df_stream_parsed.withColumn(
        "location_geometry",
        expr("ST_Point(longitude, latitude)")
    )
