import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org'). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)

# Initialize spark
spark = SparkSession \
    .builder \
    .appName('Spark Preprocessing') \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']


df_tect_plates = spark.read \
    .option('delimiter', ',') \
    .option('header', 'true') \
    .csv(HDFS_NAMENODE + '/user/root/data-lake/transform/tectonic_boundaries.csv')

# New target point (latitude and longitude)
new_target_latitude = 35.0
new_target_longitude = 135.0

window = Window.partitionBy("plate_name").orderBy("plate_name", "order") 


### Calculate distance to line formed by two consecutive points
##df_with_distance = df_tect_plates.withColumn(
##    "distance_to_line",
##    when((df_tect_plates["plate_name"]) != df_tect_plates["plate_name"].lag(), 10000)  
##    .when((df_tect_plates["latitude"] == df_tect_plates["latitude"].lag()) & 
##        (df_tect_plates["longitude"] == df_tect_plates["longitude"].lag()), 10000)
##    .otherwise(
##        abs(
##            (func.col("longitude") - new_target_longitude) * (func.col("latitude") - func.col("latitude").lag()) -
##            (func.col("latitude") - new_target_latitude) * (func.col("longitude") - func.col("longitude").lag())
##        ) / sqrt(pow(func.col("latitude") - func.col("latitude").lag(), 2) + pow(func.col("longitude") - func.col("longitude").lag(), 2))
##    )
##)
#
#
#df_with_distance = df_tect_plates.withColumn(
#    "distance_to_line",
#   when(
#       ~(df_tect_plates["plate_name"] == lag("plate_name", -1).over(window)),
#       10000
#   )
#   .when(
#       (df_tect_plates["latitude"] == lag("latitude", -1).over(window)) & 
#       (df_tect_plates["longitude"] == lag("longitude", -1).over(window)), 
#       10000
#   )
#   # .otherwise(
#   #     abs(
#   #         (df_tect_plates["longitude"] - new_target_longitude) * (df_tect_plates["latitude"] - func.lag("latitude").over(Windowspec)) -
#   #         (df_tect_plates["latitude"] - new_target_latitude) * (df_tect_plates["longitude"] - func.lag("longitude").over(Windowspec))
#   #     ) / sqrt(pow(df_tect_plates["latitude"] - func.lag("latitude").over(Windowspec), 2) + pow(df_tect_plates["longitude"] - func.lag("longitude").over(Windowspec), 2))
#   # )
#   .otherwise(
#        abs(
#            (col("longitude") - new_target_longitude) * (col("latitude") - lag("latitude", -1).over(window)) -
#            (col("latitude") - new_target_latitude) * (col("longitude") - lag("longitude", -1).over(window))
#        ) / sqrt(pow(col("latitude") - lag("latitude", -1).over(window), 2) + pow(col("longitude") - lag("longitude", -1).over(window), 2))
#   )
#)
#
#
#
#
## Calculate distance to line segment formed by two consecutive points
#df_with_distance = df_tect_plates.withColumn(
#    "distance_to_line_segment",
#    when(
#        (df_tect_plates["plate_name"] != lag("plate_name").over(window)) |
#        (df_tect_plates["latitude"] == lag("latitude").over(window)) &
#        (df_tect_plates["longitude"] == lag("longitude").over(window)),
#        10000
#    )
#    .otherwise(
#        when(
#            (df_tect_plates["latitude"] == new_target_latitude) &
#            (df_tect_plates["longitude"] == new_target_longitude),
#            0  # Point is on the line segment
#        )
#        .otherwise(
#            abs(
#                (col("longitude") - new_target_longitude) * (lag("latitude").over(window) - new_target_latitude) -
#                (col("latitude") - new_target_latitude) * (lag("longitude").over(window) - new_target_longitude)
#            ) / sqrt(pow(lag("latitude").over(window) - new_target_latitude, 2) + pow(lag("longitude").over(window) - new_target_longitude, 2))
#        )
#    )
#)
#
#
#
## Fill null values with 10000
#df_with_distance = df_with_distance.withColumn(
#    "distance_to_line",
#    when(
#        col("distance_to_line").isNull(), 
#        10000
#    ).otherwise(
#        col("distance_to_line").cast(DoubleType())
#    )
#)
#
#
#
#df_with_distance.orderBy("distance_to_line").show()
#
#
## Find the minimum distance to a line
#closest_point = df_with_distance.orderBy("distance_to_line").first()
#
## Display the result
#print("Closest Point to Line:")
#print(f"Plate: {closest_point['plate_name']}")
#print(f"Latitude: {closest_point['latitude']}")
#print(f"Longitude: {closest_point['longitude']}")
#print(f"Distance to Line: {closest_point['distance_to_line']}")
#
## Stop the Spark session
#spark.stop()



# NOVI TESTTTTTTTTTTTTTTTTTTT

# Calculate distance to segmented line formed by consecutive points
df_with_distance = df_tect_plates.withColumn(
    "distance_to_line_segment",
    when(
        (col("plate_name") != lag("plate_name").over(window)) |
        ((col("latitude") == lag("latitude").over(window)) &
         (col("longitude") == lag("longitude").over(window))),
        10000
    ).otherwise(
        when(
            (col("latitude") == new_target_latitude) &
            (col("longitude") == new_target_longitude),
            0  # Point is on the line segment
        ).otherwise(
            least(*[
                abs(
                    (col("longitude") - new_target_longitude) * (lag("latitude").over(window) - new_target_latitude) -
                    (col("latitude") - new_target_latitude) * (lag("longitude").over(window) - new_target_longitude)
                ) / sqrt(
                    pow(lag("latitude").over(window) - new_target_latitude, 2) +
                    pow(lag("longitude").over(window) - new_target_longitude, 2)
                )
                for window in [Window().rowsBetween(-1, 0)]  # Window for the current and previous rows
            ])
        )
    )
)

# Show the resulting DataFrame
df_with_distance.show()

# Stop the Spark session
spark.stop()


#RADIIIIIIIIIIIIIIIIIIIIIIIIIIII
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import func.col, pow, sqrt
#import os
#
#def quiet_logs(sc):
#    logger = sc._jvm.org.apache.log4j
#    logger.LogManager.getLogger('org'). setLevel(logger.Level.ERROR)
#    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)
#
## Initialize spark
#spark = SparkSession \
#    .builder \
#    .appName('Spark Preprocessing') \
#    .getOrCreate()
#
#quiet_logs(spark)
#
#HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']
#
#
#df_tect_plates = spark.read \
#    .option('delimiter', ',') \
#    .option('header', 'true') \
#    .csv(HDFS_NAMENODE + '/user/root/data-lake/transform/tectonic_boundaries.csv')
#
## New target point (latitude and longitude)
#new_target_latitude = 35.0
#new_target_longitude = 135.0
#
## Calculate distance without shapely
#df_with_distance = df_tect_plates.withfunc.column(
#    "distance",
#    sqrt(pow(func.col("latitude") - new_target_latitude, 2) + pow(func.col("longitude") - new_target_longitude, 2))
#)
#
#closest_point = df_with_distance.orderBy("distance").show()
#
## Find the closest coordinates
#closest_point = df_with_distance.orderBy("distance").first()
#
## Display the result
#print("Closest Coordinates:")
#print(f"Plate: {closest_point['plate_name']}")
#print(f"Latitude: {closest_point['latitude']}")
#print(f"Longitude: {closest_point['longitude']}")
#print(f"Distance: {closest_point['distance']}")
#
## Stop the Spark session
#spark.stop()















#from pyspark.sql import SparkSession
#from pyspark.sql.functions import func.col, pow, sqrt
#
## Create a Spark session
#spark = SparkSession.builder.appName("ClosestCoordinates").getOrCreate()
#
## Sample DataFrame with latitude and longitude func.columns
#data = [(1, 37.7749, -122.4194), (2, 34.0522, -118.2437), (3, 40.7128, -74.0060)]
#func.columns = ["id", "latitude", "longitude"]
#df = spark.createDataFrame(data, func.columns)
#
## Target point (latitude and longitude)
#target_latitude = 38.8951
#target_longitude = -77.0364
#
## Target point (latitude and longitude)
#target_latitude1 = 3.8951
#target_longitude1 = -7.0364
#
## Calculate distance
#df_with_distance = df.withfunc.column(
#    "distance",
#    sqrt(pow(func.col("latitude") - target_latitude, 2) + pow(func.col("longitude") - target_longitude, 2))
#)
#
#
## Find the closest coordinates
#closest_point = df_with_distance.orderBy("distance").first()
#
## Display the result
#print("Closest Coordinates:")
#print(f"ID: {closest_point['id']}")
#print(f"Latitude: {closest_point['latitude']}")
#print(f"Longitude: {closest_point['longitude']}")
#print(f"Distance: {closest_point['distance']}")
#
## Stop the Spark session
#spark.stop()
