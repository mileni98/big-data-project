from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, collect_list, struct, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, ArrayType
from shapely.geometry import Polygon, Point
import pyspark.sql.functions as F

# Function to suppress logs
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Initialize Spark Session
spark = SparkSession.builder.appName('GeoCheck').getOrCreate()

quiet_logs(spark)

# Define the schema for the polygon vertices DataFrame
polygon_schema = StructType([
    StructField('x', DoubleType(), False),
    StructField('y', DoubleType(), False),
    StructField('name', StringType(), False)
])

# Sample DataFrame with polygon vertices
polygon_vertices = [
    (0.0, 0.0, 'Polygon A'),  # Explicitly make coordinates floating-point numbers
    (2.0, 0.0, 'Polygon A'),
    (2.0, 2.0, 'Polygon A'),
    (0.0, 2.0, 'Polygon A'),
    (2.0, 0.0, 'Polygon B'),
    (4.0, 0.0, 'Polygon B'),
    (4.0, 2.0, 'Polygon B'),
    (2.0, 2.0, 'Polygon B'),
    (0.0,2.0, 'Polygon C'),
    (2.0,2.0, 'Polygon C'),
    (2.0,4.0,'Polygon C'),
    (0.0,4.0,'Polygon C'),
    (2.0,2.0,'Polygon D'),
    (4.0,2.0,'Polygon D'),
    (4.0,4.0,'Polygon D'),
    (2.0,4.0,'Polygon D')
]

polygons_df = spark.createDataFrame(polygon_vertices, schema=polygon_schema)

polygons_df.show()
# Group by polygon name and aggregate vertices into a list
polygons_grouped = polygons_df.groupBy('name').agg(collect_list(struct('x', 'y')).alias('vertices'))

# Show the grouped polygons
polygons_grouped.show()

# Broadcast the grouped polygon data
broadcast_polygons_list = spark.sparkContext.broadcast(polygons_grouped.collect())

# Show the broadcasted polygons
print(broadcast_polygons_list.value)

# Define the schema for the points DataFrame
points_schema = StructType([
    StructField('x', DoubleType(), False),
    StructField('y', DoubleType(), False),
    StructField('name', StringType(), False)
])

# Sample DataFrame with points
points_df = spark.createDataFrame([
    Row(x=0.5, y=0.5, name='Point A'),  # Inside Polygon A
    Row(x=1.5, y=1.5, name='Point B'),  # Inside Polygon B
    Row(x=0.25, y=0.25, name='Point C'), # Inside Polygon A
    Row(x=2.2, y=1.3, name='Point D'),  # Inside Polygon B
    Row(x=3.8, y=1.0, name='Point E'),  # Inside Polygon B
    Row(x=1.0, y=3.8, name='Point F'),  # Inside Polygon D (doesn't exist in given vertices)
    Row(x=3.0, y=3.0, name='Point G')   # Not inside any defined polygon
], schema=points_schema)


# UDF to check which polygon a point belongs to
@udf(StringType())
def point_in_which_polygon(x, y):
    point = Point(x, y)
    for polygon in broadcast_polygons_list.value:
        polygon_name, vertices = polygon['name'], polygon['vertices']
        polygon_shape = Polygon([(vertex['x'], vertex['y']) for vertex in vertices])
        if polygon_shape.contains(point):
            return polygon_name
    return 'Not in any polygon'

# Add a column showing which polygon each point belongs to
points_with_polygon = points_df.withColumn('polygon_name', point_in_which_polygon(col('x'), col('y')))

points_with_polygon.show()

# Stop the Spark session
spark.stop()
