
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from shapely.geometry import Point, Polygon

# Initialize Spark Session
spark = SparkSession.builder.appName('GeoCheck').getOrCreate()

# Define the schema for the points DataFrame
points_schema = StructType([
    StructField('x', DoubleType(), False),
    StructField('y', DoubleType(), False),
    StructField('name', StringType(), False)
])

# Define the Polygons DataFrame
polygons_list = [
    ([(0, 0), (2, 0), (2, 2), (0, 2)], 'Polygon A'),  # Original polygon
    ([(2,0), (4, 0), (4,2), (2,2)], 'Polygon B'),  # Original polygon, updated to not overlap
    ([(0,2), (2, 2), (2, 4), (0,4)], 'Polygon C'),  # New polygon, to the right of A
    ([(2, 2), (4,2), (4,4), (2,4)], 'Polygon D')   # New polygon, above A
]

# Convert polygon list to a broadcast variable
broadcast_polygons_list = spark.sparkContext.broadcast(polygons_list)

# Define a sample DataFrame with points using the explicitly defined schema
points_df = spark.createDataFrame([
    Row(x=0.5, y=0.5, name='Point A'),  # Inside Polygon A
    Row(x=1.5, y=1.5, name='Point B'),  # Inside Polygon B
    Row(x=0.25, y=0.25, name='Point C'), # Inside Polygon A
    Row(x=2.2, y=1.3, name='Point D'),  # Inside Polygon C
    Row(x=3.8, y=1.0, name='Point E'),  # Inside Polygon C
    Row(x=1.0, y=3.8, name='Point F'),  # Inside Polygon D
    Row(x=3.0, y=3.0, name='Point G')   # Not inside any defined polygon
], schema=points_schema)

# UDF to check which polygon a point belongs to
@udf(StringType())
def point_in_which_polygon(x, y):
    point = Point(x, y)
    for polygon_coordinates, polygon_name in broadcast_polygons_list.value:
        if Polygon(polygon_coordinates).contains(point):
            return polygon_name
    return 'Not in any polygon'

# Add a column showing which polygon each point belongs to
points_with_polygon = points_df.withColumn('polygon_name', point_in_which_polygon(col('x'), col('y')))

points_with_polygon.show()

# Stop the Spark session
spark.stop()



