from pyspark.sql import SparkSession
from shapely.geometry import Point, Polygon
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

# Initialize Spark Session
spark = SparkSession.builder.appName('GeoCheck').getOrCreate()

# Define a sample Polygon
polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

# UDF to check if a point is in the polygon
@udf(BooleanType())
def is_point_in_polygon(x, y):
    point = Point(x, y)
    return polygon.contains(point)

# Create a DataFrame with sample points
points_df = spark.createDataFrame([(0.5, 0.5, 'Point A'), (1.5, 1.5, 'Point B'), (0.25, 0.25, 'Point C')], ['x', 'y', 'name'])

# Use the UDF to add a column showing if points are inside the polygon
points_inside = points_df.withColumn('is_inside', is_point_in_polygon(col('x'), col('y')))

points_inside.show()

