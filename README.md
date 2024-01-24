# big-data-project

1. Go to "batch_processing" folder and run command to start containers:

```
docker-compose up --build
```
see running containers: docker ps


2. Go to "upload_data" folder and run command to upload data to HDFS:

```
./docker_upload.sh
```

if there is an error, run the following lines before ./docker_upload.sh

```
sudo apt-get install dos2unix
dos2unix docker_upload.sh
dos2unix upload_hdfs.sh
```

data will appear in namenode: http://localhost:9870/explorer.html#/user/root/data-lake. If there is an error about safe mode, just run the command again.

3. To open spark master bash go to "batch_processing" folder and run command:

```
docker-compose exec -it spark-master bash
docker-compose exec spark-master bash
```
without -it when running in wsl

4. To run preprocessing script in bash run command:

```
./spark/bin/spark-submit /home/preprocessing.py
```

5. To run processing script in bash run command:

```
./spark/bin/spark-submit /home/processing.py
```
./spark/bin/spark-submit --driver-class-path postgresql-42.7.0.jar /home/processing.py
.



docker-compose logs -f producer
see producer logs




TO SEE if data was written to postgresql:
```
docker exec -it batch_processing_postgresql_1 psql -U postgres -d big_data
SELECT * FROM public.price_decline;
docker exec -it postgresql psql -U postgres -d big_data

```
.

.

.

.

explained attributes: https://earthquake.usgs.gov/data/comcat/index.php#locationSource

https://stackoverflow.com/questions/70082601/how-can-i-find-the-country-using-latitude-longitude-information-using-pyspark

check types

column_data_types = df_tect_plates.dtypes

for column, data_type in column_data_types:
print(f"Column '{column}' has data type '{data_type}'")

Go to "batch_processing and run command: docker cp preprocessing.py spark-master:/preprocessing.py, it is automated now, in cd home














dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart

PS C:\Windows\system32> wsl --install -d Ubuntu-22.04
aleksa 1234

wsl --status
previous will install ubuntu command line, install in visual studio WSL extension to acces wsl terminal
in ubuntru type:
sudo apt-get update
sudo apt-get install docker-compose-plugin

Move wsl do D drive - https://www.youtube.com/watch?v=13jo3ppi7a0&ab_channel=TroubleChute
wsl --list
wsl --shutwodn
wsl
wsl --set-version Ubuntu-22.04 2



#Let's consider an example scenario where you might need to use windowing functions in your earthquake data analysis. Suppose you want to find the top N earthquakes in terms of magnitude for each location source. In this case, you would need to use the row_number() window function to assign ranks to earthquakes within each location source based on their magnitude. Here's an example query:
#
#python
#Copy code
#from pyspark.sql.window import Window
#
## Define a Window specification based on "location_source" and ordering by "magnitude" in descending order
#windowSpec = Window.partitionBy("location_source").orderBy(col("magnitude").desc())
#
## Add a new column "rank" using the row_number() window function
#df_ranked_earthquakes = df_batch.withColumn("rank", row_number().over(windowSpec))
#
## Select the top N earthquakes for each location source
#top_n_earthquakes = df_ranked_earthquakes.filter(col("rank") <= 3)
#
#top_n_earthquakes.show()
#In this example, the query uses windowing to rank earthquakes within each "location_source" partition based on their magnitude in descending order. The row_number() window function is used to assign a unique rank to each earthquake within its partition. The result is then filtered to select only the top N earthquakes for each location source (in this case, the top 3).
#
#This is just one example of a scenario where windowing functions could be useful. Depending on your specific analytical requirements, you might encounter situations where you need to perform calculations or aggregations over a specific window of rows within partitions of your data.





from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create a Spark session
spark = SparkSession.builder.appName("SparkGeometryExample").getOrCreate()

# Sample data
coordinates_data = {
    'plate': ['a', 'a', 'a', 'a', 'a', 'b', 'b', 'b', 'b', 'b'],
    'lat': [10, 10, 20, 20, 10, 20, 20, 30, 30, 20],
    'lon': [10, 20, 20, 10, 10, 20, 30, 30, 20, 20]
}

df_coordinates = spark.createDataFrame(pd.DataFrame(coordinates_data))

# Sample additional points data
additional_points_data = {
    'lat': [14, 15],
    'lon': [18, 20]
}

df_points = spark.createDataFrame(pd.DataFrame(additional_points_data))

# Define a DataFrame for additional points
df_points = df_points.selectExpr("lat as point_lat", "lon as point_lon")

# Use Spark functions for geometric operations
df_result = df_points.crossJoin(df_coordinates).withColumn(
    "inside_square",
    expr("ST_Contains(ST_PolygonFromEnvelope(array(lat, lon, lat + 1, lon + 1)), ST_Point(point_lon, point_lat))")
)

# Show the result
df_result.show()





https://github.com/IBMDataScience/sample-notebooks/blob/master/CloudPakForData/notebooks/Spatial%20Queries%20in%20PySpark.ipynb