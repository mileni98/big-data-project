# Creating docker network.
echo
echo "> Creating docker network 'big_data_network'..."
docker network create big_data_network
sleep 3

# Starting containers using Docker Compose in the background.
echo 
cd ../batch_processing
echo "build custom docker"
docker build -t custom-spark-master:latest .
sleep 3

echo "> Starting containers using Docker Compose..."
docker-compose up -d
sleep 3

# Copying upload script to the Namenode container.
echo
cd ../setup
echo "> Copying upload script to namenode..."
docker cp upload_hdfs.sh namenode:/upload_hdfs.sh
sleep 3

# Copying earthquake data and tectonic boundaries CSV files to the Namenode container.
echo 
cd ../data
echo "> Copying data to namenode..."
docker cp earthquake_data.csv namenode:/batch_data.csv
docker cp tectonic_plate_boundaries.csv namenode:/tectonic_boundaries.csv
sleep 3

docker cp hospitals.csv namenode:/hospitals.csv
docker cp countries.csv namenode:/countries.csv
sleep 3

# Wait until HDFS is out of safe mode.
echo
while docker exec namenode hdfs dfsadmin -safemode get | grep -q "Safe mode is ON"; do
    echo "> Waiting for HDFS to leave safe mode..."
    sleep 20
done

# Executing the upload to HDFS commands.
echo "> Executing upload to HDFS commands..."
docker exec namenode bash ./upload_hdfs.sh
sleep 3

# Copying the PostgreSQL JAR file to the Spark Master container.
echo 
echo "> Copying PostgreSQL JAR file to Spark Master..."
docker cp postgresql-42.7.0.jar spark-master:./postgresql-42.7.0.jar
sleep 3

# Copying the Geospark JAR files to the Spark Master container.
echo
echo "> Copying Geospark JAR files to Spark Master..."
#docker cp geospark-1.3.1.jar spark-master:./geospark-1.3.1.jar
#docker cp geospark-sql-1.3.1.jar spark-master:./geospark-sql-1.3.1.jar
#docker cp geospark-viz-1.3.1.jar spark-master:./geospark-viz-1.3.1.jar
docker cp sedona-python-adapter-3.0_2.12-1.4.0.jar spark-master:./sedona-python-adapter-3.0_2.12-1.4.0.jar
docker cp sedona-core-3.0_2.12-1.4.0.jar spark-master:./sedona-core-3.0_2.12-1.4.0.jar
docker cp sedona-viz-3.0_2.12-1.4.0.jar spark-master:./sedona-viz-3.0_2.12-1.4.0.jar
sleep 3

# Fininshing cluster setup.
echo
echo "> Cluster ready for use."
echo
sleep 3
