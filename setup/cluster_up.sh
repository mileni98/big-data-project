# Create a Docker network for the cluster.
echo
echo "> Creating docker network 'big_data_network'..."
docker network create big_data_network
sleep 3

# Starting containers for the batch processing cluster.
echo 
cd ../batch_processing
echo "> Starting batch processing containers..."
docker-compose up -d
sleep 3

# Copying upload script to the Namenode container.
echo
cd ../setup
echo "> Copying HDFS upload script to namenode..."
docker cp upload_hdfs.sh namenode:/upload_hdfs.sh
sleep 3

# Copying earthquake data and tectonic boundaries CSV files to the Namenode container.
echo 
cd ../data
echo "> Copying data to namenode..."
docker cp earthquake_data.csv namenode:/batch_data.csv
docker cp plate_polygons_wkt.csv namenode:/plate_polygons_wkt.csv
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
docker cp postgresql-42.7.7.jar spark-master:./postgresql-42.7.7.jar
sleep 3

# Starting containers for the stream processing cluster.
cd ../stream_processing
echo
echo "> Starting stream processing containers..."
docker-compose up -d
sleep 3

# Fininshing cluster setup.
echo
echo "> Cluster ready for use."
echo
