# Starting containers using Docker Compose in the background.
echo 
cd ../batch_processing
echo "> Starting containers using Docker Compose..."
docker-compose up -d
sleep 5

# Copying upload script to the Namenode container.
echo
cd ../upload_data
echo "> Copying upload script to namenode..."
docker cp upload_hdfs.sh namenode:/upload_hdfs.sh
sleep 5

# Copying earthquake data and tectonic boundaries CSV files to the Namenode container.
echo 
cd ../data
echo "> Copying data to namenode..."
docker cp earthquake_data.csv namenode:/batch_data.csv
docker cp tectonic_plate_boundaries.csv namenode:/tectonic_boundaries.csv
sleep 5

# Executing the upload to HDFS commands.
echo 
echo "> Executing upload to HDFS commands..."
docker exec namenode bash ./upload_hdfs.sh
sleep 5

# Copying the PostgreSQL JAR file to the Spark Master container.
echo 
echo "> Copying PostgreSQL JAR file to Spark Master..."
docker cp postgresql-42.7.0.jar spark-master:./postgresql-42.7.0.jar
sleep 5

# Prompting to press any key to exit.
echo
read -n 1 -s -r -p "> Press any key to exit..."
echo
echo


