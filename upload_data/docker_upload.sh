echo "Copying data and upload script to namenode..."
docker cp upload_hdfs.sh namenode:/upload_hdfs.sh

# Navigate to the data directory.
cd ../data

# Copying earthquake data and tectonic boundaries CSV files to the Namenode container.
docker cp earthquake_data.csv namenode:/batch_data.csv
docker cp tectonic_plate_boundaries.csv namenode:/tectonic_boundaries.csv

# Executing the upload to HDFS commands.
echo 
echo "Executing upload to HDFS commands..."
docker exec namenode bash ./upload_hdfs.sh

# Check if the upload_hdfs.sh script executed successfully.
if [ $? -eq 0 ]; then
    echo "Data upload to HDFS was successful."
else
    echo "Data upload to HDFS failed."
fi

# Copying the PostgreSQL JAR file to the Spark Master container.
echo 
echo "Copying PostgreSQL JAR file to Spark Master..."
docker cp postgresql-42.7.0.jar spark-master:./postgresql-42.7.0.jar

# Checking if the PostgreSQL JAR was copied successfully.
if [ $? -eq 0 ]; then
    echo "Copying PostgreSQL JAR file to Spark Master was successful."
else
    echo "Copying PostgreSQL JAR file to Spark Master failed."
fi

# Prompting to press any key to exit.
echo
read -n 1 -s -r -p "Press any key to exit..."


