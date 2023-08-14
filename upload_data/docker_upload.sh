echo "Copying data and upload script to namenode..."
docker cp upload_hdfs.sh namenode:/upload_hdfs.sh

cd ../data

docker cp earthquake_data.csv namenode:/batch_data.csv
docker cp tectonic_plate_boundaries.csv namenode:/tectonic_boundaries.csv

echo "Executing upload to HDFS commands..."
docker exec namenode bash ./upload_hdfs.sh

# Check if the upload_hdfs.sh script executed successfully
if [ $? -eq 0 ]; then
    echo "Data upload to HDFS was successful."
else
    echo "Data upload to HDFS failed."
fi

read -n 1 -s -r -p "Press any key to exit..."