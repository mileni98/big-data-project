# big-data-project

1. Go to "batch_processing" folder and run command to start containers:

```
docker-compose up --build
```

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

TO SEE if data was written to postgresql:
```
docker exec -it batch_processing_postgresql_1 psql -U postgres -d big_data
SELECT * FROM public.price_decline;
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
