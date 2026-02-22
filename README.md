# Big data project


downloa plates data from https://github.com/fraxen/tectonicplates/tree/master
and downloa PB2002_plates.shp, PB2002_plates.dbf, PB2002_plates.shx files and put them in the data folder.

Run the helper script to convert the shapefile to a csv with WKT geometry format, which will be used in the batch processing step:
```
python batch_processing/spark/helper_script.py
```
(! NOTE currently spark incompatible with sedona to load directly shapefile)

### 1. Arhitecture Setup

Go to the root older and run the following command to run the full pipeline:

```
./run_pipeline.sh
```
Base on the flags in the script it will run cluster_up.sh, batch_jobs_run.sh and stream_jobs_run.sh. 
- Cluster up will start the full architecture needed for both batch and stream processing, create docker networks, run containers and copy data to the hadoop datanodes.
- Batch jobs will copy the corresponding code to the spark master container and will run the preprocessing and processing script. 
- Stream jobs will copy the corresponding code to the spark master container and will run the streaming script.

If there is an error on initial setup run the following commands on all schell scripts in the project:
```
sudo apt-get install dos2unix
dos2unix run_pipeline.sh
chmod +x run_pipeline.sh
```

If you want to shut down the cluster, from the root folder run:
```
./setup/cluster_down.sh
```

Arhicecture diagram:

// TODO - Add architecture diagram

---

### 2. Data Upload Verification

Verify that data has been successfully copied to datanodes - http://localhost:9870/explorer.html#/user/root/data-lake

--- 

### 3. Batch Processing

Make sure that the RUN_BATCH tag in the run_pipeline.sh is set to true. If you want to run only batch processing set the other flags (RUN_SETUP, RUN_STREAM) to false.

There are 2 scripts:
- Preprocessing script - Which loads raw data from data-lake, does some cleaning and stores it back to the transformation zone in data-lake.
- Processing script - Which loads preprocessed data from the transformation zone in data-lake, does aggregations, displays results and stores final results in PostgreSQL database.

To confirm all is working well, Spark Master UI is on http://localhost:8080. Once the app is running there is information about the current run on http://localhost:4040 .

--- 

### 3. Batch Processing Visualisation

Once the processing has finished, open http://localhost:3000/admin/databases -> Add Database -> Connection String (jdbc:postgresql://postgresql:5432/big_data
)  and press 'Sync database schema'.


* If this is the first time running metabase, create an admin account and add the big_data database.

- Go to http://localhost:3000/browse/databases and choose the 'big_data', go to Visualization and visualize the tables.


### 4. Streaming Processing

Make sure that the RUN_STREAM tag in the run_pipeline.sh is set to true. If you want to run only stream processing set the other flags (RUN_SETUP, RUN_BATCH) to false.

There two major components of the streaming pipeline:
- Producer - Simulates real time data ingestion by continuosly reading records from a SCV file and publishing them to a Kafka topic.
- Consumer - Reads data from the Kafka topic in real time, parses and processes the incoming events using Spark Structured Streaming and stores the results in Elastic Search for visualization.

To verify streaming ingestion open the kafdrop UI on http://localhost:9008 and check that messages are being sent for the topic "earthquakes" -> View Messages.

---
### 5. Stream Processing Visualisation

Once the processing starts results are visible in both the terminal and stored in Elastic Search. Ensure Elastic Search service is running on http://localhost:9200/.

If all is running well, open Kibana on http://localhost:5601 to visualize the data, by opening a saved data view.

* If its your first time running Kibana go to Analytics -> Discover -> Create Data View -> Select "earthquakes" index -> Choose the Timestamp field -> Save Data View to Kibana.


### 6. Queries List
// TODO