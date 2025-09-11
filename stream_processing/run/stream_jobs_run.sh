cd ..

# Starting the stream processing cluster.
echo
echo "> Starting stream processing cluster..."
sleep 3
docker-compose up -d
sleep 3

# Copying stream scripts to the Spark Master container.
echo
echo "> Copying stream scripts to master node..."
docker cp ./run/stream_jobs.sh spark-master:./stream_jobs.sh
sleep 3

# Executing spark streaming scripts.
echo
docker exec -it spark-master bash ./stream_jobs.sh
