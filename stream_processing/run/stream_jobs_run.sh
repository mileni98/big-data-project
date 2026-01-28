# Copying stream scripts to the Spark Master container.
echo
echo "> Copying stream scripts to master node..."
docker cp ./stream_jobs.sh spark-master:./stream_jobs.sh
sleep 3

# Executing spark streaming scripts.
echo
docker exec -it spark-master bash ./stream_jobs.sh
