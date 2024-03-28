# Copying batch scripts to the Spark Master container.
echo
echo "> Copying batch scripts to master node..."
docker cp ./batch_jobs.sh spark-master:./batch_jobs.sh
sleep 3

# Executing spark preprocessing and processing scripts.
echo 
docker exec -it spark-master bash ./batch_jobs.sh