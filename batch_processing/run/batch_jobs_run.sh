docker cp ./batch_jobs.sh spark-master:./batch_jobs.sh

docker exec -it spark-master bash ./batch_jobs.sh
