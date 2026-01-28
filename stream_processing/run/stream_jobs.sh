echo "> Starting consumer script..."
sleep 3
./spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0 \
    /home/streaming/consumer/consumer.py