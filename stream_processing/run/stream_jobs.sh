echo "> Starting consumer script..."
sleep 3
./spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0,org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1 \
    /home/streaming/consumer/consumer.py