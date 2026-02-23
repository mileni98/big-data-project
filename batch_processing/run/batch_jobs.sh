echo "> Starting the preprocessing script..."
sleep 1
./spark/bin/spark-submit \
    --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1 \
    /home/batch/preprocessing.py

echo 
echo "> Starting the processing script..."
sleep 1
./spark/bin/spark-submit \
    --driver-class-path postgresql-42.7.7.jar \
    --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1 \
    /home/batch/processing.py

echo
echo "> Finished batch jobs."