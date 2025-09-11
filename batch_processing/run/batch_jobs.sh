echo "> Starting preprocessing script..."
sleep 5
#./spark/bin/spark-submit /home/batch/preprocessing.py 
./spark/bin/spark-submit --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.6.1 /home/batch/preprocessing.py

#echo 
#echo "> Starting processing script..."
#sleep 5
#./spark/bin/spark-submit --driver-class-path postgresql-42.7.7.jar /home/batch/processing.py


#echo "> Starting test script..."
#sleep 5
#./spark/bin/spark-submit /home/batch/test.py 