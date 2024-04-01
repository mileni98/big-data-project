#echo "> Starting preprocessing script..."
#sleep 5
#./spark/bin/spark-submit /home/batch/preprocessing.py 

#echo 
#echo "> Starting processing script..."
#sleep 5
#./spark/bin/spark-submit --driver-class-path postgresql-42.7.0.jar /home/batch/processing.py


#echo "> Starting test script..."
#sleep 5
#./spark/bin/spark-submit /home/batch/test.py 

echo "> Starting teeeest script..."
sleep 5
#./spark/bin/spark-submit --jars /geospark-1.3.1.jar /home/batch/ttteeest.py
./spark/bin/spark-submit --jars /geospark-1.3.1.jar,/geospark-sql-1.3.1.jar,/geospark-viz-1.3.1.jar /home/batch/ttteeest.py