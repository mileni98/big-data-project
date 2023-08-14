# big-data-project

1. Go to "batch_processing" folder and run command: docker-compose up --build
2. Go to "upload_data" folder and run command: ./docker_upload.sh
   (data will appear in namenode: http://localhost:9870/explorer.html#/user/root/data-lake)
   if it is in safe mode, just run again

3. Go to "batch_processing and run command: docker cp preprocessing.py spark-master:/preprocessing.py

4. To open spark master bash run command: docker-compose exec -it spark-master bash

5. In spark bash type to run the script: ./spark/bin/spark-submit /preprocessing.py
