import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


spark = SparkSession \
    .builder \
    .appName("Spark Preprocessing") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


batch_df = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/batch_data.csv")


tect_plates_df = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/tectonic_boundaries.csv")

batch_df.show()
