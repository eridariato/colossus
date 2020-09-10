from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, lit, when, col
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from datetime import date
from datetime import timedelta

# create Spark context with Spark configuration
conf = SparkConf().setAppName("athena - opensignal db2.0 device")
sc = SparkContext(conf=conf)
spark_session = SparkSession(sc)
sqlContext = SQLContext(sc)

##----------------------------------------------------------------------------------------------------##

file = in_filedir + "pi_feed_devices_all_regions_telkomselidn_20200420_20200520.json.gz"
df = spark_session.read.json(file, multiLine=True)
                
spark_session.stop()
