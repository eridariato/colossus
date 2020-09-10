from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, lit, when, col
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# create Spark context with Spark configuration
conf = SparkConf().setAppName("athena - opensignal db2.0 infrastructure")
sc = SparkContext(conf=conf)
spark_session = SparkSession(sc)
sqlContext = SQLContext(sc)

##----------------------------------------------------------------------------------------------------##


device_df = spark_session.read.parquet("hdfs:///data/landing/g_debi/athena/01_opensignal/01_pi_device")
device_df.groupBy('weekstart', 'level').agg(
    F.countDistinct('network_name_mapped').alias('operator'),
    F.countDistinct('frequency').alias('frequency'),
    F.countDistinct('location').alias('location'),
    F.count('*').alias('record'),
).sort('weekstart','level').show()

spark_session.stop()
