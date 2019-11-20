#https://spark-packages.org/package/mongodb/mongo-spark
#bin\pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1
#bin\spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 C:\Users\chrmitic\Documents\Twitter\twitter.py

import unicodedata
from pyspark.sql import SparkSession

#Connect to the MongoDB
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/February03.Twitter") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/February03.Twitter") \
    .getOrCreate()

#Add log4j
logger = spark._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

#Read the data from MongoDB
df = spark.read.format("mongo").option("encoding", "UTF-8").load()
#df.printSchema()

def turnToAscii(var):
    if var is None:
        var = u'a'
    nfkd_var = unicodedata.normalize('NFKD', var)
    ascii_var = nfkd_var.encode('ascii', 'ignore')
    return ascii_var

#Create a selection of just the column of interest and turn it into a RDD map
df.registerTempTable("temp")
sqlDF = spark.sql("SELECT text FROM temp")

def costF(row):
    return (row.text)
data_mapped = sqlDF.rdd.map(costF)

data_mapped.foreach(turnToAscii)
cnt = data_mapped.count()
print(data_mapped.take(cnt))
