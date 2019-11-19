#https://spark-packages.org/package/mongodb/mongo-spark
#bin\pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1
spark.jars.packages = org.mongodb.spark:mongo-spark-connector_2.11:2.4.1

from pyspark.sql import SparkSession

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/February03.Twitter") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/February03.Twitter") \
    .getOrCreate()

df = spark.read.format("mongo").load()