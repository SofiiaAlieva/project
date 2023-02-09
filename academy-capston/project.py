from pyspark import SparkConf
from pyspark.sql import SparkSession

# url = "s3://dataminded-academy-capstone-resources/raw/open_aq/"

config = {"spark.jars.packages":"org.apache.hadoop:hadoop-aws:3.3.1", 
"spark.hadoop.fs.s3.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem", 
"fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain"}

conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.json("s3://dataminded-academy-capstone-resources/raw/open_aq/")
df.show()


