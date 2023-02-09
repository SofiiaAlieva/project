from pyspark import SparkConf
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame, Column


config = {"spark.jars.packages":"org.apache.hadoop:hadoop-aws:3.3.1", 
"spark.hadoop.fs.s3.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem", 
"fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain"}

conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def flatten():
    df = spark.read.json("s3://dataminded-academy-capstone-resources/raw/open_aq/")
    flattened_df = (
        df.withColumn(colName="coordinates_latitude", col=df["coordinates"].getField("latitude"))
        .withColumn(colName="coordinates_longitude", col=df["coordinates"].getField("longitude"))
        .drop("coordinates")
        .withColumn(colName="date_local", col=df["date"].getField("local"))
        .withColumn(colName="date_utc", col=df["date.utc"])
        .drop("date")
    )
    return flattened_df

