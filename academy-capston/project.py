from pyspark import SparkConf
from pyspark.sql import SparkSession
import boto3
import snowflake.connector as sc
# url = "s3://dataminded-academy-capstone-resources/raw/open_aq/"

config = {"spark.jars.packages":"org.apache.hadoop:hadoop-aws:3.3.1,net.snowflake:snowflake-jdbc:3.13.3,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1", 
"spark.hadoop.fs.s3.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem", 
"fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
}

conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.json("s3://dataminded-academy-capstone-resources/raw/open_aq/").cache()

flattened_df = (
    df.withColumn(colName="coordinates_latitude", col=df["coordinates"].getField("latitude"))
    .withColumn(colName="coordinates_longitude", col=df["coordinates"].getField("longitude"))
    .drop("coordinates")
    .withColumn(colName="date_local", col=df["date"].getField("local"))
    .withColumn(colName="date_utc", col=df["date.utc"])
    .drop("date")
)

client = boto3.client('secretsmanager')
secret = client.get_secret_value(SecretId="snowflake/capstone/login")

print(secret)