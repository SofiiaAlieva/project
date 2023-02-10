from datetime import timedelta
import datetime as dt
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark import SparkConf
from pyspark.sql import SparkSession
import boto3

from pyspark.sql.types import *

"""DAG for the project based on the job academy-capstone/project.py"""

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "depends_on_past": False,
    "start_date": dt.datetime(2023, 2, 10, tz="Europe/Brussels"),
}

dag = DAG(
    "Sofiia-capstone",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
)


# Code
def capstone_run():
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1,net.snowflake:snowflake-jdbc:3.13.3,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }

    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    df = spark.read.json("s3://dataminded-academy-capstone-resources/raw/open_aq/")

    flattened_df = (
        df.withColumn(
            colName="coordinates_latitude", col=df["coordinates"].getField("latitude")
        )
        .withColumn(
            colName="coordinates_longitude", col=df["coordinates"].getField("longitude")
        )
        .drop("coordinates")
        .withColumn(colName="date_local", col=df["date"].getField("local"))
        .withColumn(colName="date_utc", col=df["date.utc"])
        .drop("date")
    )

    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId="snowflake/capstone/login")

    x = json.loads(secret["SecretString"])
    username = x["USER_NAME"]
    password = x["PASSWORD"]
    warehouse = x["WAREHOUSE"]
    database = x["DATABASE"]
    url = x["URL"]

    sfOptions = {
        "sfURL": url,
        "sfUser": username,
        "sfPassword": password,
        "sfDatabase": database,
        "sfSchema": "SOFIIA",
        "sfWarehouse": warehouse,
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    (
        df.write.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("dbtable", "Help")
        .mode("overwrite")
        .save()
    )


with dag:
    t = PythonOperator(
        task_id="running_capstone",
        python_callable=capstone_run,
        dag=dag,
    )
