from datetime import timedelta
import datetime as dt
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator

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

with dag:
    submit_batch_job = BatchOperator(
        task_id="running_capstone",
        job_name="Sofiia-capstone",
        job_queue="arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2023-job-queue",
        job_definition="arn:aws:batch:eu-west-1:338791806049:job-definition/Sofiia-capstone:2",
    )
