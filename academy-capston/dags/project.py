from datetime import timedelta

import pendulum as dt
from airflow import DAG
from airflow.models import Variable
from conveyor.operators import ConveyorSparkSubmitOperatorV2

DAG 