FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

WORKDIR /academy-capston

COPY . .

RUN pip install pyspark&&pip install boto3&&pip install json

COPY . .

ENTRYPOINT ['python3 project.py']
