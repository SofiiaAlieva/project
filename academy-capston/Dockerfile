FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

USER 0 

WORKDIR /academy-capston

ENV [ACCESS_KEY_ID]=[value]&&[SECRET_ACCESS_KEY]=[value2]

RUN pip install pyspark==3.1.2&&pip install boto3
COPY . .

ENTRYPOINT ['python3']

CMD ['project.py']
