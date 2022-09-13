import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime
from airflow.models.baseoperator import chain
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'
default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}
dag_spark = DAG(
    dag_id = "view_etl",
    default_args=default_args,
    schedule_interval='@daily',
)
events_partitioned = SparkSubmitOperator(
    task_id='view_creator',
    dag=dag_spark,
    application ='/home/gerzzog198/views_creator.py' ,
    conn_id= 'yarn_spark',
    application_args = ["2022-04-05", 5, "/user/master/data/geo/events", "/lessons/geo.csv", "/user/gerzzog198/data/analytics/"],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g')

events_partitioned 

