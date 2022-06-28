import findspark
import os
import sys
findspark.init(os.environ["SPARK_HOME"])
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when, col, regexp_replace, split
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
import pandas

sys.path.append("/Users/maya/Desktop/AiCore_git/Pinterest_Data_Processing_Pipeline")
import S3_to_Spark_to_Cassandra


default_args = {
    'owner': 'Maya',
    'depends_on_past': False,
    'email': ['maya_iuga@yahoo.ro'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2022, 6, 28),
    'retry_delay': timedelta(minutes=1),
    'end_date': datetime(2022, 6, 29),
    
}


spark_session = S3_to_Spark_to_Cassandra.Spark_DAG()


with DAG(dag_id='dag_spark',
         default_args=default_args,
         schedule_interval='09 13 * * *',
         catchup=False,
         tags=['spark']
         ) as dag:
    # define task
    Spark_task = PythonOperator(
        task_id = 'run_spark_job',
        python_callable = spark_session.run_spark_session,
        )
   