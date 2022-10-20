import sys
sys.path.insert(1, '/opt/airflow/modules')
from utils import *
from airflow import DAG
import json
import utils 
import logging
import pandas as pd
import dataGenerators
import pysparkModules
from random import randint
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import findspark
import functools
import os
from pathlib import Path # Find certain directories.
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

FILE_PATH = '/opt/airflow/generatedData'

spark = SparkSession \
        .builder \
        .appName("Capstone Final Project") \
        .getOrCreate()


with DAG("data_generation", start_date=datetime(2022,1,1),
    schedule_interval="@once", catchup=False) as dag:

    with TaskGroup(group_id="Data_Generation") as process_results:
        json_generator = PythonOperator(
            task_id="json_generator",
            python_callable= dataGenerators.jsonDataGenerator,
            op_kwargs={"size":100},
            dag=dag,
        )
        parquet_generator = PythonOperator(
            task_id="parquet_generator",
            python_callable= dataGenerators.parquetDataGenerator,
            op_kwargs={"size":100},
            dag=dag,
        )
        sensor_triggered_dag = TriggerDagRunOperator(
            task_id='RDBMS_generation',
            trigger_dag_id='rdbms_Generator')

        
    
    pull_dataframes = PythonOperator(
            task_id="pull_dataframes",
            python_callable= pysparkModules.pullDataframe_Pg,
            op_args=[spark],
            trigger_rule='all_success'
        )

    sensor_triggered_dag
        
    process_results>>pull_dataframes