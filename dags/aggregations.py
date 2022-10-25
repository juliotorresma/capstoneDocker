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
SIZE = 100

with DAG("data_generation", start_date=datetime(2022,1,1),
    schedule_interval="@hourly", catchup=False) as dag:

   
    dataMainGeneration = PythonOperator(
        task_id="dataMainGeneration",
        python_callable= dataGenerators.mainGenerator,
        op_kwargs={"size":SIZE},
        dag=dag
    )

    pull_unify_aggregations_dataframes = PythonOperator(
            task_id="pull_unify_aggregations_dataframes",
            python_callable= pysparkModules.pullUnifyDataframes,
            op_kwargs={"table_names":["customer","transaction"],"extra_formats":["json","parquet"]},
            trigger_rule='all_success'
        )

    dataMainGeneration>>pull_unify_aggregations_dataframes