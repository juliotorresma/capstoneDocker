import sys
sys.path.insert(1, '/opt/airflow/modules')
from utils import *
import dataGenerators
from airflow import DAG
import json
import utils 
import logging
import pandas as pd
import psycopg2
import logging
import random
import uuid
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator,PythonOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

SIZE = 100


FILE_PATH = '/opt/airflow/generatedData'

with DAG("rdbms_Generator", start_date=datetime(2022,1,1),
    schedule_interval="@once", catchup=False) as dag:

    def pg_hook(sql_to_get_schema, sql_to_check_table_exist, table_name):
        hook = PostgresHook()
        query = hook.get_records(sql=sql_to_get_schema)
        logging.info(query)
        for result in query:
            if 'airflow' in result:
                schema = result[0]
                break
        query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name)) 
        return query


    def check_if_table_exist_customer(sql_to_get_schema, sql_to_check_table_exist, table_name):
        query = pg_hook(sql_to_get_schema, sql_to_check_table_exist,table_name)
        if query: return "check_table_exist_transaction" 
        else : return "create_table_customer"
    
    def check_if_table_exist_transaction(sql_to_get_schema, sql_to_check_table_exist, table_name):
        query = pg_hook(sql_to_get_schema, sql_to_check_table_exist,table_name)
        if query: return "insert_values" 
        else: return "create_table_transaction"
    
    drop_table_customer = PostgresOperator(task_id='drop_table_customer',
                                    postgres_conn_id= 'postgres_default',
                                    sql= f'''DROP TABLE IF EXISTS customer''' ,
                                    dag=dag)
    drop_table_transaction = PostgresOperator(task_id='drop_table_transaction',
                                    postgres_conn_id= 'postgres_default',
                                    sql= f'''DROP TABLE IF EXISTS transaction''' ,
                                    dag=dag)

    table_customer = 'customer'

    check_table_exist_customer = BranchPythonOperator(
            task_id='check_table_exist_customer',
            python_callable=check_if_table_exist_customer,
            op_args=["select * from pg_tables;",
                     "SELECT * FROM information_schema.tables "
                     "WHERE table_schema = '{}'"
                     "AND table_name = '{}';", table_customer
                     ] ,
            provide_context=True,
            dag=dag)

    create_table_customer = PostgresOperator(task_id='create_table_customer',
                                    postgres_conn_id= 'postgres_default',
                                    sql= f''' CREATE TABLE customer(
                                            id INT PRIMARY KEY,
                                            FIRST_NAME CHAR(30) NOT NULL,
                                            LAST_NAME CHAR(30),
                                            Phone_number CHAR(30),
                                            Address CHAR(40)
                                            )''' ,
                                    dag=dag)


    table_transaction = 'transaction'

    check_table_exist_transaction = BranchPythonOperator(
            task_id='check_table_exist_transaction',
            python_callable=check_if_table_exist_transaction,
            op_args=["select * from pg_tables;",
                     "SELECT * FROM information_schema.tables "
                     "WHERE table_schema = '{}'"
                     "AND table_name = '{}';", table_transaction
                     ] ,
            provide_context=True,
            trigger_rule= "one_success",
            dag=dag)

    create_table_transaction = PostgresOperator(task_id='create_table_transaction',
                                    postgres_conn_id= 'postgres_default',
                                    sql= f'''   CREATE TABLE transaction(
                                                id INT PRIMARY KEY,
                                                Customer_id INT,
                                                Transaction_ts CHAR(30),
                                                Amount INT)''' ,
                                    dag=dag)

    insert_values = PythonOperator(
            task_id="insert_values",
            python_callable= dataGenerators.insertQueries,
            op_args=[SIZE],
            trigger_rule='one_success'
        )
    
    

    drop_table_customer>>drop_table_transaction>>check_table_exist_customer>>[check_table_exist_transaction,create_table_customer]
    create_table_customer>>check_table_exist_transaction
    check_table_exist_transaction>>[create_table_transaction,insert_values]
    create_table_transaction>>insert_values
    
