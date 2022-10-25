import sys
sys.path.insert(1, '/opt/airflow/modules')
from utils import *
from airflow import DAG
import json
import utils 
import logging
import psycopg2
import pandas as pd
from random import randint
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook

FILE_PATH = '/opt/airflow/generatedData'
DATABASE = 'airflow'
USER = 'airflow'
PASSWORD = 'airflow'
HOST = 'host.docker.internal'
PORT = '5432'

def rdbmsGeneration(size, days_difference = 0):
    '''
    FUNCTION RESPPONSABLE OF CREATE RDBMS RECORDS IN AIRFLOW DATABASE
    '''
    conn = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cursor = conn.cursor()

    if days_difference == 0:
        #Doping Transaction table if already exists.
        cursor.execute("DROP TABLE IF EXISTS transaction")
        cursor.execute("DROP TABLE IF EXISTS customer")
        
        #Creating table as per requirement
        sql ='''CREATE TABLE transaction(
        id INT PRIMARY KEY,
        Customer_id INT,
        Transaction_ts CHAR(30),
        Amount INT)
        '''
        cursor.execute(sql)
        
        #Creating table as per requirement
        sql ='''CREATE TABLE customer(
        id INT PRIMARY KEY,
        FIRST_NAME CHAR(30) NOT NULL,
        LAST_NAME CHAR(30),
        Phone_number CHAR(30),
        Address CHAR(40)
        )'''
        cursor.execute(sql)

    id_list_transaction = utils.randomlistID(days_difference * size * 3, size)
    id_list_costumer = utils.randomlistID(1, size)
    time_stamp = utils.listTimeStamp(size)
    first_names = utils.getFirstNames(size)
    last_names = utils.getLastNames(size)
    amounts = utils.getRandomAmounts(size)
    phones = utils.generatePhoneNumbers(size)
    address = utils.randomCities(size)

    for i in range(size):

        postgres_insert_query = """ INSERT INTO transaction(id, Customer_id, Transaction_ts, Amount) VALUES (%s,%s,%s,%s)"""
        record_to_insert = (id_list_transaction[i], id_list_costumer[i], time_stamp[i], amounts[i])
        cursor.execute(postgres_insert_query, record_to_insert)

        postgres_insert_query = """ INSERT INTO customer(id, FIRST_NAME, LAST_NAME, Phone_number, Address) VALUES (%s,%s,%s,%s,%s)"""
        record_to_insert = (id_list_transaction[i], first_names[i], last_names[i], phones[i], address[i])
        cursor.execute(postgres_insert_query, record_to_insert)
    
    conn.commit()
    conn.close()
    return 

def parquetDataGenerator(size, days_difference = 0):
    '''
    FUNCTION RESPPONSABLE OF CREATE PARQUET RECORDS
    '''
    id_list = utils.randomlistID((days_difference * size * 3) + 100, size)
    time_stamp = utils.listTimeStamp(size)
    first_names = utils.getFirstNames(size)
    last_names = utils.getLastNames(size)
    amounts = utils.getRandomAmounts(size)
    store_ids = utils.generateStoresIds(size)
    frame = []
    for i in range(size):
        frame.append([id_list[i], first_names[i], last_names[i], amounts[i], time_stamp[i], store_ids[i]])

    df = pd.DataFrame(frame, columns =['Id', 'First_name', 'Last_name', 'Amount', 'ts', 'Store_id'])
    df.to_parquet(f'{FILE_PATH}/{utils.systemDate().strftime("%d-%m-%Y")}.parquet') 
    return 

def jsonDataGenerator(size, days_difference = 0):
    '''
    FUNCTION RESPPONSABLE OF CREATE JSON RECORDS
    '''
    id_list = utils.randomlistID((days_difference * size * 3) + 200, size)
    time_stamp = utils.listTimeStamp(size)
    first_names = utils.getFirstNames(size)
    last_names = utils.getLastNames(size)
    amounts = utils.getRandomAmounts(size)
    types = utils.generateRandomTypes(size)

    with open(f'{FILE_PATH}/{utils.systemDate().strftime("%d-%m-%Y")}.json', 'w') as f:
        for i in range(size):
            json_format = {"id":id_list[i],
                        "ts":time_stamp[i],
                        "customer_first_name":first_names[i],
                        "customer_last_name":last_names[i],
                        "amount":amounts[i],
                        "type":types[i]}

            json_line = json.dumps(json_format)
            f.write(json_line)
            f.write('\n')
    return 

def mainGenerator(size, reset=True, days_back=5):
    '''
    THIS FUNCTION IS RESPONSABLE OF CALLING THE 3 DIFFERENT TYPES OF DATA GENERATORS
    '''
    #False flag in reset to not create new data.
    utils.reset(reset, days_back)
    today_date = utils.todaysDate()
    i = 0

    #Checking if thres any past files.
    while systemDate() < today_date:
        jsonDataGenerator(size, i)
        parquetDataGenerator(size, i)
        rdbmsGeneration(size, i)
        utils.updateDate()
        i+=1
     

