import sys
sys.path.insert(1, '/opt/airflow/modules')
import utils
import findspark
from functools import reduce
import functools
import os
import logging
from pathlib import Path # Find certain directories.
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

DATAFRAMES_FILE_PATH = '/opt/airflow/dataFrames'
RAW_DATA_FILE_PATH = '/opt/airflow/generatedData'
USER = 'airflow'
PASSWORD = 'airflow'
findspark.init()
findspark.find()

aka_dictionary = {  "id":["transaction_id","id"],
                    "customer_id":["customer_id"],
                    "ts":["transaction_ts", "ts"],
                    "customer_first_name":["first_name","customer_first_name"],
                    "customer_last_name":["customer_last_name", "last_name"],
                    "phone_number":["phone_number", "number"],
                    "address":["address","city"],
                    "transaction_type":["type"],
                    "store_id":["store_id","store"],
                    "amount":["amount"]
                 }

def turnToDF(spark, file, format_of_filelist):
    '''
    THIS FUNCTION RECEIVES A FILE NAME AND A FORMAT,
    RETURNS A PYSPARK DATAFRAME.
    '''
    if format_of_filelist == "json": 
        df = spark.read.json(str(file)) 
    elif format_of_filelist == "parquet": 
        df =  spark.read.parquet(str(file))
    elif format_of_filelist == "csv": 
        df =spark.read.options(header='True', inferSchema='True', delimiter=';').csv(file)
    return df

def renameColumns(df):
    '''
    THIS FUNCTION NORMALIZE THE HERADERS NAMES IN THE DATAFRAMES IT RECEIVES.
    '''
    for official_schema_field in aka_dictionary:
        for field in df.schema.fields:
            if str(field.name).lower() in aka_dictionary[official_schema_field]:
                df = df.withColumnRenamed(field.name, official_schema_field)
    return df

def pullDataframe_Pg(spark, table_names):
    '''
    THIS FUNCTION PULLS THE RDBMS RECORDS FROM THE AIRFLOW DATABASE
    AND TRANSFORM IT TO PYSPARK DATAFRAME.
    '''
    utils.emptyDirectory(DATAFRAMES_FILE_PATH)

    df1 = spark.read.format("jdbc" ).option("url","jdbc:postgresql://postgres:5432/airflow")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable",table_names[0]).option("user",USER).option("password",PASSWORD).load()
    
    df2 = spark.read.format("jdbc" ).option("url","jdbc:postgresql://postgres:5432/airflow")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable",table_names[1]).option("user",USER).option("password",PASSWORD).load()

    #Merging both tables Costumer and Transaction
    merged = df1.join(df2, "id")

    #Writing dataframe to temporary dir "dataFrames"
    merged.write.parquet(f"{DATAFRAMES_FILE_PATH}/RDBMS")
    
def pullExtraFormatDataframes(spark, extra_formats):
    '''
    THIS FUNCTION RECOGNIZE ALL FILES WITH THE SPECIFIC FORMAT YOU SPECIFY
    AND RETURNS YOU A PYSPARK DATAFRAME
    '''
    for format in extra_formats:
        pathlist = list( Path(RAW_DATA_FILE_PATH).glob(f'**/*.{format}') )
        filelist = sorted( [str(file) for file in pathlist] )
        for file in filelist:
            df = turnToDF(spark, file, format)
            file_name = file.split('/')[-1]
            df.write.parquet(f"/opt/airflow/dataFrames/{file_name.split('.')[0]}_{format}")

def dataframesUnification(spark):
    '''
    FUNCTION RESPOSABLE OF UNIFIY ALL DATAFRAMES FOUND IN TEMPORARY DIR ./dataFrames
    '''
    pathlist = list( Path(DATAFRAMES_FILE_PATH).glob(f'**/*.parquet') )
    filelist = [str(file) for file in pathlist]
    #//////////////////////////// NORMALIZATION //////////////////////////////
    unified_df =  renameColumns(turnToDF(spark, filelist[0], 'parquet'))
    for i in range(len(filelist)):
        df = turnToDF(spark, filelist[i], 'parquet')
        df = renameColumns(df)
        unified_df = df.unionByName(unified_df, allowMissingColumns=True)
    unified_df = unified_df.withColumn('ts', f.split(unified_df['ts'], 'T').getItem(0))    
    #/////////////////////////////////////////////////////////////////////////
    aggregations(unified_df)

def aggregations(unified_df):
    '''
    FUNCTION RESPOSABLE OF DO AGGREGATIONS TO THE UNIFIED DATAFRAME.
    '''
    #Count and sum amount transactions for each type (online or offline(in store)) for day
    trns_4_trans_type = unified_df.select('transaction_type','ts','amount').where((unified_df.transaction_type.isNotNull()) & (unified_df.ts.isNotNull()))

    #Count and sum amount transactions for each city (city can be extracted from address) for day
    trns_4_address = unified_df.select('address','ts','amount').where((unified_df.address.isNotNull()) & (unified_df.ts.isNotNull()))

    #Count and sum amount transactions for each store for day
    trns_4_store = unified_df.select('store_id','ts','amount').where((unified_df.store_id.isNotNull()) & (unified_df.ts.isNotNull()))


    sum_trans_type_df = trns_4_trans_type.groupBy('transaction_type','ts').agg(f.sum("amount"),f.count("transaction_type"))
    trns_4_store_df = trns_4_store.groupBy('store_id','ts').agg(f.sum("amount"),f.count("store_id"))
    trns_4_different_address_df = trns_4_address.groupBy('address','ts').agg(f.sum("amount"))

    sum_trans_type_df.show()
    trns_4_store_df.show()
    trns_4_different_address_df.show()

    '''
    AGGREGATIONS INGESTION TO HDFS.
    '''
    trns_4_different_address_df.write.mode("overwrite").parquet(f"hdfs://namenode:9000/user/root/capstone/{utils.cleanTodaysDate()}/trns_4_different_address_df")
    trns_4_store_df.write.mode("overwrite").parquet(f"hdfs://namenode:9000/user/root/capstone/{utils.cleanTodaysDate()}/trns_4_store_df")
    sum_trans_type_df.write.mode("overwrite").parquet(f"hdfs://namenode:9000/user/root/capstone/{utils.cleanTodaysDate()}/sum_trans_type_df")

def pullUnifyDataframes(table_names, extra_formats):
    '''
    MAIN FUNCTION TO INITIALIZE PULLING, UNIFICATION AND AGGREGATIONS OF RECOLECTED DATA.
    '''

    spark = SparkSession.builder.config('spark.jars', '/opt/airflow/drivers/postgresql-42.5.0.jar').getOrCreate()
    
    pullDataframe_Pg(spark, table_names)
    pullExtraFormatDataframes(spark, extra_formats)
    dataframesUnification(spark)

    spark.stop()
