from pickle import NONE
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

def fecthForFiles(format):
    pathlist = list( Path( '.' ).glob(f'**/*.{format}') )
    filelist = sorted( [str(file) for file in pathlist] )
    print(filelist)
    return filelist

def deleteFiles(path, extension):
    directory = path
    files_in_directory = os.listdir(directory)
    filtered_files = [file for file in files_in_directory if file.endswith(extension)]
    for file in filtered_files:
        path_to_file = os.path.join(directory, file)
        os.remove(path_to_file)
    return 

def turnToDF(spark, file, format_of_filelist):
    
    if format_of_filelist == "json": 
        df = spark.read.json(str(file)) 
    elif format_of_filelist == "parquet": 
        df =  spark.read.parquet(str(file))
    elif format_of_filelist == "csv": 
        df =spark.read.options(header='True', inferSchema='True', delimiter=';').csv(file)

    #df.printSchema()
    #df.show()
    
    return df

def renameColumns(df):
    for official_schema_field in aka_dictionary:
        for field in df.schema.fields:
            if str(field.name).lower() in aka_dictionary[official_schema_field]:
                df = df.withColumnRenamed(field.name, official_schema_field)
    #df.printSchema()
    #df.show()
    return df

def count_amount_transactions(df):
    return str(df.count())

def sum_amount_transactions(df):  
    return df.agg(f.sum("amount")).collect()[0][0]

def writeToCsv(df):
    #df.write.format("csv").mode("overwrite").save("filename.csv")
    pass

def pullDataframe_Pg(spark, table_names):

    utils.emptyDirectory(DATAFRAMES_FILE_PATH)

    
    df1 = spark.read.format("jdbc" ).option("url","jdbc:postgresql://postgres:5432/airflow")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable",table_names[0]).option("user","airflow").option("password","airflow").load()
    
    df2 = spark.read.format("jdbc" ).option("url","jdbc:postgresql://postgres:5432/airflow")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable",table_names[1]).option("user","airflow").option("password","airflow").load()


    
    merged = df1.join(df2, "id")
    
    
    
    merged.show()
    #merged2 = merged1.withColumn("test", f.date_format('Transaction_ts', 'dd-MM-yyyy'))
    #merged.withColumn("Transaction_ts", f.to_timestamp("Transaction_ts", "dd-MM-yyyy'T'HH:mm:ss").alias('date'))

    merged.write.parquet(f"/opt/airflow/dataFrames/RDBMS")
    
def pullExtraFormatDataframes(spark, extra_formats):
    for format in extra_formats:
        pathlist = list( Path(RAW_DATA_FILE_PATH).glob(f'**/*.{format}') )
        filelist = sorted( [str(file) for file in pathlist] )
        for file in filelist:
            logging.info(os.path.splitext(file))
            df = turnToDF(spark, file, format)
            file_name = file.split('/')[-1]
            df.write.parquet(f"/opt/airflow/dataFrames/{file_name.split('.')[0]}_{format}")
        #print(filelist)

def pullUnifyDataframes(table_names, extra_formats):

    spark = SparkSession.builder.config('spark.jars', '/opt/airflow/drivers/postgresql-42.5.0.jar').getOrCreate()
    
    pullDataframe_Pg(spark, table_names)
    pullExtraFormatDataframes(spark, extra_formats)
    dataframesUnification(spark)

    spark.stop()

def aggregations(unified_df):
    #Count and sum amount transactions for each type (online or offline(in store)) for day
    trns_4_trans_type = unified_df.select('transaction_type','ts','amount').where((unified_df.transaction_type.isNotNull()) & (unified_df.ts.isNotNull()))

    #Count and sum amount transactions for each city (city can be extracted from address) for day
    trns_4_address = unified_df.select('address','ts','amount').where((unified_df.address.isNotNull()) & (unified_df.ts.isNotNull()))

    #Count and sum amount transactions for each store for day
    trns_4_store = unified_df.select('store_id','ts','amount').where((unified_df.store_id.isNotNull()) & (unified_df.ts.isNotNull()))

    sum_trans_type_df = trns_4_trans_type.groupBy('transaction_type','ts').agg(f.sum("amount"),f.count("transaction_type"))
    sum_trans_type_df.show()

    trns_4_store_df = trns_4_store.groupBy('store_id','ts').agg(f.sum("amount"),f.count("store_id"))
    trns_4_store_df.show()

    trns_4_different_address_df = trns_4_address.groupBy('address','ts').agg(f.sum("amount"))
    trns_4_different_address_df.show()

def dataframesUnification(spark):

    pathlist = list( Path(DATAFRAMES_FILE_PATH).glob(f'**/*.parquet') )
    filelist = [str(file) for file in pathlist]
    
    #//////////////////////////// NORMALIZATION //////////////////////////////
    unified_df =  renameColumns(turnToDF(spark, filelist[0], 'parquet'))
    for i in range(len(filelist)):
        #for file in filelist:
        logging.info(os.path.splitext(filelist[i]))
        df = turnToDF(spark, filelist[i], 'parquet')
        df = renameColumns(df)
        unified_df = df.unionByName(unified_df, allowMissingColumns=True)
    unified_df = unified_df.withColumn('ts', f.split(unified_df['ts'], 'T').getItem(0))    
    #/////////////////////////////////////////////////////////////////////////
    #unified_df.printSchema()
    #unified_df.show(400)
    aggregations(unified_df)

