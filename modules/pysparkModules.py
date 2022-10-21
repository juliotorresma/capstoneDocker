import sys
sys.path.insert(1, '/opt/airflow/modules')
import utils
import findspark
import functools
import os
import logging
from pathlib import Path # Find certain directories.
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
DATAFRAMES_FILE_PATH = '/opt/airflow/dataFrames'
RAW_DATA_FILE_PATH = '/opt/airflow/generatedData'

findspark.init()
findspark.find()

aka_dictionary = {  "id":["transaction_id","id"],
                    "customer_id":["customer_id"],
                    "ts":["Transaction_ts", "ts"],
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
    df.printSchema()
    df.show()
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

    for table_name in table_names:
        df = spark.read.format("jdbc" ).option("url","jdbc:postgresql://postgres:5432/airflow")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable",table_name).option("user","airflow").option("password","airflow").load()
        df.write.parquet(f"/opt/airflow/dataFrames/{table_name}")
    
    
    logging.info(df.show(5))
    
def pullExtraFormatDataframes(spark, extra_formats):
    for format in extra_formats:
        pathlist = list( Path(RAW_DATA_FILE_PATH).glob(f'**/*.{format}') )
        filelist = sorted( [str(file) for file in pathlist] )
        for file in filelist:
            logging.info(os.path.splitext(file))
            df = turnToDF(spark, file, format)
            file_name = file.split('/')[-1]
            df.write.parquet(f"/opt/airflow/dataFrames/{file_name.split('.')[0]}_{format}")
        print(filelist)

def pullDataframes(table_names, extra_formats):

    spark = SparkSession.builder.config('spark.jars', '/opt/airflow/drivers/postgresql-42.5.0.jar').getOrCreate()
    
    pullDataframe_Pg(spark, table_names)
    pullExtraFormatDataframes(spark, extra_formats)

    spark.stop()

def dataframesUnification():
    spark = SparkSession.builder.config('spark.jars', '/opt/airflow/drivers/postgresql-42.5.0.jar').getOrCreate()
    
    
    pathlist = list( Path(DATAFRAMES_FILE_PATH).glob(f'**/*.parquet') )
    filelist = [str(file) for file in pathlist]
    logging.info(filelist)
    for file in filelist:
        logging.info(os.path.splitext(file))
        df = turnToDF(spark, file, 'parquet')
        df = renameColumns(df)

    spark.stop()