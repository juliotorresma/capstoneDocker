import findspark
import functools
import os
import logging
from pathlib import Path # Find certain directories.
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

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

    df.printSchema()
    df.show()
    
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

def pullDataframe_Pg(spark):
    df = spark.read. format("jdbc" ).option("url","jdbc:postgresql://localhost:5432/dezyre new")\
        .option("dtable","customer") \
        .option("user","airflow").option("password","airflow").load()
    
    logging.info(df.show(5))
    
    return df