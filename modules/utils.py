import os
import random
import shutil
import datetime
from random import shuffle, seed
from faker.providers.person.en import Provider
from faker.providers.address import Provider as prcity
import faker
from faker import Faker
import logging
FILE_PATH = '/opt/airflow/generatedData'

def reset(flag, day_dif):
    '''
    FUNCTION RESPONSABLE OF DELETING ALL TEMPORARY FILES IN ./dataGenerators 
    AND RESET THE INITIAL DATE
    '''
    if flag:
        deleteFiles(FILE_PATH,".json")
        deleteFiles(FILE_PATH,".parquet")
        date_object = systemDate()
        step = datetime.timedelta(days=day_dif)
        date_object -= step
        system_date = open("/opt/airflow/modules/initial_date.txt", "w")
        system_date.write(f"{date_object.strftime('%d-%m-%YT%H:%M:%S')}")
        system_date.close()

def randomlistIdCostumer(size, days_difference = 0):
    return random.sample(range(1000, size+1000), size)

def randomlistID(begin, size):
    return random.sample(range(begin, size+begin), size)

def randomCities(size):
    en_us_faker = Faker()
    final_list = []
    list_20_cities = [en_us_faker.city() for i in range(20)]
    contador = 0
    for i in range(size):
        if i % len(list_20_cities) == 0:
            contador += len(list_20_cities) 
        final_list.append(list_20_cities[i - contador])
    shuffle(final_list)
    return final_list

def listTimeStamp(size):

    date_object = systemDate()
    step = datetime.timedelta(minutes=5)
    result = []
    for i in range(size):
        result.append(date_object.strftime("%d-%m-%YT%H:%M:%S"))
        date_object += step
    return result

def todaysDate():
    return datetime.datetime.now()

def cleanTodaysDate():
    e = datetime.datetime.now()
    return e.strftime("%d-%m-%Y")

def systemDate():
    '''
    READ DATE FROM INITIAL_DATE.TXT
    '''
    f = open("/opt/airflow/modules/initial_date.txt", "r")
    date = datetime.datetime.strptime(f.read(), "%d-%m-%YT%H:%M:%S")
    f.close()
    return date

def updateDate():
    '''
    UPDATE DATE IN INITIAL_DATE.TXT
    '''
    date_object = systemDate()
    step = datetime.timedelta(days=1)
    date_object += step

    system_date = open("/opt/airflow/modules/initial_date.txt", "w")
    system_date.write(f"{date_object.strftime('%d-%m-%YT%H:%M:%S')}")
    system_date.close()
    logging.info(f"Updated date to {date_object.strftime('%d-%m-%YT%H:%M:%S')}")
    return 

def getFirstNames(size):
    first_names = list(set(Provider.first_names))
    seed(4321)
    shuffle(first_names)
    return first_names[0:size+1]

def getLastNames(size):
    last_names = list(set(Provider.last_names))
    seed(4321)
    shuffle(last_names)
    return last_names[0:size+1]

def getRandomAmounts(size):
    return [random.randint(100, 100000) for i in range(size)]

def generateRandomTypes(size):
    return [random.randint(0, 1) for i in range(size)]

def generateStoresIds(size):
    return [random.randint(0, 20) for i in range(size)]

def generatePhoneNumbers(size):
    fake = Faker()
    return [fake.msisdn() for i in range(size)]

def deleteFiles(path, extension):
    directory = path
    files_in_directory = os.listdir(directory)
    filtered_files = [file for file in files_in_directory if file.endswith(extension)]
    for file in filtered_files:
        path_to_file = os.path.join(directory, file)
        os.remove(path_to_file)
    return 

def emptyDirectory(dir):
    for files in os.listdir(dir):
        path = os.path.join(dir, files)
        try:
            shutil.rmtree(path)
        except OSError:
            os.remove(path) 