import os
import random
import shutil
import datetime
from random import shuffle, seed
from faker.providers.person.en import Provider
from faker.providers.address import Provider as prcity
import faker
from faker import Faker

def randomlistIdCostumer(size):
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
    return final_list # random.choice(list_20_cities)

def listTimeStamp(size):
    dt = datetime.datetime(2021, 12, 1)
    step = datetime.timedelta(minutes=5)
    result = []
    for i in range(size):
        result.append(dt.strftime("%d-%m-%YT%H:%M:%S"))
        dt += step
    return result

def timeStamp():
    return datetime.now().timestamp()

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