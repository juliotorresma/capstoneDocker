# Capstone Project - Big Data Intenship at GridDynamics

## Mision

As a Data Engineer you will need to design and implement some part of the data platform for a data analytics team. Customers have various sources but data is not unified, so we also need to think about data structure. 
We have 3 sources of data that need to be ingested. These sources are updated every day around 12PM.

* We have following sources:
  - RDBMS (Postresql, or MySQL) (WEB) 
  - Parquet files (MOBILE APP) 
  - Json files (PHYSICAL STORE) 

## Data Schemas:

* RDBMS tables:
  - Transaction
	    - id int,
	    - Customer_id int,
	    - Transaction_ts timestamp,
	    - Amount int
  - Customer
	  - Id int,
	  - First_name int,
	  - Last_name int,
	  - Phone_number int,
	  - Address string

* JSON structure:
{
  - ‘id’:1,
  - “ts”: 2022--06-06T22:06:06, 
  - “Customer_first_name” : “test”
  - “Customer_last_name”: “test”,
  - “Amount”: 1000
  - “Type” : “0” # 0 - in_store, 1-online	

}

* Parquet structure:
  - Id: int
  - Customer: Struct
    - First_name: String
    - Last_name: String
  - Amount: int
  - ts: timestamp,
  - Store_id: int


## Stages:
<ol>

<li>Create data generators : Create data using the 3 formats. Create around 100 entries for each format and for a total of 3 days. </li>

</ol>

* Integrate tools: You can use RDBMS (Postresql, or MySQL) + Spark locally, or you can also choose Docker Desktop or Cloud. 

* Once tools are installed and can communicate please perform the following aggregations in the Aggregations section, and save in the target folder (simulating the Data Warehouse). Make sure the aggregations are performed with at least 3 partitions (you will notice this when saving the data in parquet, you should have 3 files).

* Save the unificated dataset and save the aggregate tables. Save as csv(for debugging purposes) and as parquet . 

* Install Airflow or integrate in Docker / Cloud, then schedule the Spark execution to Aggregate the data of the first day in one hour, next day the next hour, last day the following hour. 

* Install HDFS or use Docker / Cloud and configure spark jobs to save the resulting data there. 

## Aggregations:

* Build next tables:
  - Count and sum amount transactions for each type (online or offline(in store)) for day
  - Count and sum amount transactions for each store for day
  - Count and sum amount transactions for each city (city can be extracted from address) for day


## Installation

* Clone this repository.

```bash
git clone https://github.com/juliotorresma/capstoneDocker.git
```

* Build the project (Install Airflow image with Java Included for PySpark).

```bash
docker-compose build
```

* Run the project.

```bash
docker-compose up
```

## Usage

* Go to http://localhost:8080

* Login with User: airflow and Password: airflow in Airflow Gui.

## Turn on the "data_generation" DAG and open it.
![Step # 1](https://github.com/juliotorresma/capstoneDocker/blob/main/img/1.png?raw=true)
![Step # 2](https://github.com/juliotorresma/capstoneDocker/blob/main/img/2.png?raw=true)

## Trigger your dag.
![Step # 4](https://github.com/juliotorresma/capstoneDocker/blob/main/img/4.png?raw=true)

## Wait for your DAG to complete its tasks (you can see the second task Log with the data aggregations).
![Step # 5](https://github.com/juliotorresma/capstoneDocker/blob/main/img/5.png?raw=true)
![Step # 6](https://github.com/juliotorresma/capstoneDocker/blob/main/img/6.png?raw=true)

## Go to http://localhost:9870

## Go to Hadoop Home - > Utilities -> Browse the file system.
![Step # 7](https://github.com/juliotorresma/capstoneDocker/blob/main/img/7.png?raw=true)

## There you can found your aggregations saved in a partition for each day.
![Step # 8](https://github.com/juliotorresma/capstoneDocker/blob/main/img/8.png?raw=true)
![Step # 9](https://github.com/juliotorresma/capstoneDocker/blob/main/img/9.png?raw=true)
