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

![Step # 1](https://github.com/juliotorresma/capstoneDocker/img/1.png?raw=true)