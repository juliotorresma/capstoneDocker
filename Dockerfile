FROM apache/airflow:2.4.1
USER root
RUN apt-get update && apt-get upgrade -y && apt-get install -y default-jre
