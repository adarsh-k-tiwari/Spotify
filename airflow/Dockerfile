FROM apache/airflow:latest

# Install Kafka provider for Airflow
RUN pip install apache-airflow-providers-apache-kafka
RUN pip install apache-airflow-providers-snowflake
RUN pip install requests
RUN pip install kafka-python

# Set the default command
CMD ["airflow", "scheduler"]