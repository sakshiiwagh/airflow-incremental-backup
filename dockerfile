FROM apache/airflow:latest

# Switch to root user to install dependencies
USER root

# Update system and install necessary packages
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

# Switch back to the airflow user
USER airflow
