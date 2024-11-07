# Use a PySpark base image
FROM bitnami/spark:latest

# Copy project files into the container
COPY . /StreamingPipeline

# Set the working directory
WORKDIR /StreamingPipeline

# Expose necessary ports for Spark
EXPOSE 8080 7077

RUN pip3 install apache-airflow-providers-apache-spark

# Run an idle command to keep the container running, allowing you to execute Spark jobs manually
ENTRYPOINT ["tail", "-f", "/dev/null"]
