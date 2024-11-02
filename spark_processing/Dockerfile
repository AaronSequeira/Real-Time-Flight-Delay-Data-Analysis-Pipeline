# Use the specific Spark image
FROM bitnami/spark:3.5.0

WORKDIR /app

# Switch to root to install dependencies
USER root

# Install Python packages
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install boto3

# Copy your Spark application code
COPY spark_streaming.py .

# Set the command (optional if set in docker-compose)
# CMD ["/opt/bitnami/spark/bin/spark-submit", "/app/spark_streaming.py"]

