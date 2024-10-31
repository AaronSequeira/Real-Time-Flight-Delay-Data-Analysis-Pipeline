# Use a lightweight Python image as the base
FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the Kafka producer script and dataset
COPY kafka_producer.py .

# Run the Kafka producer script
CMD ["python", "kafka_producer.py"]


