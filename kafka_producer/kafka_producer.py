import os
import pandas as pd
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json

# Retrieve the Kafka bootstrap server from environment variable or default to 'broker:9092'
bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')

# Specify the Kafka API version
api_version = (2, 8, 1)  # Set this to match your Kafka broker version

# Implement retry logic for connecting to the Kafka broker
max_retries = 5
for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=api_version
        )
        print("Connected to Kafka broker")
        break
    except NoBrokersAvailable:
        if attempt < max_retries - 1:
            print("No brokers available, retrying...")
            time.sleep(5)
        else:
            print("Failed to connect to Kafka broker after multiple attempts.")
            raise

# Load the dataset
data = pd.read_csv('/app/Airline_Delay_Cause.csv')

i=0
# Stream each row as a separate message to Kafka
for _, row in data.iterrows():
    producer.send('flight_data', value=row.to_dict())
    print(f"row {i} sent to kafka")
    i+=1
    time.sleep(0.001)  # Add a slight delay to avoid overloading Kafka

print("Data streaming to Kafka completed.")

# Load dataset
# data = pd.read_csv('/app/Airline_Delay_Cause.csv')
# chunk_size = 8000
# # Stream data to Kafka in chunks
# for start in range(0, data.shape[0], chunk_size):
#     chunk = data.iloc[start:start + chunk_size]
#     for _, row in chunk.iterrows():
#         producer.send('flight_data', value=row.to_dict())
#     time.sleep(5)  # Wait before sending the next chunk
