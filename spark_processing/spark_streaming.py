from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, concat_ws
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
import os
# import boto3  # Uncomment if you plan to use boto3 later

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDataProcessing").getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define schema based on the dataset
schema = StructType() \
    .add("year", IntegerType()) \
    .add("month", IntegerType()) \
    .add("carrier", StringType()) \
    .add("carrier_name", StringType()) \
    .add("airport", StringType()) \
    .add("airport_name", StringType()) \
    .add("arr_flights", FloatType()) \
    .add("arr_del15", FloatType()) \
    .add("carrier_ct", FloatType()) \
    .add("weather_ct", FloatType()) \
    .add("nas_ct", FloatType()) \
    .add("security_ct", FloatType()) \
    .add("late_aircraft_ct", FloatType()) \
    .add("arr_cancelled", FloatType()) \
    .add("arr_diverted", FloatType()) \
    .add("arr_delay", FloatType()) \
    .add("carrier_delay", FloatType()) \
    .add("weather_delay", FloatType()) \
    .add("nas_delay", FloatType()) \
    .add("security_delay", FloatType()) \
    .add("late_aircraft_delay", FloatType())

# Define file path for local CSV output
output_file = "/app/processed_flight_data.csv"

# Function to write each batch to a CSV file
def write_to_csv(df, epoch_id):
    if not os.path.isfile(output_file):
        # Write headers if the file does not exist
        df.write.mode("overwrite").csv(output_file, header=True)
    else:
        # Append data if the file already exists
        df.write.mode("append").csv(output_file, header=False)
    pass  # Replace pass with S3 upload code when ready

    # Uncomment and modify the code below when ready to write to S3 using boto3
    """
    # Convert DataFrame to Pandas for boto3 upload
    pandas_df = df.toPandas()
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)
    
    # Upload to S3 using boto3
    s3 = boto3.client('s3')
    s3.put_object(Bucket='your-bucket-name', Key='processed_flight_data.csv', Body=csv_buffer.getvalue())
    """

# Read data stream from Kafka
flight_data_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "flight_data") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse JSON data and apply schema
flight_data = flight_data_stream.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Preprocessing Steps

# 1. Drop rows with more than 50% missing values (threshold for minimum non-null columns)
min_non_nulls = int(len(schema.names) * 0.5) + 1
flight_data = flight_data.na.drop(thresh=min_non_nulls)

# 2. Fill remaining missing values with 0 in count-related columns
fill_values = {field.name: 0 for field in schema.fields if isinstance(field.dataType, (IntegerType, FloatType))}
flight_data = flight_data.fillna(fill_values)

# 3. Rename columns for readability and derived column calculations
flight_data = flight_data \
    .withColumn("date", concat_ws("-", col("year"), col("month"))) \
    .withColumn("delay_percentage", when(col("arr_flights") > 0, (col("arr_del15") / col("arr_flights")) * 100).otherwise(0)) \
    .withColumn("total_delay_by_cause", 
                col("carrier_delay") + col("weather_delay") + col("nas_delay") + col("security_delay") + col("late_aircraft_delay")) \
    .withColumnRenamed("arr_flights", "total_arrivals") \
    .withColumnRenamed("arr_del15", "arrivals_delayed_15_min") \
    .withColumnRenamed("carrier_ct", "carrier_delay_count") \
    .withColumnRenamed("weather_ct", "weather_delay_count") \
    .withColumnRenamed("nas_ct", "nas_delay_count") \
    .withColumnRenamed("security_ct", "security_delay_count") \
    .withColumnRenamed("late_aircraft_ct", "late_aircraft_delay_count") \
    .withColumnRenamed("arr_cancelled", "arrivals_cancelled") \
    .withColumnRenamed("arr_diverted", "arrivals_diverted") \
    .withColumnRenamed("arr_delay", "total_arrival_delay") \
    .withColumnRenamed("carrier_delay", "total_carrier_delay") \
    .withColumnRenamed("weather_delay", "total_weather_delay") \
    .withColumnRenamed("nas_delay", "total_nas_delay") \
    .withColumnRenamed("security_delay", "total_security_delay") \
    .withColumnRenamed("late_aircraft_delay", "total_late_aircraft_delay")

# 4. Outlier Handling: Cap delay_percentage at 100%
flight_data = flight_data.withColumn("delay_percentage", when(col("delay_percentage") > 100, 100).otherwise(col("delay_percentage")))

# Write the processed data to the local CSV file using foreachBatch
query = flight_data.writeStream \
    .foreachBatch(write_to_csv) \
    .outputMode("append") \
    .option("checkpointLocation", "/app/checkpoints") \
    .start()

query.awaitTermination()
