from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv
import os

# Create a Spark session
spark = SparkSession.builder.appName("AzureEventHubReader").getOrCreate()

# Define the Event Hub connection string and consumer group
event_hub_name = "EVENT_HUB_NAME"  # Replace "YOUR_EVENT_HUB_NAME" with the actual event hub name
consumer_group = "$Default"

# Define the schema for the incoming data
schema = StructType([
    StructField("message", StringType())
])

# Read data from the Event Hub
credential = DefaultAzureCredential()
df = spark.readStream \
    .format("eventhubs") \
    .option("eventhubs.credential", credential) \
    .option("eventhubs.consumerGroup", consumer_group) \
    .option("eventhubs.name", event_hub_name) \
    .load()

# Check if connection is successful
df.isStreaming # Returns True
print("Connection successful")

# Parse the JSON message
parsed_df = df.select(from_json(col("body").cast("string"), schema).alias("data"))

# Print the data to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

# Stop the Spark session
spark.stop()
