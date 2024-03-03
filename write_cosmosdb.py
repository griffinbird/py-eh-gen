import json
import random
import os
import uuid
import time
import asyncio

from datetime import datetime

from azure.cosmos import CosmosClient
from azure.core.exceptions import AzureError
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential

load_dotenv()

CDB_ENDPOINT = os.getenv('CDB_URI')
CDB_CONNECTON_STRING = os.getenv('CDB_CNN_STRING')
DATABASE_NAME = os.getenv('CDB_DB_NAME')
CONTAINER_NAME = os.getenv('CDB_CONTAINER_NAME')

#credential = DefaultAzureCredential()

#client = CosmosClient(CDB_ENDPOINT, credential)

async def generate_random_data():
    while True:
        data = {
            "id": str(uuid.uuid4()),
            "EstimatedArrivalTime": datetime.now().strftime("%m/%d/%Y %H:%M"),
            "Destination": "Downtown",
            "BusRouteID": random.randint(100, 999),
            "Occupancy": random.randint(1, 10),
            "RouteVia": "Courthouse",
            "DepartureTime": datetime.now().strftime("%m/%d/%Y %H:%M"),
            "BusNo": random.randint(1, 100),
            "Capacity": 60,
            "WithHTAP": random.randint(0, 1)
        }
        #print("Datatype before conversion: ",type(data))
        #event_dict = json.loads(json.dumps(data)) 
        await write_to_cosmosdb(data)
        time.sleep(1)

async def write_to_cosmosdb(data):
    #print("Datatype before conversion: ",type(data))
    # Check if data is a dictionary.
    if not isinstance(data, dict):
        print(f"Error: data is not a dictionary. Data: {data}")
        return
    # print the data
    #print(f"Read: {data}")
    
    # get the client
    client = CosmosClient.from_connection_string(CDB_CONNECTON_STRING)
    # get the database
    database = client.get_database_client(DATABASE_NAME)
    # get the container
    container = database.get_container_client(CONTAINER_NAME)

    # write the data to the container
    container.create_item(body=data)
    # print the data written to the container
    print(f"Data written to CosmosDB: {data}")

# Call the function to generate and send random data
asyncio.run(generate_random_data())