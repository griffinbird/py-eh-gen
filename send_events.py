import json
import random
import os
import uuid
import time
import asyncio

from datetime import datetime
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

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
        await send_to_event_hubs(data)
        time.sleep(1)

async def send_to_event_hubs(data):
    load_dotenv()

    EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.getenv('EVENT_HUB_FULLY_QUALIFIED_NAMESPACE')
    EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

    print(f"Sending data to Event Hub: {EVENT_HUB_NAME}"
            f" at {EVENT_HUB_FULLY_QUALIFIED_NAMESPACE}"
    )

    credential = DefaultAzureCredential()

    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credential,
    )

    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add data to the batch.
        event_data_batch.add(EventData(json.dumps(data)))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

        print(f"Sent data: {data}")

        # Close credential when no longer needed.
        await credential.close()

# Call the function to generate and send random data
asyncio.run(generate_random_data())