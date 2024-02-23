import asyncio
import os
import json
import uuid

from azure.eventhub.aio import EventHubConsumerClient
from azure.core.exceptions import AzureError
from azure.cosmos import CosmosClient, PartitionKey
#from azure.eventhub.extensions.checkpointstoreblobaio import (
#    BlobCheckpointStore,
#)
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

BLOB_STORAGE_ACCOUNT_URL = "BLOB_STORAGE_ACCOUNT_URL"
BLOB_CONTAINER_NAME = "BLOB_CONTAINER_NAME"
EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.getenv('EVENT_HUB_FULLY_QUALIFIED_NAMESPACE')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')
CONNECTION_STRING = os.getenv('CDB_URI')
DATABASE_NAME = os.getenv('CDB_DB_NAME')
CONTAINER_NAME = os.getenv('CDB_CONTAINER_NAME')

credential = DefaultAzureCredential()

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    # checkpoint_store = BlobCheckpointStore(
    #    blob_account_url=BLOB_STORAGE_ACCOUNT_URL,
    #    container_name=BLOB_CONTAINER_NAME,
    #    credential=credential,
    #)

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient(
        fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        consumer_group="$Default",
        # checkpoint_store=checkpoint_store,
        credential=credential,
    )
    async with client:
        # Call the receive method. Read from the beginning of the partition
        # (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")

    # Close credential when no longer needed.
    await credential.close()

async def on_event(partition_context, event):
    # Store event data in a variable.
    event_data = event.body_as_str(encoding="UTF-8")

    # Print the event data.
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event_data, partition_context.partition_id
        )
    )

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    # await partition_context.update_checkpoint(event_data)

    # Try to convert the event data to a dictionary.
    try:
        event_dict = json.loads(event_data)
    except json.JSONDecodeError:
        print(f"Error: event data is not valid JSON. Data: {event_data}")
        return

    # Add an id property to the event.
    event_dict['id'] = str(uuid.uuid4())
    
    await write_to_cosmosdb(event_dict)


async def write_to_cosmosdb(data):
    # Check if data is a dictionary.
    if not isinstance(data, dict):
        print(f"Error: data is not a dictionary. Data: {data}")
        return
    # print the data
    print(f"Data: {data}")
    
    # get the client
    client = CosmosClient.from_connection_string(CONNECTION_STRING)
    # get the database
    database = client.get_database_client(DATABASE_NAME)
    # get the container
    container = database.get_container_client(CONTAINER_NAME)

    # write the data to the container
    container.create_item(body=data)
    # print the data written to the container
    print(f"Data written to CosmosDB: {data}")


if __name__ == "__main__":
    # Run the main method.
    asyncio.run(main())