#%%
import boto3
import json
import time
from faker import Faker
import random

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-2')
fake = Faker()

def generate_click_event():
    return {
        "user_id": str(random.randint(1, 1000)),
        "event": "click",
        "page": random.choice(["homepage", "product", "cart", "checkout"]),
        "timestamp": fake.iso8601()
    }

def send_to_kinesis(stream_name, data):
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=data['user_id']
        )
        print(f"Sent record to shard: {response['ShardId']} at {time.time()}")
        return response
    except Exception as e:
        print(f"Error sending record: {str(e)}")
        raise
#%%
# Simulate continuous data
stream_name = 'SimulatedClickStream'
while True:
    event = generate_click_event()
    send_to_kinesis(stream_name, event)
    time.sleep(0.5)  # Simulate data every 0.5 seconds
# %%
