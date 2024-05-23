import json
import logging
from google.cloud import pubsub_v1, storage
from datetime import datetime
import time
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
cloud_logger = logging.getLogger('cloudLogger')

# Configuration
BUCKET_NAME = "maintenence-bucket"  
PROJECT_ID = "data-engineering-420705"
SUBSCRIPTION_ID = "archivetest-sub"
UPLOAD_INTERVAL = 60  # Interval to upload messages to the bucket (in seconds)
INSTANCE_ID = "test dump"

messages = []
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def callback(message):
    message_data = json.loads(message.data.decode('utf-8'))
    messages.append(message_data)
    message.ack()  # Acknowledge the message
    
    if len(messages) % 1000 == 0:
        print(f"{INSTANCE_ID}: Processed {len(messages)} messages.", end='\r', flush=True)

def sort_and_store_messages():
    if not messages:
        cloud_logger.debug("No messages to process.")
        return

    # Group messages by 'VEHICLE_ID'
    grouped_messages = {}
    for message in messages:
        vehicle_id = message['VEHICLE_ID']
        if vehicle_id not in grouped_messages:
            grouped_messages[vehicle_id] = []
        grouped_messages[vehicle_id].append(message)

    # Sort each group by 'ACT_TIME'
    for vehicle_id in grouped_messages:
        grouped_messages[vehicle_id].sort(key=lambda x: x['ACT_TIME'])

    # Sort vehicle IDs to ensure keys are in order
    sorted_grouped_messages = {k: grouped_messages[k] for k in sorted(grouped_messages)}

    # Generate a filename with the current date
    today_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"TriMet__{today_date}.json"
    folder_name = "data_via_topic"

    # Save all messages as a single JSON object indexed by sorted 'VEHICLE_ID'
    blob = bucket.blob(f"{folder_name}/{filename}")
    blob.upload_from_string(json.dumps(sorted_grouped_messages), content_type='application/json')
    cloud_logger.info(f"All messages processed and saved to GCS. Filename: {filename}, Total vehicles processed: {len(sorted_grouped_messages)}.")
    
    # Clear messages after processing
    messages.clear()

def subscribe_and_process():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logging.info(f"Listening for messages on {subscription_path}...")

    last_upload_time = time.time()

    try:
        while True:
            time.sleep(10)  # Adjust the sleep time as needed
            if time.time() - last_upload_time >= UPLOAD_INTERVAL:
                if messages:
                    sort_and_store_messages()
                last_upload_time = time.time()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        # Ensure any remaining messages are processed
        if messages:
            sort_and_store_messages()

if __name__ == "__main__":
    subscribe_and_process()

