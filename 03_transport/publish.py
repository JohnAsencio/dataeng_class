from google.cloud import pubsub_v1
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# TODO(developer)
project_id = "data-engineering-420705"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

with open('bcsample.json', 'r') as file:
    data = json.load(file)

    # Iterate over each vehicle's data
    for record in data:
        # Convert record to JSON string
        record_json = json.dumps(record)
        # Publish the JSON record as a message
        future = publisher.publish(topic_path, data=record_json.encode("utf-8"))
   #     print(f"Published record to Pub/Sub: {future.result()}")
print(f"Published all messages to {topic_path}.")
