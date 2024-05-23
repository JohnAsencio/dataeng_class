import requests
import pandas as pd
from datetime import datetime
from google.cloud import pubsub_v1
import json
import time
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_vehicle_ids():
    doc_key = "10VKMye65LhbEgMLld5Ol3lOocWUwCaEgnPVgFQf9em0"
    url = f"https://docs.google.com/spreadsheets/d/{doc_key}/export?format=csv"
    response = requests.get(url)
    csv_data = response.content
    with open("vehicle_ids_sheet.csv", "wb") as file:
        file.write(csv_data)
    vehicle_ids = pd.read_csv("vehicle_ids_sheet.csv")['Doodle'].tolist()
    return vehicle_ids

def publish_breadcrumbs():
    project_id = "data-engineering-420705"
    topic_id = "archivetest"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    vehicle_ids = get_vehicle_ids()

    for vehicle_id in tqdm(vehicle_ids, desc="Processing vehicle IDs"):
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
        response = requests.get(url)
        if response.status_code == 200:
            breadcrumbs = response.json()
            for breadcrumb in breadcrumbs:
                data_str = json.dumps(breadcrumb)
                data_bytes = data_str.encode('utf-8')
                future = publisher.publish(topic_path, data_bytes)
#                logging.info(f"Published message: {data_str}")
        time.sleep(1)  # Respectful delay to avoid rate limiting

    print('Published to', topic_path, data_bytes)

if __name__ == "__main__":
    publish_breadcrumbs()

