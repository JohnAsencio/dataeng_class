from google.cloud import pubsub_v1

project_id = "data-engineering-420705"
subscription_id = "my-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    pass

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}.. and discarding them.\n")
    try:
        streaming_pull_future.result()  
    except KeyboardInterrupt:
        streaming_pull_future.cancel()  
