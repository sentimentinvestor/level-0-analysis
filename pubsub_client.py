import os
from google.cloud import pubsub_v1

credential_path = r"admin_credentials/pubsub-creds.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
publisher_client = pubsub_v1.PublisherClient()


def publisher():
    return publisher_client
