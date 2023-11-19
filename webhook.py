import functions_framework
from google.cloud import pubsub_v1
import json
import os
import dotenv

AIR_ALARM_ACTIVATE_STATUS = 'ACTIVATE'
AIR_ALARM_DEACTIVATE_STATUS = 'DEACTIVATE'


dotenv.load_dotenv()


def check_region_for_alarm(data: dict) -> bool:
    """
    Checks whether an air alarm was triggered in the specified region.

    Args:
        data (dict): A dictionary containing webhook data. Expects the presence of keys 'status' and 'regionId'.

    Returns:
        bool: True if an air alarm was triggered in the region; False otherwise.

    Raises:
        Exception: If 'ALARM_API_REGION_ID' is not found in the environment variables.
        Exception: If keys 'status' or 'regionId' are missing in the data argument.
                   The error message will include details (Details: {data}).

    Usage:
        >>> data = {'status': 'activate', 'regionId': '123'}
        >>> check_region_for_alarm(data)
        True
    """
    valuable_region_id = os.getenv('ALARM_API_REGION_ID')
    status = data.get('status')
    region_id = data.get('regionId')

    if not valuable_region_id:
        raise Exception("Not found `ALARM_API_REGION_ID` in `.env`.")

    if not status or not region_id:
        raise Exception("Not found `status` or `regionId` in webhook. Details: {}".format(data))

    return status.upper() == AIR_ALARM_ACTIVATE_STATUS and region_id == valuable_region_id


def produce_message_to_pub_sub():
    """
    Publishes a message to a Google Cloud Pub/Sub topic.

    The function uses the values of 'GCP_PROJECT_ID' and 'GCP_TOPIC_ID' from the environment variables
    to determine the project ID and topic ID for publishing the message.

    Raises:
        Exception: If 'GCP_PROJECT_ID' or 'GCP_TOPIC_ID' is missing in the environment variables.

    Usage:
        >>> produce_message_to_pub_sub()
        Message published.
    """
    publisher = pubsub_v1.PublisherClient()
    project_id = os.getenv("GCP_PROJECT_ID")
    topic_id = os.getenv("GCP_TOPIC_ID")

    if not topic_id or not project_id:
        raise Exception("Missing `GCP_PROJECT_ID` or `GCP_TOPIC_ID` in `.env`.")

    topic_path = publisher.topic_path(project_id, topic_id)

    message_json = json.dumps(
        {
            "data": {"message": "Air Alert!"},
        }
    )
    message_bytes = message_json.encode("utf-8")

    publish_future = publisher.publish(topic_path, data=message_bytes)
    publish_future.result()
    print("Message published.")

@functions_framework.http
def http(request):
    """
    Handles incoming HTTP requests, specifically designed for webhook processing.

    Args:
        request (flask.Request): The incoming request object.

    Returns:
        tuple: A tuple containing a response message and HTTP status code.

    Raises:
        Exception: If an internal server error occurs during request processing.

    Usage:
        This function is typically deployed as a Cloud Function to handle incoming webhook requests.
        It checks if the request is a POST method, processes the payload for air alarm alerts,
        and publishes a message to a Google Cloud Pub/Sub topic if an air alarm is detected.
    """
    try:
        if request.method == "POST":
            request_json = request.get_json(silent=True)
            if check_region_for_alarm(request_json) is True:
                print("[IMPORTANT] Air alert in region.")
                produce_message_to_pub_sub()
            else:
                print("[OTHER] Air alert! Details: {}".format(request_json))

            return "Done.", 200
        else:
            return "Not allowed request type", 405
    except Exception as e:
        print(e)
        return "Internal server error!", 500

