import base64
import dotenv
import os
import requests
import feedparser
from datetime import datetime, timedelta, timezone
import json
import functions_framework
from google.cloud import pubsub_v1
import time

dotenv.load_dotenv()


def air_alarm_exist(region_id: str) -> bool:
    """
    Checks if an air alarm exists in the specified region using an external API.

    Args:
        region_id (str): The identifier of the region to check for air alarms.

    Returns:
        bool: True if an active air alarm is present; False otherwise.

    Raises:
        requests.exceptions.RequestException: If an error occurs during the API request.

    Usage:
        This function queries an external API to check if there are active air alarms in the specified region.
        It uses the 'ALARM_API_KEY' from environment variables for authorization.

        Example:
        >>> region_id = '123'
        >>> air_alarm_exist(region_id)
        True
        """
    headers = {
        "Authorization": os.getenv("ALARM_API_KEY"),
        "Accept": "application/json"
    }

    url = '{}{}'.format(os.getenv("ALARM_API_ENDPOINT"), region_id)

    try:
        response = requests.get(url, headers=headers)

        response.raise_for_status()

        data = response.json()[0]

        if 'activeAlerts' in data and data['activeAlerts']:
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        print("Error executing the request: {}".format(e))


def get_datetime_diff_in_seconds(string_datetime: str) -> float:
    """
    Calculates the time difference in minutes between the provided string datetime and the current UTC time.

    Args:
        string_datetime (str): A string representing the datetime in the format "%a, %d %b %Y %H:%M:%S %z".

    Returns:
        float: The time difference in minutes.

    Usage:
        This function calculates the time difference between the provided datetime string and the current UTC time.
        The datetime string should be in the format "%a, %d %b %Y %H:%M:%S %z".

        Example:
        >>> datetime_difference = get_datetime_diff_in_seconds("Fri, 17 Nov 2023 17:45:42 +0000")
        >>> print(datetime_difference)
        120.5
    """
    passed_datetime = datetime.strptime(string_datetime, "%a, %d %b %Y %H:%M:%S %z")  # + timedelta(hours=2)

    current_datetime = datetime.now(timezone.utc) + timedelta(hours=2)

    return (current_datetime - passed_datetime).total_seconds()


def message_exist_in_rss() -> bool:
    """
    Checks if there is a message in the specified RSS feed containing keywords within a certain time difference.

    Returns:
        bool: True if a matching message is found; otherwise, False.

    Raises:
        Exception: If data for `ALERT_CHANNEL_RSS_KEYWORDS` or `ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN`
                   is not found in `.env` or `ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN` is less than or equal to 3.

    Usage:
        This function checks if there is a message in the specified RSS feed containing keywords defined in
        `ALERT_CHANNEL_RSS_KEYWORDS` within the time difference specified by `ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN`.

        Example:
        >>> result = message_exist_in_rss()
        >>> print(result)
        True
    """
    keywords = [value.strip().lower() for value in os.getenv("ALERT_CHANNEL_RSS_KEYWORDS").split(',') if value.strip()]
    published_diff_in_seconds = int(os.getenv("ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_SEC"))

    if not keywords:
        raise Exception("Data of `ALERT_CHANNEL_RSS_KEYWORDS` not found in `.env`.")

    if not published_diff_in_seconds or published_diff_in_seconds <= 30:
        raise Exception("Data of `ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN` not found in `.env` or "
                        "`ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_SEC` <= 30.")

    feed = feedparser.parse(os.getenv("ALERT_CHANNEL_RSS"))

    message_exist = False
    for entry in feed.get('entries'):
        title = entry.get('title')
        details = entry.get('summary_detail').get('value')
        published = entry.get('published')

        found_keywords = [keyword for keyword in keywords if keyword in title.lower() or keyword in details.lower()]
        if found_keywords and get_datetime_diff_in_seconds(published) <= published_diff_in_seconds:
            print(title, details)
            message_exist = True

    return message_exist


def generate_notification_event():
    """
    Generates a notification event in eSputnik using the provided credentials and parameters.

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: If an error occurs during the API request.

    Usage:
        This function sends a POST request to the eSputnik Event API to generate a notification event.
        It uses the 'ESPUTNIK_SERVICE_USERNAME', 'ESPUTNIK_SERVICE_PASSWORD', 'ESPUTNIK_EVENT_TYPE_KEY',
        and 'ESPUTNIK_MAIN_USER_ID' from environment variables.

        Example:
        >>> generate_notification_event()
        """
    credentials = "{}:{}".format(os.getenv("ESPUTNIK_SERVICE_USERNAME"), os.getenv("ESPUTNIK_SERVICE_PASSWORD"))
    base64_credentials = base64.b64encode(credentials.encode()).decode()

    payload = {
        "eventTypeKey": os.getenv("ESPUTNIK_EVENT_TYPE_KEY"),
        "keyValue": os.getenv("ESPUTNIK_MAIN_USER_ID")
    }

    headers = {
        "accept": "application/json; charset=UTF-8",
        "content-type": "application/json",
        "authorization": "Basic {}".format(base64_credentials)
    }

    try:
        response = requests.post(os.getenv("ESPUTNIK_EVENT_API_ENDPOINT"), json=payload, headers=headers)

        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        print("Error executing the request: {}".format(e))


def wait():
    """
    Pauses the execution of the script for a specified duration.

    Returns:
        None

    Raises:
        Exception: If the timeout is less than 180 seconds.

    Usage:
        This function is used to introduce a delay in the script execution, allowing time for Cloud Functions to stabilize.
        The duration of the pause is determined by the value of `GCP_CF_TIMEOUT_SEC` from the `.env` file.

        Example:
        >>> wait()
    """
    timeout = int(os.getenv("GCP_CF_TIMEOUT_SEC"))
    if timeout < 180:
        raise Exception("Invalid timeout! There will be an increase in Cloud Function instances.")

    print("Wait {} seconds. After that publish next event to Pub/Sub.".format(timeout))
    time.sleep(timeout)


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


def main():
    """
    The main function orchestrating the workflow for air alarm notifications.

    Returns:
        None

    Usage: This function checks if an air alarm exists in the specified region using the `air_alarm_exist` function.
    If an air alarm is detected, it checks for relevant messages in the RSS feed using the `message_exist_in_rss`
    function. If a relevant message is found, it generates a notification event using the
    `generate_notification_event` function. If no relevant message is found, it introduces a delay using the `wait`
    function and then publishes the next event to Pub/Sub using the `produce_message_to_pub_sub` function.

        Example:
        >>> main()
    """
    region_id = os.getenv("ALARM_API_REGION_ID")
    if not air_alarm_exist(os.getenv("ALARM_API_REGION_ID")):
        print("Air alarm is not exists in region `{}`".format(region_id))
        return

    if message_exist_in_rss():
        generate_notification_event()
        wait()
        produce_message_to_pub_sub()
    else:
        wait()
        produce_message_to_pub_sub()


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def pubsub(cloud_event):
    """
    Cloud Function triggered by Pub/Sub messages.

    Args:
        cloud_event (functions.CloudEvent): CloudEvent representing the Pub/Sub message.

    Returns:
        None

    Usage: This function is triggered by Pub/Sub messages. It decodes the message data and prints it to prove
    successful execution. It then calls the `main` function, which orchestrates the workflow for air alarm
    notifications.

        Example:
        >>> pubsub(cloud_event)

    Note:
        Ensure that the `main` function is correctly implemented and contains the necessary logic for your use case.
    """
    # Print out the data from Pub/Sub, to prove that it worked
    # print(base64.b64decode(cloud_event.data["message"]["data"]))

    main()

    return "Function execution completed."
