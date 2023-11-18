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


def air_alarm_exist(region_id):
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


def generate_notification_event():
    # {'status': 'DEACTIVATE', 'regionId': 12, 'alarmType': 'AIR', 'createdAt': '2023-11-18T09:04:05Z'}

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


def get_datetime_diff_in_minutes(string_datetime):
    passed_datetime = datetime.strptime(string_datetime, "%a, %d %b %Y %H:%M:%S %z") + timedelta(hours=2)

    current_datetime = datetime.now(timezone.utc) + timedelta(hours=2)

    return (current_datetime - passed_datetime).total_seconds() / 60


def message_exist_in_rss():
    keywords = [value.strip().lower() for value in os.getenv("ALERT_CHANNEL_RSS_KEYWORDS").split(',') if value.strip()]
    published_diff_in_minutes = int(os.getenv("ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN"))

    if not keywords:
        raise Exception("Data of `ALERT_CHANNEL_RSS_KEYWORDS` not found in `.env`.")

    if not published_diff_in_minutes or published_diff_in_minutes <= 3:
        raise Exception("Data of `ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN` not found in `.env` or "
                        "`ALERT_CHANNEL_RSS_PUBLISHED_DIFF_IN_MIN` <= 3.")

    feed = feedparser.parse(os.getenv("ALERT_CHANNEL_RSS"))

    message_exist = False
    for entry in feed.get('entries'):
        title = entry.get('title')
        details = entry.get('title_detail').get('value')
        published = entry.get('published')

        found_keywords = [keyword for keyword in keywords if keyword in title.lower() or keyword in details.lower()]
        if found_keywords and get_datetime_diff_in_minutes(published) <= published_diff_in_minutes:
            print(title)
            message_exist = True

    return message_exist


def produce_message_to_pub_sub():
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


def wait():
    timeout = int(os.getenv("GCP_CF_TIMEOUT_SEC"))
    if timeout < 180:
        raise Exception("Invalid timeout! There will be an increase in Cloud Function instances.")

    print("Wait {} seconds. After that publish next event to Pub/Sub.".format(timeout))
    time.sleep(timeout)


def main():
    region_id = os.getenv("ALARM_API_REGION_ID")
    if not air_alarm_exist(os.getenv("ALARM_API_REGION_ID")):
        print("Air alarm is not exists in region `{}`".format(region_id))
        return

    if message_exist_in_rss():
        generate_notification_event()
    else:
        wait()
        produce_message_to_pub_sub()
