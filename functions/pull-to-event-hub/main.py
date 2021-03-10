import json
import logging
import os
import time
from concurrent.futures import TimeoutError
from datetime import datetime

import utils
from azure.eventhub import EventData, EventHubProducerClient
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logging.getLogger("google.cloud.pubsub_v1").setLevel(logging.WARNING)


event_hub_connection_string = utils.get_secret(
    os.environ["PROJECT_ID"], os.environ["CONNECTION_SECRET"]
)

event_hub_name = utils.get_secret(
    os.environ["PROJECT_ID"], os.environ["EVENTHUB_SECRET"]
)

FUNCTION_TIMEOUT = int(os.getenv("FUNCTION_TIMEOUT", "500"))


def pull_to_event_hub(request):

    """
    Handler function that subscribes to gcp pub/sub
    and sends messages to azure event hub.
    """

    last_message_received_time = datetime.now()

    def callback(msg):
        """
        Callback function for pub/sub subscriber.
        """

        last_message_received_time = datetime.now()  # noqa: F841

        event = {
            "message": msg.data.decode(),
            "subscription": subscription_path.split("/")[-1],
        }

        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        logging.info(f"Sending {batch.size_in_bytes} bytes of messages...")
        producer.send_batch(batch)

        msg.ack()

    subscription_path = request.data.decode("utf-8")

    logging.info("Creating Azure producer...")
    producer = EventHubProducerClient.from_connection_string(
        conn_str=event_hub_connection_string, eventhub_name=event_hub_name
    )

    logging.info("Creating GCP subscriber...")
    subscriber = pubsub_v1.SubscriberClient()
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
        flow_control=pubsub_v1.types.FlowControl(max_messages=25),
    )

    logging.info(f"Listening for messages on {subscription_path}...")

    start = datetime.now()

    try:
        while True:

            # limit the duration of the function
            if (datetime.now() - start).total_seconds() > FUNCTION_TIMEOUT:
                streaming_pull_future.cancel(await_msg_callbacks=True)
                break

            # assume there are no more messages
            if (datetime.now() - last_message_received_time).total_seconds() > 2:
                streaming_pull_future.cancel(await_msg_callbacks=True)
                break

            time.sleep(1)

    except TimeoutError:
        streaming_pull_future.cancel(await_msg_callbacks=True)
    except Exception:
        logging.exception(
            f"Listening for messages on {subscription_path} threw an exception."
        )
    finally:
        producer.close()

    return "OK", 204
