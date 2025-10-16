import functools
import math
import time
from typing import Union

from confluent_kafka import Producer
from fastapi import HTTPException
import requests

from kafka_producer import config
from kafka_producer.config import KAFKA_PRODUCER_MSG_MAX_MSG_LEN
from kafka_producer.model_mediator import protobuf_producer_model_mediator
from services.obj_events.status import ObjEventStatus


def _get_token_for_kafka_producer(conf):
    """Get token from Keycloak for MS Inventory kafka producer and returns it with
    expires time"""
    payload = {
        "grant_type": "client_credentials",
        "scope": str(config.KAFKA_KEYCLOAK_SCOPES),
    }

    attempt = 5
    while attempt > 0:
        try:
            resp = requests.post(
                config.KAFKA_KEYCLOAK_TOKEN_URL,
                auth=(
                    config.KAFKA_KEYCLOAK_CLIENT_ID,
                    config.KAFKA_KEYCLOAK_CLIENT_SECRET,
                ),
                data=payload,
                timeout=5,
            )
        except ConnectionError:
            time.sleep(1)
            attempt -= 1
        else:
            if resp.status_code == 200:
                break
            else:
                time.sleep(1)
                attempt -= 1
                continue
    else:
        raise HTTPException(
            status_code=503, detail="Token verification service unavailable"
        )

    token = resp.json()

    return token["access_token"], time.time() + float(token["expires_in"])


def producer_config():
    if not config.KAFKA_SECURED:
        return config.KAFKA_PRODUCER_CONNECT_CONFIG

    config_dict = dict()
    config_dict.update(config.KAFKA_PRODUCER_CONNECT_CONFIG)
    config_dict["oauth_cb"] = functools.partial(_get_token_for_kafka_producer)
    return config_dict


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        "User record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


def prepare_msg_for_kafka(
    obj_class_name: str, event: ObjEventStatus, items_data: Union[list, set]
):
    obj_class_info = protobuf_producer_model_mediator(obj_class_name)

    if obj_class_info:
        obj_proto_list_temp = obj_class_info["proto_list_template"]
        obj_proto_unit_temp = obj_class_info["proto_unit_template"]

        try:
            res_proto = [
                obj_proto_unit_temp(**item_data) for item_data in items_data
            ]
            res_message = obj_proto_list_temp(objects=res_proto)

        except ValueError:
            print("Invalid input, discarding record...")

        else:
            if len(res_proto) > KAFKA_PRODUCER_MSG_MAX_MSG_LEN:
                parts = math.ceil(
                    len(res_proto) / KAFKA_PRODUCER_MSG_MAX_MSG_LEN
                )
                for step in range(parts):
                    start = step * KAFKA_PRODUCER_MSG_MAX_MSG_LEN
                    end = start + KAFKA_PRODUCER_MSG_MAX_MSG_LEN
                    message_data = res_proto[start:end]
                    res_message = obj_proto_list_temp(objects=message_data)
                    send_to_kafka(res_message, obj_class_name, event)
            else:
                send_to_kafka(res_message, obj_class_name, event)


def send_to_kafka(msg, obj_class_name: str, event: ObjEventStatus):
    prod_config = producer_config()
    producer = Producer(prod_config)
    producer.produce(
        topic=config.KAFKA_PRODUCER_TOPIC,
        key=str(obj_class_name) + ":" + event.value,
        value=msg.SerializeToString(),
        on_delivery=delivery_report,
    )
    producer.flush()
