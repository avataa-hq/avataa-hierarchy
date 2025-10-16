import asyncio
import functools
from typing import Callable

from confluent_kafka import Consumer

from kafka_config import config
from kafka_config.batch_change_handler.utils.rebuild_order_handler import (
    HierarchyRebuildOrderChecker,
)
from kafka_config.protobuf.custom_deserializer import protobuf_kafka_msg_to_dict
from kafka_config.utils import consumer_config
from services.meta_singleton.impl import SingletonMeta


class KafkaConnectionHandler(metaclass=SingletonMeta):
    DEFAULT_COUNT_OF_EMPTY_MSG_TO_CHECK_REBUILD_ORDER = 30

    def __init__(
        self, loop=None, async_message_handler_function: Callable = None
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.number_of_consecutive_empty_messages = -1
        self.__connected = False
        self.__consumer = None
        self.message_handler_function = async_message_handler_function

    @property
    def message_handler_function(self):
        return self._message_handler_function

    @message_handler_function.setter
    def message_handler_function(self, value):
        if value is None:

            async def noname(*args, **kwargs):
                return None

            self._message_handler_function = noname
        else:
            self._message_handler_function = value

    @property
    def is_connected(self):
        return self.__connected

    @property
    def consumer(self):
        return self.__consumer

    @staticmethod
    def on_connection_to_kafka_topic(*args, **kwargs):
        print("Connection to the topic")

    @staticmethod
    def on_disconnect_from_kafka_function():
        print("Disconnected from kafka")

    async def __increase_empty_msg_counter(self):
        self.number_of_consecutive_empty_messages += 1
        if (
            self.number_of_consecutive_empty_messages
            >= self.DEFAULT_COUNT_OF_EMPTY_MSG_TO_CHECK_REBUILD_ORDER
        ):
            self.disconnect_from_kafka_topic()
            rebuild_order_checker = HierarchyRebuildOrderChecker()
            await rebuild_order_checker.check_order_and_rebuild_hierarchy()
            self.connect_to_kafka_topic()
            self.number_of_consecutive_empty_messages = -1

    def __reset_empty_msg_counter(self):
        self.number_of_consecutive_empty_messages = -1

    def connect_to_kafka_topic(self):
        if self.__connected:
            return
        try:
            self.on_connection_to_kafka_topic()
            self.__consumer = Consumer(
                consumer_config(config.KAFKA_CONSUMER_CONNECT_CONFIG)
            )
            self.loop.create_task(self.__start_to_read_connect_to_kafka_topic())
        except Exception:
            self.loop.create_task(asyncio.sleep(5))
            self.connect_to_kafka_topic()

    async def __start_to_read_connect_to_kafka_topic(self):
        self.__consumer.subscribe(
            config.KAFKA_SUBSCRIBE_TOPICS,
            # on_assign=self.on_connection_to_kafka_topic
        )
        self.__connected = True

        while self.__connected:
            try:
                loop = asyncio.get_running_loop()
                poll = functools.partial(self.__consumer.poll, 2.0)
                msg = await loop.run_in_executor(None, poll)

                # await self.__increase_empty_msg_counter()

                if msg is None:
                    continue

                if getattr(msg, "key", None) is None:
                    self.__consumer.commit(asynchronous=True, message=msg)
                    continue

                if msg.key() is None:
                    self.__consumer.commit(asynchronous=True, message=msg)
                    continue

                msg_key = msg.key().decode("utf-8")
                if msg_key.find(":") == -1:
                    self.__consumer.commit(asynchronous=True, message=msg)
                    continue

                msg_class_name, msg_event = msg_key.split(":")
                if (
                    msg_class_name
                    not in config.KAFKA_PROTOBUF_DESERIALIZERS.keys()
                ):
                    self.__consumer.commit(asynchronous=True, message=msg)
                    continue

                msg_object = config.KAFKA_PROTOBUF_DESERIALIZERS[
                    msg_class_name
                ]()
                msg_object.ParseFromString(msg.value())

                if msg_object is not None:
                    # self.__reset_empty_msg_counter()

                    message_as_dict = protobuf_kafka_msg_to_dict(
                        msg=msg_object, including_default_value_fields=True
                    )

                    await self.message_handler_function(
                        msg_class_name, msg_event, message_as_dict
                    )

                self.__consumer.commit(asynchronous=True, message=msg)
            except Exception as e:
                self.disconnect_from_kafka_topic()
                raise e

    def disconnect_from_kafka_topic(self):
        self.__consumer.close()
        self.__connected = False
        self.on_disconnect_from_kafka_function()


def sync_kafka_stopping_to_perform_a_function(func: Callable):
    """SYNC Decorator function. The decorator stops listening to Kafka while the function is executing"""

    def wrapper(*args, **kwargs):
        kafka_connection_handler = KafkaConnectionHandler()
        if kafka_connection_handler.is_connected:
            kafka_connection_handler.disconnect_from_kafka_topic()
            res = func(*args, **kwargs)
            kafka_connection_handler.connect_to_kafka_topic()
        else:
            res = func(*args, **kwargs)
        return res

    return wrapper


def async_kafka_stopping_to_perform_a_function(func: Callable):
    """ASYNC Decorator function. The decorator stops listening to Kafka while the function is executing"""
    func = func

    async def wrapper(*args, **kwargs):
        kafka_connection_handler = KafkaConnectionHandler()
        if kafka_connection_handler.is_connected:
            kafka_connection_handler.disconnect_from_kafka_topic()
            res = await func(*args, **kwargs)
            kafka_connection_handler.connect_to_kafka_topic()
        else:
            res = await func(*args, **kwargs)
        return res

    return wrapper
