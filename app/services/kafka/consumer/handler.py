import asyncio
from asyncio import CancelledError
from multiprocessing import Event
import signal
from sys import stderr
import traceback

from confluent_kafka import Consumer

from database import async_session_maker_with_admin_perm
from kafka_config import config
from kafka_config.protobuf.custom_deserializer import protobuf_kafka_msg_to_dict
from services.kafka.consumer.interface import KafkaConnectionHandlerI
from services.updater.event_handlers.mediator.interface import (
    UpdaterEventMediator,
)
from settings import KafkaConfigs


class KafkaConnectionHandlerImpl(KafkaConnectionHandlerI):
    def __init__(
        self,
        kafka_configs: KafkaConfigs,
        msg_handler: UpdaterEventMediator,
        hierarchy_id: int,
        event: Event = None,
    ):
        self.kafka_configs = kafka_configs
        self.msg_handler = msg_handler
        self.hierarchy_id = hierarchy_id
        self.__event = event or Event()

    @property
    def __connected(self):
        return not self.__event.is_set()

    @__connected.setter
    def __connected(self, v: bool):
        print("set event", v, end=" ")
        if not v:
            self.__event.set()
        else:
            self.__event.clear()
        print(not self.__event.is_set())

    @property
    def is_connected(self):
        return self.__connected

    @property
    def consumer(self):
        return self.__consumer

    def __on_connection_to_kafka_topic(*args, **kwargs):
        print("Connection to the topic")

    @staticmethod
    def __on_disconnect_from_kafka_function():
        print("Disconnected from kafka topic")

    def connect_to_kafka_topic(self):
        if not self.__connected:
            return
        try:
            self.__on_connection_to_kafka_topic()
            connection_data = self.kafka_configs.get_connection_settings_for_special_hierarchy(
                hierarchy_id=self.hierarchy_id
            )
            print(connection_data)
            self.__consumer = Consumer(connection_data)
            # asyncio.run(self.__start_to_read_connect_to_kafka_topic(hierarchy_id=self.hierarchy_id))
            loop = asyncio.new_event_loop()
            asyncio.get_event_loop()
            loop.add_signal_handler(
                signal.SIGTERM, self.disconnect_from_kafka_topic
            )
            try:
                loop.run_until_complete(
                    self.__start_to_read_connect_to_kafka_topic(
                        hierarchy_id=self.hierarchy_id
                    )
                )
            except (CancelledError, KeyboardInterrupt):
                loop.close()
        except Exception:
            print(traceback.format_exc(), file=stderr)
            asyncio.run(asyncio.sleep(5))
            self.connect_to_kafka_topic()

    async def __start_to_read_connect_to_kafka_topic(self, hierarchy_id: int):
        print("HERER")
        print(self.kafka_configs.topics)
        self.__consumer.subscribe(self.kafka_configs.topics)
        self.__connected = True
        print(f"Done {hierarchy_id}")
        try:
            while self.__connected:
                msg = self.__consumer.poll(2)

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
                    message_as_dict = protobuf_kafka_msg_to_dict(
                        msg=msg_object, including_default_value_fields=True
                    )
                    async with async_session_maker_with_admin_perm() as session:
                        await self.msg_handler.handle_the_message(
                            msg=message_as_dict,
                            class_name=msg_class_name,
                            event=msg_event,
                            session=session,
                            hierarchy_id=hierarchy_id,
                        )

                self.__consumer.commit(asynchronous=True, message=msg)
        except Exception as e:
            print(traceback.format_exc(), file=stderr)
            print("HERER WAS ")
            self.disconnect_from_kafka_topic()
            raise e

    def disconnect_from_kafka_topic(self):
        print("disconnect_from_kafka_topic")
        self.__consumer.close()
        self.__connected = False
        self.__on_disconnect_from_kafka_function()
