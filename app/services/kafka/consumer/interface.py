from abc import abstractmethod

from services.meta_singleton.impl import SingletonABCMeta


class KafkaConnectionHandlerI(metaclass=SingletonABCMeta):
    @abstractmethod
    def connect_to_kafka_topic(self):
        pass

    @abstractmethod
    def disconnect_from_kafka_topic(self):
        pass
