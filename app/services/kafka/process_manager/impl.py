import dataclasses
from multiprocessing import Event, Process
from typing import Callable

from sqlalchemy import select
from sqlalchemy.event import listen
from sqlalchemy.orm import Session

from database import database
from schemas.hier_schemas import Hierarchy
from services.kafka.consumer.handler import KafkaConnectionHandlerImpl
from services.kafka.consumer.interface import KafkaConnectionHandlerI
from services.meta_singleton.impl import SingletonMeta
from services.session_utils.listeners.processes.inner_listener import (
    process_session_receive_after_commit,
    process_session_receive_after_flush,
)
from services.updater.event_handlers.mediator.impl import (
    UpdaterEventMediatorImpl,
)
from settings import KafkaConfigs


@dataclasses.dataclass(frozen=True, slots=True)
class ProcessInfo:
    process: Process
    event: Event


def target(hierarchy_id: int, event: Event):
    # add session listeners
    listen(Session, "after_flush", process_session_receive_after_flush)
    listen(Session, "after_commit", process_session_receive_after_commit)

    kafka_config = KafkaConfigs()
    msg_handler = UpdaterEventMediatorImpl()
    handler = KafkaConnectionHandlerImpl(
        kafka_configs=kafka_config,
        msg_handler=msg_handler,
        hierarchy_id=hierarchy_id,
        event=event,
    )
    handler.connect_to_kafka_topic()


class KafkaConsumerProcessManager(metaclass=SingletonMeta):
    def __init__(
        self,
        target_callable: Callable = None,
        default_kafka_consumer_handler: KafkaConnectionHandlerI = None,
    ):
        self.__temporary_process_db: dict[int, ProcessInfo] = dict()
        self.default_kafka_consumer_handler = default_kafka_consumer_handler
        self.target_callable = target_callable

    def start_new_process_for_hierarchy_id(self, hierarchy_id: int):
        if self.get_process_info_by_hierarchy_id(hierarchy_id):
            print(
                f"Kafka Consumer Process for hierarchy id {hierarchy_id} already exist"
            )
        else:
            e = Event()
            p = Process(
                target=self.target_callable,
                kwargs={"hierarchy_id": hierarchy_id, "event": e},
                daemon=True,
            )
            self.__start_new_process(p)
            self.__temporary_process_db[hierarchy_id] = ProcessInfo(
                process=p, event=e
            )

    @staticmethod
    def __start_new_process(process_inst: Process):
        process_inst.start()

    def get_process_info_by_hierarchy_id(
        self, hierarchy_id: int
    ) -> ProcessInfo | None:
        return self.__temporary_process_db.get(hierarchy_id)

    def stop_process_for_hierarchy_id(self, hierarchy_id: int):
        process_info = self.get_process_info_by_hierarchy_id(
            hierarchy_id=hierarchy_id
        )
        if not process_info:
            print(
                f"Kafka Consumer Process for hierarchy id {hierarchy_id} does not exist"
            )
            return
        process_inst = process_info.process

        if process_inst is None:
            print(
                f"Kafka Consumer Process for hierarchy id {hierarchy_id} do not exist"
            )
        else:
            print(
                f"{process_inst.name=}, {process_inst.is_alive()=},{process_inst.pid=}, {process_inst.ident=},"
            )
            self.__stop_process(process_info)
            process_inst.join()
            self.__temporary_process_db.pop(hierarchy_id, None)
            print(
                f"{process_inst.name=}, {process_inst.is_alive()=},{process_inst.pid=}, {process_inst.ident=},"
            )
            print("after join", len(self.__temporary_process_db))

    @staticmethod
    def __stop_process(process_info: ProcessInfo):
        print("event set", flush=True)
        process_info.event.set()

    def stop_all_processes(self):
        for p in self.__temporary_process_db.values():
            self.__stop_process(p)
        for p in self.__temporary_process_db.values():
            p.process.join()
        self.__temporary_process_db = dict()


async def init_all_kafka_consumer_processes_with_admin_session():
    pm = KafkaConsumerProcessManager(target_callable=target)
    stmt = select(Hierarchy)
    async with database.async_session_factory() as session:
        all_h = (await session.execute(stmt)).scalars().all()
    for h in all_h:
        pm.start_new_process_for_hierarchy_id(hierarchy_id=h.id)


if __name__ == "__main__":
    init_all_kafka_consumer_processes_with_admin_session()
