"""
Listeners for kafka
"""

import time

from kafka_producer.config import KAFKA_PRODUCER_TURN_ON
from kafka_producer.model_mediator import MODEL_EQ_MESSAGE
from kafka_producer.producer import prepare_msg_for_kafka
from services.kafka.process_manager.lisnter_handler import (
    process_manager_mediator_for_hierarchies,
)
from services.obj_events.status import ObjEventStatus
from services.session_utils.listeners.enum_models import SessionDataKeys


# @event.listens_for(Session, 'after_flush')
def receive_after_flush(session, flush_context):
    """listen for the 'after_flush' event"""

    def session_data_handler(
        session_data, key_for_session_data: SessionDataKeys
    ):
        if not session.info.get(key_for_session_data.value, False):
            session.info.setdefault(key_for_session_data.value, dict())

        for item in session_data:
            item_class_name = type(item).__name__
            if item_class_name in MODEL_EQ_MESSAGE.keys():
                if not session.info[key_for_session_data.value].get(
                    item_class_name, False
                ):
                    session.info[key_for_session_data.value][
                        item_class_name
                    ] = list()
                session.info[key_for_session_data.value][
                    item_class_name
                ].append(item.to_proto())

    if session.new:
        session_data_handler(session.new, SessionDataKeys.NEW)

    if session.deleted:
        session_data_handler(session.deleted, SessionDataKeys.DELETED)

    if session.dirty:
        session_data_handler(session.dirty, SessionDataKeys.DIRTY)


# @event.listens_for(Session, "after_commit")
def receive_after_commit(session):
    """listen for the 'after_commit' event"""

    def after_commit_data_handler(
        key_for_session_data: SessionDataKeys, event: ObjEventStatus
    ):
        data = session.info[key_for_session_data.value]
        del session.info[key_for_session_data.value]
        for class_name, items_data in data.items():
            if class_name == "Hierarchy":
                process_manager_mediator_for_hierarchies(
                    hierarchy_event=event, list_of_hierarchies_data=items_data
                )
            if KAFKA_PRODUCER_TURN_ON:
                start_time = time.perf_counter()
                prepare_msg_for_kafka(
                    obj_class_name=class_name,
                    event=event,
                    items_data=items_data,
                )
                end_time = time.perf_counter()
                print(f"prepare_msg_for_kafka: {end_time - start_time}")

    if session.info.get(SessionDataKeys.NEW.value, False):
        after_commit_data_handler(SessionDataKeys.NEW, ObjEventStatus.CREATED)

    if session.info.get(SessionDataKeys.DIRTY.value, False):
        after_commit_data_handler(SessionDataKeys.DIRTY, ObjEventStatus.UPDATED)

    if session.info.get(SessionDataKeys.DELETED.value, False):
        after_commit_data_handler(
            SessionDataKeys.DELETED, ObjEventStatus.DELETED
        )
