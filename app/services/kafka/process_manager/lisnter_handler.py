from typing import List

from schemas.enum_models import HierarchyStatus
from services.kafka.process_manager.impl import KafkaConsumerProcessManager
from services.obj_events.status import ObjEventStatus


def process_manager_mediator_for_hierarchies(
    hierarchy_event: ObjEventStatus, list_of_hierarchies_data: List[dict]
):
    """Based on event status, creates or delete process for handling changes for each hierarchy
    from list_of_hierarchy_data"""

    if not list_of_hierarchies_data:
        return

    p_m = KafkaConsumerProcessManager()
    if hierarchy_event == ObjEventStatus.CREATED:
        for h_data in list_of_hierarchies_data:
            p_m.start_new_process_for_hierarchy_id(hierarchy_id=h_data["id"])
    elif hierarchy_event == ObjEventStatus.DELETED:
        for h_data in list_of_hierarchies_data:
            p_m.stop_process_for_hierarchy_id(hierarchy_id=h_data["id"])

    elif hierarchy_event == ObjEventStatus.UPDATED:
        for h_data in list_of_hierarchies_data:
            status = h_data.get("status")
            if status == HierarchyStatus.IN_PROCESS.value:
                p_m.stop_process_for_hierarchy_id(hierarchy_id=h_data["id"])
            elif status == HierarchyStatus.COMPLETE.value:
                p_m.start_new_process_for_hierarchy_id(
                    hierarchy_id=h_data["id"]
                )
