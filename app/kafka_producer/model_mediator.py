from kafka_producer.protobuf import hierarchy_producer_msg_pb2
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from services.security.data.permissions.hierarchy import HierarchyPermission

MODEL_EQ_MESSAGE = {
    "Hierarchy": {
        "class": Hierarchy,
        "proto_unit_template": hierarchy_producer_msg_pb2.HierarchyMessageSchema,
        "proto_list_template": hierarchy_producer_msg_pb2.ListHierarchy,
    },
    "Level": {
        "class": Level,
        "proto_unit_template": hierarchy_producer_msg_pb2.LevelMessageSchema,
        "proto_list_template": hierarchy_producer_msg_pb2.ListLevel,
    },
    "Obj": {
        "class": Obj,
        "proto_unit_template": hierarchy_producer_msg_pb2.NodeMessageSchema,
        "proto_list_template": hierarchy_producer_msg_pb2.ListNode,
    },
    "NodeData": {
        "class": NodeData,
        "proto_unit_template": hierarchy_producer_msg_pb2.NodeDataMessageSchema,
        "proto_list_template": hierarchy_producer_msg_pb2.ListNodeData,
    },
    "HierarchyPermission": {
        "class": HierarchyPermission,
        "proto_unit_template": hierarchy_producer_msg_pb2.HierarchyPermissionMessageSchema,
        "proto_list_template": hierarchy_producer_msg_pb2.ListHierarchyPermission,
    },
}


def protobuf_producer_model_mediator(class_name: str) -> dict | None:
    """returns dict of protobuf models for special class"""
    return MODEL_EQ_MESSAGE.get(class_name)
