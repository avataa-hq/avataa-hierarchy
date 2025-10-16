import json

from google.protobuf.timestamp_pb2 import Timestamp

from grpc_server.hierarchy.hierarchy_data_pb2 import (
    HierarchySchema,
    LevelSchema,
    NodeDataSchema,
    ObjSchema,
)
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj


def convert_obj_to_obj_proto_schema(item: Obj) -> ObjSchema:
    """Converts Obj to ObjSchema"""
    item_dict = item.dict()
    item_dict["id"] = str(item.id)

    parent_id = item_dict.get("parent_id")
    if parent_id:
        item_dict["parent_id"] = str(parent_id)

    return ObjSchema(**item_dict)


def convert_node_data_to_node_data_proto_schema(
    item: NodeData,
) -> NodeDataSchema:
    """Converts NodeData to NodeDataSchema"""
    node_data_d = item.dict()
    if item.node_id:
        node_data_d["node_id"] = str(item.node_id)
    if "unfolded_key" in node_data_d:
        if item.unfolded_key is not None:
            node_data_d["unfolded_key"] = json.dumps(item.unfolded_key)
        else:
            del node_data_d["unfolded_key"]
    return NodeDataSchema(**node_data_d)


def convert_hierarchy_to_hierarchy_proto_schema(
    item: Hierarchy,
) -> HierarchySchema:
    """Converts Hierarchy to HierarchySchema"""
    h_data = item.dict()
    if not item.created:
        del h_data["created"]
    else:
        created = Timestamp()
        created.FromDatetime(item.created)
        h_data["created"] = created

    if not item.modified:
        del h_data["modified"]
    else:
        modified = Timestamp()
        modified.FromDatetime(item.modified)
        h_data["modified"] = modified
    return HierarchySchema(**h_data)


def convert_level_to_level_proto_schema(item: Level) -> LevelSchema:
    """Converts Level to LevelSchema"""
    l_data = item.dict()
    if not item.created:
        del l_data["created"]
    else:
        created = Timestamp()
        created.FromDatetime(item.created)
        l_data["created"] = created

    if not item.modified:
        del l_data["modified"]
    else:
        modified = Timestamp()
        modified.FromDatetime(item.modified)
        l_data["modified"] = modified

    return LevelSchema(**l_data)
